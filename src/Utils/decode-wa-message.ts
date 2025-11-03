import { Boom } from "@hapi/boom";
import { proto } from "../../WAProto";
import { AuthenticationState, WAMessage, WAMessageKey } from "../Types";
import {
  areJidsSameUser,
  BinaryNode,
  isJidBroadcast,
  isJidGroup,
  isJidMetaAI,
  isJidNewsletter,
  isJidStatusBroadcast,
  isJidUser,
  isLidUser,
  jidDecode,
  jidEncode
} from "../WABinary";
import { unpadRandomMax16 } from "./generics";
import {
  decryptGroupSignalProto,
  decryptSignalProto,
  processSenderKeyMessage
} from "./signal";

const NO_MESSAGE_FOUND_ERROR_TEXT = "Message absent from node";

type MessageType =
  | "chat"
  | "peer_broadcast"
  | "other_broadcast"
  | "group"
  | "direct_peer_status"
  | "other_status"
  | "newsletter";

export const decodeMessageStanza = (
  stanza: BinaryNode,
  auth: AuthenticationState
) => {
  let msgType: MessageType;
  let chatId: string;
  let author: string;

  const meUser = jidDecode(auth.creds.me!.id)?.user!;

  const senderPn = stanza.attrs.sender_lid
    ? stanza.attrs.from
    : stanza.attrs.sender_pn;

  const senderLid = stanza.attrs.sender_pn
    ? stanza.attrs.from
    : stanza.attrs.sender_lid;

  const participantPn = isJidUser(stanza.attrs.participant)
    ? stanza.attrs.participant
    : stanza.attrs.participant_pn;

  const participantLid = isLidUser(stanza.attrs.participant)
    ? stanza.attrs.participant
    : stanza.attrs.participant_lid;

  const isGroup = isJidGroup(stanza.attrs.from);

  const fromJidUser = jidDecode(senderPn)?.user;
  const fromDevice = jidDecode(stanza.attrs.from)?.device;

  const participantJidUser = jidDecode(participantPn)?.user;
  const participantDevice = jidDecode(stanza.attrs.participant)?.device;

  const participantFullJid = participantJidUser
    ? jidEncode(participantJidUser, "s.whatsapp.net", participantDevice)
    : undefined;

  const fromFullJid =
    !isGroup && fromJidUser
      ? jidEncode(fromJidUser, "s.whatsapp.net", fromDevice)
      : undefined;

  const msgId = stanza.attrs.id;
  const from = fromFullJid || stanza.attrs.from;
  const participant = participantFullJid || stanza.attrs.participant;
  const recipient = stanza.attrs.recipient;
  const recipientLid = stanza.attrs.peer_recipient_lid;

  const isMe = (jid: string) => areJidsSameUser(jid, auth.creds.me!.id);
  const isMeLid = (jid: string) => areJidsSameUser(jid, auth.creds.me!.lid);

  if (isJidUser(from) || isLidUser(from)) {
    if (recipient && !isJidMetaAI(recipient)) {
      if (!isMe(from) && !isMeLid(from)) {
        throw new Boom("recipient present, but msg not from me", {
          data: stanza
        });
      }

      chatId = recipient;
    } else {
      chatId = from;
    }

    msgType = "chat";
    author = from;
  } else if (isJidGroup(from)) {
    if (!participant) {
      throw new Boom("No participant in group message");
    }

    msgType = "group";
    chatId = from;
    author = isMeLid(participant)
      ? jidEncode(meUser, "s.whatsapp.net", participantDevice)
      : participant;
  } else if (isJidBroadcast(from)) {
    if (!participant) {
      throw new Boom("No participant in group message");
    }

    const isParticipantMe = isMe(participant) || isMeLid(participant);
    if (isJidStatusBroadcast(from)) {
      msgType = isParticipantMe ? "direct_peer_status" : "other_status";
    } else {
      msgType = isParticipantMe ? "peer_broadcast" : "other_broadcast";
    }

    chatId = from;
    author = participant;
  } else if (isJidNewsletter(from)) {
    msgType = "newsletter";
    chatId = from;
    author = from;
  } else {
    throw new Boom("Unknown message type", { data: stanza });
  }

  const sender = msgType === "chat" ? author : chatId;

  const fromMe = (isLidUser(from) || isLidUser(participant) ? isMeLid : isMe)(
    stanza.attrs.participant || stanza.attrs.from
  );
  const pushname = stanza.attrs.notify;

  const key: WAMessageKey = {
    remoteJid: chatId,
    fromMe,
    id: msgId,
    senderLid,
    senderPn,
    participant,
    participantPn,
    participantLid,
    recipientLid
  };

  const fullMessage: WAMessage = {
    key,
    category: stanza.attrs.category,
    messageTimestamp: +stanza.attrs.t,
    pushName: pushname
  };

  if (key.fromMe) {
    fullMessage.status = proto.WebMessageInfo.Status.SERVER_ACK;
  }

  return {
    fullMessage,
    category: stanza.attrs.category,
    author,
    decryptionTask: (async () => {
      let decryptables = 0;
      if (Array.isArray(stanza.content)) {
        for (const { tag, attrs, content } of stanza.content) {
          if (tag === "unavailable" && attrs.type === "view_once") {
            fullMessage.key.isViewOnce = true;
          }

          if (tag === "verified_name" && content instanceof Uint8Array) {
            const cert = proto.VerifiedNameCertificate.decode(content);
            const details = proto.VerifiedNameCertificate.Details.decode(
              cert.details!
            );
            fullMessage.verifiedBizName = details.verifiedName;
          }

          if (attrs.count && tag === "enc") {
            fullMessage.retryCount = Number(attrs.count);
          }

          if (tag !== "enc" && tag !== "plaintext") {
            continue;
          }

          if (!(content instanceof Uint8Array)) {
            continue;
          }

          decryptables += 1;

          let msgBuffer: Buffer;

          try {
            const e2eType = tag === "plaintext" ? "plaintext" : attrs.type;
            switch (e2eType) {
              case "skmsg":
                msgBuffer = await decryptGroupSignalProto(
                  sender,
                  author,
                  content,
                  auth
                );
                break;
              case "pkmsg":
              case "msg":
                const user = isJidUser(sender) ? sender : author;
                msgBuffer = await decryptSignalProto(
                  user,
                  e2eType,
                  content as Buffer,
                  auth
                );
                break;
              case "msmsg":
                return; // ignore meta IA messages
              case "plaintext":
                msgBuffer = content as Buffer;
                break;
              default:
                throw new Error(`Unknown e2e type: ${e2eType}`);
            }

            let msg: proto.IMessage = proto.Message.decode(
              e2eType !== "plaintext" ? unpadRandomMax16(msgBuffer) : msgBuffer
            );

            msg = msg.deviceSentMessage?.message || msg;
            if (msg.senderKeyDistributionMessage) {
              await processSenderKeyMessage(
                author,
                msg.senderKeyDistributionMessage,
                auth
              );
            }

            if (fullMessage.message) {
              Object.assign(fullMessage.message, msg);
            } else {
              fullMessage.message = msg;
            }
          } catch (error) {
            fullMessage.messageStubType =
              proto.WebMessageInfo.StubType.CIPHERTEXT;
            fullMessage.messageStubParameters = [error.message];
          }
        }
      }

      // if nothing was found to decrypt
      if (!decryptables && !fullMessage.key?.isViewOnce) {
        fullMessage.messageStubType = proto.WebMessageInfo.StubType.CIPHERTEXT;
        fullMessage.messageStubParameters = [NO_MESSAGE_FOUND_ERROR_TEXT];
      }
    })()
  };
};
