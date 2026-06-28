import { AxiosRequestConfig } from "axios";
import type { Logger } from "pino";
import { proto } from "../../WAProto";
import {
  AuthenticationCreds,
  BaileysEventEmitter,
  Chat,
  GroupMetadata,
  ParticipantAction,
  SignalKeyStoreWithTransaction,
  SocketConfig,
  WAMessage,
  WAMessageKey,
  WAMessageStubType
} from "../Types";
import {
  aesDecryptGCM,
  downloadAndProcessHistorySyncNotification,
  normalizeMessageContent,
  toNumber
} from "../Utils";
import {
  areJidsSameUser,
  isJidGroup,
  isLidUser,
  jidNormalizedUser
} from "../WABinary";
import { generateMsgSecretKey } from "./reporting-utils";

const SECRET_ENC_LABELS: Readonly<
  Partial<Record<proto.Message.SecretEncryptedMessage.SecretEncType, string>>
> = {
  [proto.Message.SecretEncryptedMessage.SecretEncType.MESSAGE_EDIT]:
    "Message Edit",
  [proto.Message.SecretEncryptedMessage.SecretEncType.EVENT_EDIT]: "Event Edit"
};

type ProcessMessageContext = {
  shouldProcessHistoryMsg: boolean;
  creds: AuthenticationCreds;
  keyStore: SignalKeyStoreWithTransaction;
  ev: BaileysEventEmitter;
  logger?: Logger;
  options: AxiosRequestConfig<any>;
  config: SocketConfig;
};

const MSG_MISSED_CALL_TYPES = new Set([
  WAMessageStubType.CALL_MISSED_GROUP_VIDEO,
  WAMessageStubType.CALL_MISSED_GROUP_VOICE,
  WAMessageStubType.CALL_MISSED_VIDEO,
  WAMessageStubType.CALL_MISSED_VOICE
]);

/** Cleans a received message to further processing */
export const cleanMessage = (
  message: proto.IWebMessageInfo,
  meId: string,
  meLid?: string
) => {
  // ensure remoteJid and participant doesn't have device or agent in it
  message.key.remoteJid = jidNormalizedUser(message.key.remoteJid!);
  message.key.participant = message.key.participant
    ? jidNormalizedUser(message.key.participant!)
    : undefined;
  const content = normalizeMessageContent(message.message);
  // if the message has a reaction, ensure fromMe & remoteJid are from our perspective
  if (content?.reactionMessage?.key) {
    normaliseKey(content.reactionMessage.key);
  }

  if (content?.secretEncryptedMessage?.targetMessageKey) {
    normaliseKey(content.secretEncryptedMessage.targetMessageKey);
  }

  function normaliseKey(msgKey: proto.IMessageKey) {
    // if the reaction is from another user
    // we've to correctly map the key to this user's perspective
    if (!message.key.fromMe) {
      // if the sender believed the message being reacted to is not from them
      // we've to correct the key to be from them, or some other participant
      msgKey.fromMe = !msgKey.fromMe
        ? areJidsSameUser(msgKey.participant || msgKey.remoteJid!, meId) ||
          (!!meLid &&
            areJidsSameUser(msgKey.participant || msgKey.remoteJid!, meLid))
        : // if the message being reacted to, was from them
          // fromMe automatically becomes false
          false;
      // set the remoteJid to being the same as the chat the message came from
      msgKey.remoteJid = message.key.remoteJid;
      // set participant of the message
      msgKey.participant = msgKey.participant || message.key.participant;
    }
  }
};

export const decryptSecretEncryptedMessage = async (
  message: WAMessage,
  messageSecret: Uint8Array,
  meId: string,
  meLid: string | undefined,
  logger?: Logger
) => {
  // PATCH: Revive messageSecret if it was corrupted by JSON stringification in the DB
  if (
    messageSecret &&
    !Buffer.isBuffer(messageSecret) &&
    !(messageSecret instanceof Uint8Array)
  ) {
    if (typeof messageSecret === "string") {
      messageSecret = Buffer.from(messageSecret, "base64");
    } else if (typeof messageSecret === "object") {
      const obj: any = messageSecret;
      const keys = Object.keys(obj).filter(k => !isNaN(Number(k)));
      if (keys.length > 0) {
        const arr = new Uint8Array(keys.length);
        for (let i = 0; i < keys.length; i++) arr[i] = obj[String(i)];
        messageSecret = arr;
      }
    }
  }

  const content = normalizeMessageContent(message.message);
  const secretEncryptedMessage = content?.secretEncryptedMessage;
  if (!secretEncryptedMessage) return;

  const targetMessageKey = secretEncryptedMessage.targetMessageKey as
    WAMessageKey | undefined;
  const secretEncType = secretEncryptedMessage.secretEncType;
  const { SecretEncType } = proto.Message.SecretEncryptedMessage;

  if (
    secretEncType === null ||
    secretEncType === undefined ||
    secretEncType === SecretEncType.UNKNOWN ||
    !targetMessageKey?.id
  ) {
    logger?.warn(
      {
        secretEncType,
        targetMessageKey: secretEncryptedMessage.targetMessageKey
      },
      "unsupported secret encrypted message type"
    );
    return;
  }

  const useCaseLabel = SECRET_ENC_LABELS[secretEncType];
  if (!useCaseLabel) {
    logger?.info(
      { secretEncType, targetMessageKey, messageKey: message.key },
      "no HKDF label registered for secret encrypted message type, leaving envelope intact"
    );
    return;
  }

  if (
    !secretEncryptedMessage.encPayload?.length ||
    !secretEncryptedMessage.encIv?.length
  ) {
    logger?.warn({ targetMessageKey }, "missing encrypted edit payload");
    return;
  }

  const modLid = message.key.senderLid || message.key.participantLid;
  const targetLid =
    targetMessageKey.senderLid || targetMessageKey.participantLid;
  const envelopeIsLid =
    !!modLid ||
    !!targetLid ||
    isLidUser(message.key.remoteJid ?? undefined) ||
    isLidUser(message.key.participant ?? undefined) ||
    isLidUser(targetMessageKey.remoteJid ?? undefined) ||
    isLidUser(targetMessageKey.participant ?? undefined);
  const ownSender = jidNormalizedUser(envelopeIsLid && meLid ? meLid : meId);
  const originalSender = targetMessageKey.fromMe
    ? ownSender
    : jidNormalizedUser(
        (envelopeIsLid && (targetLid || modLid)) ||
          targetMessageKey.participant ||
          targetMessageKey.remoteJid ||
          ""
      );
  const modificationSender = message.key.fromMe
    ? ownSender
    : jidNormalizedUser(
        (envelopeIsLid && modLid) ||
          message.key.participant ||
          message.key.remoteJid ||
          ""
      );

  if (!originalSender || !modificationSender) {
    logger?.warn(
      { targetMessageKey, messageKey: message.key },
      "missing sender for secret encrypted message"
    );
    return;
  }

  let editedMessage: proto.IMessage | undefined;
  try {
    const decryptKey = await generateMsgSecretKey(
      useCaseLabel,
      targetMessageKey.id,
      originalSender,
      modificationSender,
      messageSecret
    );
    const decrypted = aesDecryptGCM(
      secretEncryptedMessage.encPayload,
      decryptKey,
      secretEncryptedMessage.encIv,
      Buffer.alloc(0)
    );
    editedMessage = proto.Message.decode(decrypted);
  } catch (err) {
    logger?.warn(
      {
        err,
        targetMessageKey,
        messageKey: message.key,
        originalSender,
        modificationSender
      },
      "failed to decrypt secret encrypted message"
    );
    return;
  }

  if (
    editedMessage.protocolMessage?.type ===
    proto.Message.ProtocolMessage.Type.MESSAGE_EDIT
  ) {
    message.message = editedMessage;
    return;
  }

  if (
    message.message?.messageContextInfo &&
    !editedMessage.messageContextInfo
  ) {
    editedMessage.messageContextInfo = message.message.messageContextInfo;
  }

  message.message = {
    protocolMessage: {
      key: targetMessageKey,
      type: proto.Message.ProtocolMessage.Type.MESSAGE_EDIT,
      editedMessage,
      timestampMs: toNumber(message.messageTimestamp) * 1000
    }
  };
};

export const isRealMessage = (message: proto.IWebMessageInfo) => {
  const normalizedContent = normalizeMessageContent(message.message);
  return (
    (!!normalizedContent ||
      MSG_MISSED_CALL_TYPES.has(message.messageStubType!)) &&
    !normalizedContent?.protocolMessage &&
    !normalizedContent?.secretEncryptedMessage &&
    !normalizedContent?.reactionMessage
  );
};

export const shouldIncrementChatUnread = (message: proto.IWebMessageInfo) =>
  !message.key.fromMe && !message.messageStubType;

const updateGroupMetadata = (
  groupMetadata: GroupMetadata,
  action: ParticipantAction,
  participants: string[]
): GroupMetadata => {
  const findParticipant = (id: string) =>
    groupMetadata.participants.find(p => areJidsSameUser(p.id, id));

  switch (action) {
    case "add":
      participants.forEach(id => {
        if (!findParticipant(id)) {
          groupMetadata.participants.push({ id, admin: null });
        }
      });
      break;

    case "remove":
      groupMetadata.participants = groupMetadata.participants.filter(
        p => !participants.includes(p.id)
      );
      break;

    case "promote":
      participants.forEach(id => {
        const p = findParticipant(id);
        if (p) p.admin = "admin";
      });
      break;

    case "demote":
      participants.forEach(id => {
        const p = findParticipant(id);
        if (p) p.admin = null;
      });
      break;
  }

  return groupMetadata;
};

const processMessage = async (
  message: WAMessage,
  {
    shouldProcessHistoryMsg,
    ev,
    creds,
    keyStore,
    logger,
    options,
    config
  }: ProcessMessageContext
) => {
  const meId = creds.me!.id;
  const { accountSettings } = creds;

  const chat: Partial<Chat> = { id: jidNormalizedUser(message.key.remoteJid!) };

  if (isRealMessage(message)) {
    chat.conversationTimestamp = toNumber(message.messageTimestamp);
    // only increment unread count if not CIPHERTEXT and from another person
    if (shouldIncrementChatUnread(message)) {
      chat.unreadCount = (chat.unreadCount || 0) + 1;
    }

    if (accountSettings?.unarchiveChats) {
      chat.archived = false;
      chat.readOnly = false;
    }
  }

  const content = normalizeMessageContent(message.message);
  const protocolMsg = content?.protocolMessage;
  if (protocolMsg) {
    switch (protocolMsg.type) {
      case proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION:
        const histNotification = protocolMsg!.historySyncNotification!;
        const process = shouldProcessHistoryMsg;
        const isLatest = !creds.processedHistoryMessages?.length;

        logger?.info(
          {
            histNotification,
            process,
            id: message.key.id,
            isLatest
          },
          "got history notification"
        );

        if (process) {
          if (
            histNotification.syncType !==
            proto.HistorySync.HistorySyncType.ON_DEMAND
          ) {
            ev.emit("creds.update", {
              processedHistoryMessages: [
                ...(creds.processedHistoryMessages || []),
                { key: message.key, messageTimestamp: message.messageTimestamp }
              ]
            });
          }

          const data = await downloadAndProcessHistorySyncNotification(
            histNotification,
            options
          );

          ev.emit("messaging-history.set", {
            ...data,
            isLatest:
              histNotification.syncType !==
              proto.HistorySync.HistorySyncType.ON_DEMAND
                ? isLatest
                : false
          });
        }

        break;
      case proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE:
        const keys = protocolMsg.appStateSyncKeyShare!.keys;
        if (keys?.length) {
          let newAppStateSyncKeyId = "";
          await keyStore.transaction(async () => {
            const newKeys: string[] = [];
            for (const { keyData, keyId } of keys) {
              const strKeyId = Buffer.from(keyId!.keyId!).toString("base64");
              newKeys.push(strKeyId);

              await keyStore.set({
                "app-state-sync-key": { [strKeyId]: keyData! }
              });

              newAppStateSyncKeyId = strKeyId;
            }

            logger?.info(
              { newAppStateSyncKeyId, newKeys },
              "injecting new app state sync keys"
            );
          });

          ev.emit("creds.update", { myAppStateKeyId: newAppStateSyncKeyId });
        } else {
          logger?.info({ protocolMsg }, "recv app state sync with 0 keys");
        }

        break;
      case proto.Message.ProtocolMessage.Type.REVOKE:
        ev.emit("messages.update", [
          {
            key: {
              ...message.key,
              id: protocolMsg.key!.id
            },
            update: {
              message: null,
              messageStubType: WAMessageStubType.REVOKE,
              key: message.key
            }
          }
        ]);
        break;
      case proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING:
        Object.assign(chat, {
          ephemeralSettingTimestamp: toNumber(message.messageTimestamp),
          ephemeralExpiration: protocolMsg.ephemeralExpiration || null
        });

        if (chat.id && isJidGroup(chat.id)) {
          const groupMetadata = config.groupMetadataCache?.get(chat.id) as
            GroupMetadata | undefined;

          if (!groupMetadata) break;

          config.groupMetadataCache?.set(chat.id, {
            ...groupMetadata,
            ephemeralDuration: protocolMsg.ephemeralExpiration || null
          });
        }

        break;
      case proto.Message.ProtocolMessage.Type
        .PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE:
        const response = protocolMsg.peerDataOperationRequestResponseMessage!;

        if (!response?.peerDataOperationResult) break;

        const { peerDataOperationResult } = response;

        const responseMessages = peerDataOperationResult
          .map(result => {
            const responseBytes =
              result.placeholderMessageResendResponse?.webMessageInfoBytes;

            if (!responseBytes) return;

            return proto.WebMessageInfo.decode(responseBytes);
          })
          .filter(Boolean);

        ev.emit("messages.pdo-response", {
          messages: responseMessages as WAMessage[]
        });

        break;
      case proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER:
        ev.emit("chats.phoneNumberShare", {
          lid: message.key.senderLid!,
          jid: message.key.senderPn!
        });

        ev.emit("contacts.phone-number-share", {
          lid: message.key.senderLid!,
          jid: message.key.senderPn!
        });

        break;
    }
  } else if (content?.reactionMessage) {
    const reaction: proto.IReaction = {
      ...content.reactionMessage,
      key: message.key
    };
    ev.emit("messages.reaction", [
      {
        reaction,
        key: content.reactionMessage!.key!
      }
    ]);
  } else if (message.messageStubType) {
    const jid = message.key!.remoteJid!;
    //let actor = whatsappID (message.participant)
    let participants: string[];

    const emitParticipantsUpdate = (action: ParticipantAction) => {
      ev.emit("group-participants.update", { id: jid, participants, action });

      const groupMetadata = config.groupMetadataCache?.get(jid) as
        GroupMetadata | undefined;

      if (!groupMetadata) return;

      const updatedMetadata = updateGroupMetadata(
        groupMetadata,
        action,
        participants
      );

      config.groupMetadataCache?.set(jid, updatedMetadata);
    };

    const emitGroupUpdate = (update: Partial<GroupMetadata>) => {
      ev.emit("groups.update", [{ id: jid, ...update }]);

      const groupMetadata = config.groupMetadataCache?.get(jid) as
        GroupMetadata | undefined;

      if (!groupMetadata) return;

      config.groupMetadataCache?.set(jid, { ...groupMetadata, ...update });
    };

    const participantsIncludesMe = () =>
      participants.find(jid => areJidsSameUser(meId, jid));

    switch (message.messageStubType) {
      case WAMessageStubType.GROUP_PARTICIPANT_LEAVE:
      case WAMessageStubType.GROUP_PARTICIPANT_REMOVE:
        participants = message.messageStubParameters || [];
        emitParticipantsUpdate("remove");
        // mark the chat read only if you left the group
        if (participantsIncludesMe()) {
          chat.readOnly = true;
        }

        break;
      case WAMessageStubType.GROUP_PARTICIPANT_ADD:
      case WAMessageStubType.GROUP_PARTICIPANT_INVITE:
      case WAMessageStubType.GROUP_PARTICIPANT_ADD_REQUEST_JOIN:
        participants = message.messageStubParameters || [];
        if (participantsIncludesMe()) {
          chat.readOnly = false;
        }

        emitParticipantsUpdate("add");
        break;
      case WAMessageStubType.GROUP_PARTICIPANT_DEMOTE:
        participants = message.messageStubParameters || [];
        emitParticipantsUpdate("demote");
        break;
      case WAMessageStubType.GROUP_PARTICIPANT_PROMOTE:
        participants = message.messageStubParameters || [];
        emitParticipantsUpdate("promote");
        break;
      case WAMessageStubType.GROUP_CHANGE_ANNOUNCE:
        const announceValue = message.messageStubParameters?.[0];
        emitGroupUpdate({
          announce: announceValue === "true" || announceValue === "on"
        });
        break;
      case WAMessageStubType.GROUP_CHANGE_RESTRICT:
        const restrictValue = message.messageStubParameters?.[0];
        emitGroupUpdate({
          restrict: restrictValue === "true" || restrictValue === "on"
        });
        break;
      case WAMessageStubType.GROUP_CHANGE_SUBJECT:
        const name = message.messageStubParameters?.[0];
        chat.name = name;
        emitGroupUpdate({ subject: name });
        break;
    }
  }

  if (Object.keys(chat).length > 1) {
    ev.emit("chats.update", [chat]);
  }
};

export default processMessage;
