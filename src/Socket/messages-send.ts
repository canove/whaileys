import { Boom } from "@hapi/boom";
import NodeCache from "node-cache";
import { proto } from "../../WAProto";
import { WA_DEFAULT_EPHEMERAL } from "../Defaults";
import {
  AnyMessageContent,
  GroupMetadata,
  MediaConnInfo,
  MessageReceiptType,
  MessageRelayOptions,
  MiscMessageGenerationOptions,
  SocketConfig,
  WAMessageKey
} from "../Types";
import {
  aggregateMessageKeysNotFromMe,
  assertMediaContent,
  bindWaitForEvent,
  decryptMediaRetryData,
  encodeSignedDeviceIdentity,
  encodeWAMessage,
  encryptMediaRetryRequest,
  encryptSenderKeyMsgSignalProto,
  encryptSignalProto,
  extractDeviceJids,
  generateMessageIDV2,
  generateWAMessage,
  getStatusCodeForMediaRetry,
  getUrlFromDirectPath,
  getWAUploadToServer,
  jidToSignalProtocolAddress,
  parseAndInjectE2ESessions,
  patchMessageForMdIfRequired,
  unixTimestampSeconds
} from "../Utils";
import { getUrlInfo } from "../Utils/link-preview";
import {
  areJidsSameUser,
  BinaryNode,
  BinaryNodeAttributes,
  getBinaryNodeChild,
  getBinaryNodeChildren,
  isJidGroup,
  isJidStatusBroadcast,
  isJidUser,
  isLidUser,
  jidDecode,
  jidEncode,
  jidNormalizedUser,
  JidWithDevice,
  S_WHATSAPP_NET
} from "../WABinary";
import { makeGroupsSocket } from "./groups";

export const makeMessagesSocket = (config: SocketConfig) => {
  const {
    logger,
    linkPreviewImageThumbnailWidth,
    generateHighQualityLinkPreview
  } = config;
  const sock = makeGroupsSocket(config);
  const {
    ev,
    authState,
    upsertMessage,
    query,
    fetchPrivacySettings,
    generateMessageTag,
    sendNode,
    groupMetadata,
    groupToggleEphemeral
  } = sock;

  const userDevicesCache =
    config.userDevicesCache ||
    new NodeCache({
      stdTTL: 300, // 5 minutes
      useClones: false
    });

  const groupMetadataCache = config.groupMetadataCache;

  let mediaConn: Promise<MediaConnInfo>;
  const refreshMediaConn = async (forceGet = false) => {
    const media = await mediaConn;
    if (
      !media ||
      forceGet ||
      new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000
    ) {
      mediaConn = (async () => {
        const result = await query({
          tag: "iq",
          attrs: {
            type: "set",
            xmlns: "w:m",
            to: S_WHATSAPP_NET
          },
          content: [{ tag: "media_conn", attrs: {} }]
        });
        const mediaConnNode = getBinaryNodeChild(result, "media_conn");
        const node: MediaConnInfo = {
          hosts: getBinaryNodeChildren(mediaConnNode, "host").map(
            item => item.attrs as any
          ),
          auth: mediaConnNode!.attrs.auth,
          ttl: +mediaConnNode!.attrs.ttl,
          fetchDate: new Date()
        };
        logger.debug("fetched media conn");
        return node;
      })();
    }

    return mediaConn;
  };

  /**
   * generic send receipt function
   * used for receipts of phone call, read, delivery etc.
   * */
  const sendReceipt = async (
    jid: string,
    participant: string | undefined,
    messageIds: string[],
    type: MessageReceiptType
  ) => {
    const node: BinaryNode = {
      tag: "receipt",
      attrs: {
        id: messageIds[0]
      }
    };
    const isReadReceipt = type === "read" || type === "read-self";
    if (isReadReceipt) {
      node.attrs.t = unixTimestampSeconds().toString();
    }

    if (type === "sender" && isJidUser(jid)) {
      node.attrs.recipient = jid;
      node.attrs.to = participant!;
    } else {
      node.attrs.to = jid;
      if (participant) {
        node.attrs.participant = participant;
      }
    }

    if (type) {
      node.attrs.type = type;
    }

    const remainingMessageIds = messageIds.slice(1);
    if (remainingMessageIds.length) {
      node.content = [
        {
          tag: "list",
          attrs: {},
          content: remainingMessageIds.map(id => ({
            tag: "item",
            attrs: { id }
          }))
        }
      ];
    }

    logger.debug(
      { attrs: node.attrs, messageIds },
      "sending receipt for messages"
    );
    await sendNode(node);
  };

  /** Correctly bulk send receipts to multiple chats, participants */
  const sendReceipts = async (
    keys: WAMessageKey[],
    type: MessageReceiptType
  ) => {
    const recps = aggregateMessageKeysNotFromMe(keys);
    for (const { jid, participant, messageIds } of recps) {
      await sendReceipt(jid, participant, messageIds, type);
    }
  };

  /** Bulk read messages. Keys can be from different chats & participants */
  const readMessages = async (keys: WAMessageKey[]) => {
    const privacySettings = await fetchPrivacySettings();
    // based on privacy settings, we have to change the read type
    const readType =
      privacySettings.readreceipts === "all" ? "read" : "read-self";
    await sendReceipts(keys, readType);
  };

  /** Fetch all the devices we've to send a message to */
  const getUSyncDevices = async (
    jids: string[],
    useCache: boolean,
    ignoreZeroDevices: boolean
  ) => {
    const deviceResults: JidWithDevice[] = [];

    if (!useCache) {
      logger.debug("not using cache for devices");
    }

    const users: BinaryNode[] = [];
    jids = Array.from(new Set(jids));
    for (let jid of jids) {
      const user = jidDecode(jid)?.user;
      jid = jidNormalizedUser(jid);
      if (userDevicesCache.has(user!) && useCache) {
        const devices = userDevicesCache.get<JidWithDevice[]>(user!)!;
        deviceResults.push(...devices);

        logger.trace({ user }, "using cache for devices");
      } else {
        users.push({ tag: "user", attrs: { jid } });
      }
    }

    const iq: BinaryNode = {
      tag: "iq",
      attrs: {
        to: S_WHATSAPP_NET,
        type: "get",
        xmlns: "usync"
      },
      content: [
        {
          tag: "usync",
          attrs: {
            sid: generateMessageTag(),
            mode: "query",
            last: "true",
            index: "0",
            context: "message"
          },
          content: [
            {
              tag: "query",
              attrs: {},
              content: [
                {
                  tag: "devices",
                  attrs: { version: "2" }
                },
                {
                  tag: "lid",
                  attrs: {}
                }
              ]
            },
            { tag: "list", attrs: {}, content: users }
          ]
        }
      ]
    };
    const result = await query(iq);
    const extracted = extractDeviceJids(
      result,
      authState.creds.me!.id,
      ignoreZeroDevices
    );
    const deviceMap: { [_: string]: JidWithDevice[] } = {};

    for (const item of extracted) {
      deviceMap[item.user] = deviceMap[item.user] || [];
      deviceMap[item.user].push(item);

      deviceResults.push(item);
    }

    for (const key in deviceMap) {
      userDevicesCache.set(key, deviceMap[key]);
    }

    return deviceResults;
  };

  const assertSessions = async (jids: string[], force: boolean) => {
    let didFetchNewSession = false;
    let jidsRequiringFetch: string[] = [];
    if (force) {
      jidsRequiringFetch = jids;
    } else {
      const addrs = jids.map(jid => jidToSignalProtocolAddress(jid).toString());
      const sessions = await authState.keys.get("session", addrs);
      for (const jid of jids) {
        const signalId = jidToSignalProtocolAddress(jid).toString();
        if (!sessions[signalId]) {
          jidsRequiringFetch.push(jid);
        }
      }
    }

    if (jidsRequiringFetch.length) {
      logger.debug({ jidsRequiringFetch }, "fetching sessions");
      const result = await query({
        tag: "iq",
        attrs: {
          xmlns: "encrypt",
          type: "get",
          to: S_WHATSAPP_NET
        },
        content: [
          {
            tag: "key",
            attrs: {},
            content: jidsRequiringFetch.map(jid => ({
              tag: "user",
              attrs: { jid }
            }))
          }
        ]
      });
      await parseAndInjectE2ESessions(result, authState);

      didFetchNewSession = true;
    }

    return didFetchNewSession;
  };

  const sendPeerDataOperationMessage = async (
    pdoMessage: proto.Message.IPeerDataOperationRequestMessage
  ): Promise<string> => {
    if (!authState.creds.me?.id) {
      throw new Boom("Not authenticated");
    }

    const protocolMessage: proto.IMessage = {
      protocolMessage: {
        peerDataOperationRequestMessage: pdoMessage,
        type: proto.Message.ProtocolMessage.Type
          .PEER_DATA_OPERATION_REQUEST_MESSAGE
      }
    };

    const meLid = jidNormalizedUser(authState.creds.me.lid);

    const msgId = await relayMessage(meLid, protocolMessage, {
      additionalAttributes: {
        category: "peer",
        // eslint-disable-next-line camelcase
        push_priority: "high_force"
      }
    });

    return msgId;
  };

  const createParticipantNodes = async (
    jids: string[],
    bytes: Buffer,
    extraAttrs?: BinaryNode["attrs"]
  ) => {
    let shouldIncludeDeviceIdentity = false;
    const nodes = await Promise.all(
      jids.map(async jid => {
        const { type, ciphertext } = await encryptSignalProto(
          jid,
          bytes,
          authState
        );
        if (type === "pkmsg") {
          shouldIncludeDeviceIdentity = true;
        }

        const node: BinaryNode = {
          tag: "to",
          attrs: { jid },
          content: [
            {
              tag: "enc",
              attrs: {
                v: "2",
                type,
                ...(extraAttrs || {})
              },
              content: ciphertext
            }
          ]
        };
        return node;
      })
    );
    return { nodes, shouldIncludeDeviceIdentity };
  };

  const createButtonNode = (message: proto.IMessage) => {
    if (message.listMessage) {
      return [
        {
          tag: "list",
          attrs: { type: "product_list", v: "2" }
        }
      ];
    }

    if (
      message.buttonsMessage ||
      message.interactiveMessage?.nativeFlowMessage
    ) {
      return [
        {
          tag: "interactive",
          attrs: { type: "native_flow", v: "1" },
          content: [{ tag: "native_flow", attrs: { v: "9", name: "mixed" } }]
        }
      ];
    }

    return null;
  };

  const relayMessage = async (
    jid: string,
    message: proto.IMessage,
    {
      messageId: msgId,
      participant,
      additionalAttributes,
      useUserDevicesCache,
      statusJidList
    }: MessageRelayOptions
  ) => {
    const meId = authState.creds.me!.id;
    const meLid = authState.creds.me!.lid;
    const isRetryResend = Boolean(participant?.jid);

    let shouldIncludeDeviceIdentity = isRetryResend;

    const isGroup = isJidGroup(jid);
    const isStatus = isJidStatusBroadcast(jid);
    const isGroupOrStatus = isGroup || isStatus;

    msgId = msgId || generateMessageIDV2(sock.user?.id);
    useUserDevicesCache = useUserDevicesCache !== false;

    const destinationJid = isStatus ? "status@broadcast" : jid;

    const participants: BinaryNode[] = [];
    const binaryNodeContent: BinaryNode[] = [];
    const devices: JidWithDevice[] = [];

    const encodedMsg = encodeWAMessage(message);

    await authState.keys.transaction(async () => {
      if (isGroupOrStatus && !isRetryResend) {
        const [groupData, senderKeyMap] = await Promise.all([
          (async () => {
            let groupData = groupMetadataCache?.get(jid) as
              | GroupMetadata
              | undefined;
            if (groupData && Array.isArray(groupData?.participants)) {
              logger.trace(
                { jid, participants: groupData.participants.length },
                "using cached group metadata"
              );
            }

            if (!groupData && !isStatus) {
              groupData = await groupMetadata(jid);
              groupMetadataCache?.set(jid, groupData);
            }

            return groupData;
          })(),
          (async () => {
            const result = await authState.keys.get("sender-key-memory", [jid]);
            return result[jid] || {};
          })()
        ]);

        const participantsList = groupData
          ? groupData.participants.map(p => p.id)
          : [];

        if (groupData?.ephemeralDuration) {
          additionalAttributes = {
            ...additionalAttributes,
            expiration: groupData.ephemeralDuration.toString()
          };
        }

        if (isStatus && statusJidList) {
          participantsList.push(...statusJidList);
        }

        const additionalDevices = await getUSyncDevices(
          participantsList,
          !!useUserDevicesCache,
          false
        );
        devices.push(...additionalDevices);

        if (isGroup) {
          additionalAttributes = {
            ...additionalAttributes,
            addressing_mode: groupData?.addressingMode || "pn"
          };
        }

        const { ciphertext, senderKeyDistributionMessageKey } =
          await encryptSenderKeyMsgSignalProto(
            destinationJid,
            encodedMsg,
            meId,
            authState
          );

        const senderKeyJids: string[] = [];
        // ensure a connection is established with every device
        for (const { user, device } of devices) {
          const encodedLid = jidEncode(user, "lid", device);
          if (!senderKeyMap[encodedLid]) {
            senderKeyJids.push(encodedLid);
            // store that this person has had the sender keys sent to them
            senderKeyMap[encodedLid] = true;
          }
        }

        // if there are some participants with whom the session has not been established
        // if there are, we re-send the senderkey
        if (senderKeyJids.length) {
          logger.debug({ senderKeyJids }, "sending new sender key");

          const encSenderKeyMsg = encodeWAMessage({
            senderKeyDistributionMessage: {
              axolotlSenderKeyDistributionMessage:
                senderKeyDistributionMessageKey,
              groupId: destinationJid
            }
          });

          await assertSessions(senderKeyJids, false);

          const result = await createParticipantNodes(
            senderKeyJids,
            encSenderKeyMsg
          );
          shouldIncludeDeviceIdentity =
            shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;

          participants.push(...result.nodes);
        }

        binaryNodeContent.push({
          tag: "enc",
          attrs: { v: "2", type: "skmsg" },
          content: ciphertext
        });

        await authState.keys.set({
          "sender-key-memory": { [jid]: senderKeyMap }
        });
      } else if (!isRetryResend) {
        const { user } = jidDecode(destinationJid)!;
        const meLidUser = jidDecode(meLid)?.user;
        const isMe = areJidsSameUser(user, meLidUser);

        const encodedMeMsg = encodeWAMessage({
          deviceSentMessage: {
            destinationJid,
            message
          }
        });

        if (additionalAttributes?.["category"] === "peer" && isMe) {
          devices.push({ user: meLidUser! });
        } else {
          const additionalDevices = await getUSyncDevices(
            [meId, jid],
            !!useUserDevicesCache,
            false
          );
          devices.push(...additionalDevices);
        }

        const allLids: string[] = [];
        const meLids: string[] = [];
        const otherLids: string[] = [];

        for (const { user, device } of devices) {
          const isMe = user === meLidUser;

          const encodedLid = jidEncode(user, "lid", device);

          if (isMe) {
            meLids.push(encodedLid);
          } else {
            otherLids.push(encodedLid);
          }

          allLids.push(encodedLid);
        }

        await assertSessions(allLids, false);

        const [
          { nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
          { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }
        ] = await Promise.all([
          createParticipantNodes(meLids, encodedMeMsg),
          createParticipantNodes(otherLids, encodedMsg)
        ]);

        participants.push(...meNodes);
        participants.push(...otherNodes);

        shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
      }

      if (participants.length) {
        if (additionalAttributes?.["category"] === "peer") {
          const peerNode = participants[0]?.content?.[0] as BinaryNode;
          if (peerNode) {
            binaryNodeContent.push(peerNode);
          }
        } else {
          binaryNodeContent.push({
            tag: "participants",
            attrs: {},
            content: participants
          });
        }
      }

      if (isRetryResend) {
        // when the retry request is not for a group
        // only send to the specific device that asked for a retry
        // otherwise the message is sent out to every device that should be a recipient
        if (!isGroup && !isStatus) {
          additionalAttributes = {
            ...additionalAttributes,
            device_fanout: "false"
          };
        }

        const participantDevice = jidDecode(participant!.jid)!.device;

        const [{ user: participantLidUser }] = await getUSyncDevices(
          [participant!.jid],
          true,
          false
        );

        const participantFulLid = jidEncode(
          participantLidUser,
          "lid",
          participantDevice
        );

        await assertSessions([participantFulLid], true);

        if (isGroup) {
          const result = await authState.keys.get("sender-key-memory", [
            destinationJid
          ]);

          const groupSenderKeyMap = result[destinationJid] || {};
          groupSenderKeyMap[participantFulLid] = false;

          await authState.keys.set({
            "sender-key-memory": { [destinationJid]: groupSenderKeyMap }
          });
        }

        const isParticipantLid = isLidUser(participant!.jid);
        const isMe = areJidsSameUser(
          participant!.jid,
          isParticipantLid ? meLid : meId
        );

        const encodedMessageToSend = isMe
          ? encodeWAMessage({
              deviceSentMessage: {
                destinationJid,
                message
              }
            })
          : encodedMsg;

        const { type, ciphertext: encryptedContent } = await encryptSignalProto(
          participantFulLid,
          encodedMessageToSend,
          authState
        );

        binaryNodeContent.push({
          tag: "enc",
          attrs: {
            v: "2",
            type,
            count: participant!.count.toString()
          },
          content: encryptedContent
        });
      }

      const stanza: BinaryNode = {
        tag: "message",
        attrs: {
          id: msgId!,
          type: "text",
          ...(additionalAttributes || {})
        },
        content: binaryNodeContent
      };
      // if the participant to send to is explicitly specified (generally retry recp)
      // ensure the message is only sent to that person
      // if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
      if (participant) {
        if (isJidGroup(destinationJid)) {
          stanza.attrs.to = destinationJid;
          stanza.attrs.participant = participant.jid;
        } else if (areJidsSameUser(participant.jid, meId)) {
          stanza.attrs.to = participant.jid;
          stanza.attrs.recipient = destinationJid;
        } else {
          stanza.attrs.to = participant.jid;
        }
      } else {
        stanza.attrs.to = destinationJid;
      }

      if (shouldIncludeDeviceIdentity) {
        (stanza.content as BinaryNode[]).push({
          tag: "device-identity",
          attrs: {},
          content: encodeSignedDeviceIdentity(authState.creds.account!, true)
        });

        logger.debug({ jid }, "adding device identity");
      }

      const innerMessage =
        message.documentWithCaptionMessage?.message || message;

      const buttonContent = createButtonNode(innerMessage);

      if (buttonContent) {
        (stanza.content as BinaryNode[]).push({
          tag: "biz",
          attrs: {},
          content: buttonContent
        });

        logger.debug({ jid }, `adding biz node for buttons message`);
      }

      logger.debug(
        { msgId },
        `sending message to ${participants.length} devices`
      );

      await sendNode(stanza);
    });

    return msgId;
  };

  const getPrivacyTokens = async (jids: string[]) => {
    const t = unixTimestampSeconds().toString();
    const result = await query({
      tag: "iq",
      attrs: {
        to: S_WHATSAPP_NET,
        type: "set",
        xmlns: "privacy"
      },
      content: [
        {
          tag: "tokens",
          attrs: {},
          content: jids.map(jid => ({
            tag: "token",
            attrs: {
              jid: jidNormalizedUser(jid),
              t,
              type: "trusted_contact"
            }
          }))
        }
      ]
    });

    return result;
  };

  const waUploadToServer = getWAUploadToServer(config, refreshMediaConn);

  const waitForMsgMediaUpdate = bindWaitForEvent(ev, "messages.media-update");

  return {
    ...sock,
    getPrivacyTokens,
    getUSyncDevices,
    assertSessions,
    relayMessage,
    sendReceipt,
    sendReceipts,
    readMessages,
    refreshMediaConn,
    waUploadToServer,
    fetchPrivacySettings,
    sendPeerDataOperationMessage,
    updateMediaMessage: async (message: proto.IWebMessageInfo) => {
      const content = assertMediaContent(message.message);
      const mediaKey = content.mediaKey!;
      const meId = authState.creds.me!.id;
      const node = encryptMediaRetryRequest(message.key, mediaKey, meId);

      let error: Error | undefined = undefined;
      await Promise.all([
        sendNode(node),
        waitForMsgMediaUpdate(update => {
          const result = update.find(c => c.key.id === message.key.id);
          if (result) {
            if (result.error) {
              error = result.error;
            } else {
              try {
                const media = decryptMediaRetryData(
                  result.media!,
                  mediaKey,
                  result.key.id!
                );
                if (
                  media.result !==
                  proto.MediaRetryNotification.ResultType.SUCCESS
                ) {
                  const resultStr =
                    proto.MediaRetryNotification.ResultType[media.result!];
                  throw new Boom(
                    `Media re-upload failed by device (${resultStr})`,
                    {
                      data: media,
                      statusCode:
                        getStatusCodeForMediaRetry(media.result!) || 404
                    }
                  );
                }

                content.directPath = media.directPath;
                content.url = getUrlFromDirectPath(content.directPath!);

                logger.debug(
                  { directPath: media.directPath, key: result.key },
                  "media update successful"
                );
              } catch (err) {
                error = err;
              }
            }

            return true;
          }
        })
      ]);

      if (error) {
        throw error;
      }

      ev.emit("messages.update", [
        { key: message.key, update: { message: message.message } }
      ]);

      return message;
    },
    sendMessage: async (
      jid: string,
      content: AnyMessageContent,
      options: MiscMessageGenerationOptions = {}
    ) => {
      const userJid = authState.creds.me!.id;
      if (
        typeof content === "object" &&
        "disappearingMessagesInChat" in content &&
        typeof content["disappearingMessagesInChat"] !== "undefined" &&
        isJidGroup(jid)
      ) {
        const { disappearingMessagesInChat } = content;
        const value =
          typeof disappearingMessagesInChat === "boolean"
            ? disappearingMessagesInChat
              ? WA_DEFAULT_EPHEMERAL
              : 0
            : disappearingMessagesInChat;
        await groupToggleEphemeral(jid, value);
      } else {
        const fullMsg = await generateWAMessage(jid, content, {
          logger,
          userJid,
          getUrlInfo: text =>
            getUrlInfo(
              text,
              {
                thumbnailWidth: linkPreviewImageThumbnailWidth,
                timeoutMs: 3_000,
                uploadImage: generateHighQualityLinkPreview
                  ? waUploadToServer
                  : undefined
              },
              logger
            ),
          upload: waUploadToServer,
          mediaCache: config.mediaCache,
          messageId: generateMessageIDV2(sock.user?.id),
          ...options
        });
        const isDeleteMsg = "delete" in content && !!content.delete;
        const isEditMsg = "edit" in content && !!content.edit;
        const additionalAttributes: BinaryNodeAttributes = {};
        // required for delete
        if (isDeleteMsg) {
          // if the chat is a group, and I am not the author, then delete the message as an admin
          if (
            isJidGroup(content.delete?.remoteJid as string) &&
            !content.delete?.fromMe
          ) {
            additionalAttributes.edit = "8";
          } else {
            additionalAttributes.edit = "7";
          }
        } else if (isEditMsg) {
          additionalAttributes.edit = "1";
        }

        fullMsg.message = patchMessageForMdIfRequired(fullMsg.message!);

        await relayMessage(jid, fullMsg.message!, {
          messageId: fullMsg.key.id!,
          additionalAttributes
        });

        config.sentMessagesCache?.set(fullMsg.key.id!, fullMsg.message);

        if (config.emitOwnEvents) {
          process.nextTick(() => {
            upsertMessage(fullMsg, "append");
          });
        }

        return fullMsg;
      }
    }
  };
};
