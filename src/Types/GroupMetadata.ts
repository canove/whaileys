import { Contact } from "./Contact";

export type GroupParticipant = Contact & {
  isAdmin?: boolean;
  isSuperAdmin?: boolean;
  admin?: "admin" | "superadmin" | null;
};

export type ParticipantAction = "add" | "remove" | "promote" | "demote";

export interface GroupMetadata {
  id: string;
  owner: string | undefined;
  /** group uses 'lid' or 'pn' to send messages */
  addressingMode: string | undefined;
  subject: string;
  /** group subject owner */
  subjectOwner?: string;
  /** group subject modification date */
  subjectTime?: number;
  creation?: number;
  desc?: string;
  descOwner?: string;
  descId?: string;
  /** Request approval to join the group */
  joinApprovalMode?: boolean;
  /** if this group is part of a community, it returns the jid of the community to which it belongs */
  linkedParent?: string;
  /** is this a community */
  isCommunity?: boolean;
  /** is this the announce of a community */
  isCommunityAnnounce?: boolean;

  /** is set when the group only allows admins to change group settings */
  restrict?: boolean;
  /** is set when the group only allows admins to write messages */
  announce?: boolean;
  /** number of group participants */
  size?: number;
  // Baileys modified array
  participants: GroupParticipant[];
  ephemeralDuration?: number;
}

export interface WAGroupCreateResponse {
  status: number;
  gid?: string;
  participants?: [{ [key: string]: any }];
}

export interface GroupModificationResponse {
  status: number;
  participants?: { [key: string]: any };
}
