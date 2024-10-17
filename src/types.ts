export interface DidAndPds {
  did: string;
  pds: string;
}

export type PdsToDidsMap = Record<string, string[] | undefined>;
export type PdsHealthStatus = Record<string, boolean | undefined>;

export interface ServerDescription {
  availableUserDomains?: string[];
  inviteCodeRequired?: boolean;
  links?: Record<string, unknown>;
  did?: string;
  contact?: Record<string, unknown>;
}

export interface BskyPostData {
  text: string;
  $type: string;
  langs: string[];
  createdAt: string;
  cid: string;
}

export interface BskyProfileData {
  $type: string;
  avatar?: {
    ref: { $link: string };
    size: number;
    $type: string;
    mimeType: string;
  };
  banner?: {
    ref: { $link: string };
    size: number;
    $type: string;
    mimeType: string;
  };
  createdAt: string;
  description?: string;
  displayName?: string;
  cid: string;
}

export type BskyPost = Record<
  string,
  {
    cid: string;
    value: BskyPostData;
  }
>;

export type BskyProfile = Record<
  string,
  {
    cid: string;
    value: BskyProfileData;
  }
>;

export type BskyData = BskyPost | BskyProfile;

// TODO: redundant
export interface PostData {
  cid: string;
  did: string;
  rkey: string;
  hasEmojis: boolean;
  langs: string[];
  post: string;
  createdAt: string;
}

export interface ProfileData {
  cid: string;
  did: string;
  rkey: string;
  displayName: string;
  description: string;
  createdAt: string;
}
