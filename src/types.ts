export interface DidAndPds {
  did: string;
  pds: string;
  status?: 'pending' | 'processing' | 'completed' | 'failed';
  error?: string;
}

export type DidProcessingStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'retry' | null;

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
  emojis: string[];
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
  hasDisplayNameEmojis: boolean;
  hasDescriptionEmojis: boolean;
  displayNameEmojis: string[];
  descriptionEmojis: string[];
}

export interface EmojiData {
  created_at: string;
  emoji: string;
  lang: string;
  post_id: string | null;
  profile_id: string | null;
}

export interface EmojiAmio {
  codes: string;
  char: string;
  name: string;
  category: string;
  group: string;
  subgroup: string;
}

export interface Emoji {
  name: string;
  unified: string;
  non_qualified?: string;
  docomo?: string;
  au?: string;
  softbank?: string;
  google?: string;
  image: string;
  sheet_x: number;
  sheet_y: number;
  short_name: string;
  short_names: string[];
  text: string | null;
  texts: string[] | null;
  category: string;
  subcategory: string;
  sort_order: number;
  added_in: string;
  has_img_apple: boolean;
  has_img_google: boolean;
  has_img_twitter: boolean;
  has_img_facebook: boolean;
}

export interface EmojiVariationSequence {
  code: string;
  textStyle: string;
  emojiStyle: string;
  version: string;
  name: string;
}

export interface LanguageStat {
  language: string;
  count: number;
}
