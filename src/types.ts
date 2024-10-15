export interface DIDsFromDB {
  did: string;
  endpoint: string;
}

export type PDSDIDGrouped = Record<string, string[]>;
export type PDSHealthStatus = Record<string, boolean>;

export interface ServerDescription {
  availableUserDomains?: string[];
  inviteCodeRequired?: boolean;
  links?: Record<string, unknown>;
  did?: string;
  contact?: Record<string, unknown>;
}

export type BskyPost = Record<
  string,
  {
    cid: string;
    value: {
      text: string;
      $type: string;
      langs: string[];
      createdAt: string;
      cid: string;
    };
  }
>;

export type BskyProfile = Record<
  string,
  {
    cid: string;
    value: {
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
    };
  }
>;

export type BskyData = BskyPost | BskyProfile;
