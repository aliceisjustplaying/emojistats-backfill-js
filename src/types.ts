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
