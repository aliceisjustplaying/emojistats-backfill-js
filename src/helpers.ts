import ky from 'ky';

import { RELAY_URL } from './constants.js';
import logger from './logger.js';
import { ServerDescription } from './types.js';

export function sanitizePDSName(pds: string): string {
  try {
    const originalPDS = pds;
    pds = pds.trim();
    pds = pds.replace(/<\/?[^>]+(>|$)/g, '');
    // eslint-disable-next-line no-control-regex
    pds = pds.replace(/[\x00-\x1F\x7F-\x9F]/g, '');
    pds = pds.replace(/[^a-zA-Z0-9-._~:/?#[\]!$&'()*+,;=]/g, '');
    const hostnameWithPortRegex = /^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.(?!-)[A-Za-z0-9-]{1,63}(?<!-))*(:\d{1,5})?$/;

    if (!hostnameWithPortRegex.test(pds)) {
      throw new Error(`Invalid PDS hostname format. Original: ${originalPDS}`);
    }

    try {
      const url = new URL(`https://${pds}/`); // if this still throws, the PDS name is invalid
      return pds;
    } catch (error) {
      throw new Error(`sanitizePDSName Error: ${error instanceof Error ? error.message : String(error)}`);
    }
  } catch (error) {
    throw new Error(`sanitizePDSName Error: ${error instanceof Error ? error.message : String(error)}`);
  }
}

// sample responses:
// {"availableUserDomains": [], "did": "did:web:fed.brid.gy" }
// {"availableUserDomains":[".boobee.blue"],"inviteCodeRequired":true,"links":{}}
// {"did":"did:web:zio.blue","availableUserDomains":[".zio.blue"],"inviteCodeRequired":true,"links":{},"contact":{}}
// {"did":"did:web:hellthread.pro","availableUserDomains":[".hellthread.pro"],"inviteCodeRequired":true,"links":{},"contact":{}}
export async function isPDSHealthy(pds: string) {
  if (pds === RELAY_URL) {
    return true;
  }

  try {
    const res = await ky.get(`https://${pds}/xrpc/com.atproto.server.describeServer`, {
      timeout: 30000,
      retry: {
        limit: 5,
        statusCodes: [429, 500, 502, 503, 504],
      },
    });

    const data: ServerDescription = await res.json();

    return data.availableUserDomains !== undefined;
  } catch (error) {
    logger.error(`Error checking health for PDS ${pds}: ${error}`);
    return false;
  }
}
