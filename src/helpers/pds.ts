import ky from 'ky';

import { PDS_HEALTH_CHECK_TIMEOUT_MS, RELAY_URL } from '../constants.js';
import { ServerDescription } from '../types.js';

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
      timeout: PDS_HEALTH_CHECK_TIMEOUT_MS,
      retry: {
        limit: 2,
        statusCodes: [429, 500, 502, 503, 504],
      },
    });

    const data: ServerDescription = await res.json();

    return data.availableUserDomains !== undefined;
  } catch (error) {
    console.error(`Error checking health for PDS ${pds}: ${error}`);
    return false;
  }
}
