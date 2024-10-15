import ky from 'ky';

import { relay } from './constants.js';
import logger from './logger.js';
import { ServerDescription } from './types.js';

// Sanitize PDS name by removing control characters
function sanitizePDSName(pds: string): string {
  return pds.replace(/[\x00-\x1F\x7F-\x9F]/g, '');
}

// Sample responses:
// {"availableUserDomains": [], "did": "did:web:fed.brid.gy" }
// {"availableUserDomains":[".boobee.blue"],"inviteCodeRequired":true,"links":{}}
// {"did":"did:web:zio.blue","availableUserDomains":[".zio.blue"],"inviteCodeRequired":true,"links":{},"contact":{}}
// {"did":"did:web:hellthread.pro","availableUserDomains":[".hellthread.pro"],"inviteCodeRequired":true,"links":{},"contact":{}}
export async function isPDSHealthy(pds: string): Promise<boolean> {
  const sanitizedPDS = sanitizePDSName(pds);

  if (sanitizedPDS === relay) {
    return true;
  }

  const maxRetries = 5;
  const timeout = 30000; // 30 seconds

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await ky.get(`https://${sanitizedPDS}/xrpc/com.atproto.server.describeServer`, {
        timeout,
        retry: {
          limit: 1, // We'll handle retries manually
          statusCodes: [429, 502, 503, 504],
        },
      });

      const data: ServerDescription = await res.json();

      return data.availableUserDomains !== undefined;
    } catch (error: any) {
      logger.error(`Attempt ${attempt} - Error checking health for PDS ${sanitizedPDS}: ${error}`);
      if (attempt === maxRetries) {
        return false;
      }
      // Wait before retrying (exponential backoff)
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
    }
  }

  return false;
}
