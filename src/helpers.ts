import ky from 'ky';

import { PDS_HEALTH_CHECK_TIMEOUT_MS, RELAY_URL } from './constants.js';
import { ServerDescription } from './types.js';

export function sanitizePDSName(pds: string): string {
  try {
    const hostnameWithPortRegex = /^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.(?!-)[A-Za-z0-9-]{1,63}(?<!-))*(:\d{1,5})?$/;

    if (!hostnameWithPortRegex.test(pds)) {
      throw new Error(`Invalid PDS hostname: ${pds}`);
    }

    new URL(`https://${pds}/`); // if this still throws, the PDS name is invalid
    return pds;
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
      timeout: PDS_HEALTH_CHECK_TIMEOUT_MS,
      retry: {
        limit: 3,
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

export function sanitizeTimestamp(timestamp: string): string {
  return timestamp.startsWith('0000-') ? timestamp.replace('0000-', '0001-') : timestamp;
}

export function emojiToCodePoint(emoji: string): string {
  return [...emoji].map((char) => char.codePointAt(0)?.toString(16).padStart(4, '0')).join(' ');
}

export function codePointToEmoji(codePoint: string): string {
  const codePoints =
    codePoint.includes(' ') ?
      codePoint.split(' ').map((cp) => parseInt(cp, 16))
    : codePoint.split('-').map((cp) => parseInt(cp, 16));
  return String.fromCodePoint(...codePoints);
}

type AnyObject = Record<string, unknown>;

export function lowercaseObject<T>(input: T): T {
  if (Array.isArray(input)) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return input.map((item) => lowercaseObject(item)) as unknown as T;
  } else if (input !== null && typeof input === 'object') {
    // Preserve Date objects and other non-plain objects
    if (input instanceof Date) {
      return input;
    }

    const lowercasedObj: AnyObject = {};

    for (const key in input) {
      if (Object.prototype.hasOwnProperty.call(input, key)) {
        const lowerKey = typeof key === 'string' ? key.toLowerCase() : key;
        const value = (input as AnyObject)[key];

        if (typeof value === 'string') {
          lowercasedObj[lowerKey] = value.toLowerCase();
        } else {
          lowercasedObj[lowerKey] = lowercaseObject(value);
        }
      }
    }

    return lowercasedObj as T;
  } else if (typeof input === 'string') {
    return input.toLowerCase() as unknown as T;
  }

  // For other types (number, boolean, etc.), return as-is
  return input;
}

export function sanitizeString(input: string): string {
  return input.replace(/\0/g, '');
}
