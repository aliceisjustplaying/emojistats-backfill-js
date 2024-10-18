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

export function sanitizeTimestamp(timestamp: string | undefined | null): { timestamp: string; wasWeird: boolean } {
  const defaultTimestamp = '1970-01-01T00:00:00.000Z';

  // If there is no timestamp, return the default timestamp
  if (!timestamp) {
    return { timestamp: defaultTimestamp, wasWeird: false };
  }

  let wasWeird = false;

  // No such thing as year 0 in the Gregorian calendar
  if (timestamp.startsWith('0000-')) {
    console.warn(`Sanitizing timestamp: ${timestamp}`);
    timestamp = timestamp.replace('0000-', '0001-');
    wasWeird = true;
  }

  const date = new Date(timestamp);

  // If the timestamp is not a valid date, return the default timestamp
  if (isNaN(date.getTime())) {
    return { timestamp: defaultTimestamp, wasWeird: true };
  }

  const LOW_YEAR = 1; // Since Bluesky uses Go, dates don't go back further than 1AD
  const HIGH_YEAR = 294275;

  const SANE_LOW_YEAR = 2022;
  const SANE_HIGH_YEAR = 2025;

  const year = date.getFullYear();
  if (year >= LOW_YEAR && year <= HIGH_YEAR) {
    if (year >= SANE_LOW_YEAR && year <= SANE_HIGH_YEAR) {
      // sane year, valid date
      return { timestamp: date.toISOString(), wasWeird: false };
    }

    // weird year, but valid date
    return { timestamp: date.toISOString(), wasWeird: true };
  }

  // invalid date in some way
  return { timestamp: defaultTimestamp, wasWeird: true };
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

export function sanitizeString(input: string | undefined | null): string {
  if (!input) {
    return '';
  }
  // eslint-disable-next-line no-control-regex
  return input.replace(/[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, '').trim();
}

export function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}
