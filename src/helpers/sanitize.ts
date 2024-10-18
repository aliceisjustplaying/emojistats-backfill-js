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

export function sanitizeTimestamp(timestamp: string | undefined | null): {
  timestamp: string;
  wasWeird: boolean;
  defaulted: boolean;
} {
  const defaultTimestamp = '1970-01-01T00:00:00.000Z';

  // If there is no timestamp, return the default timestamp
  if (!timestamp) {
    return { timestamp: defaultTimestamp, wasWeird: false, defaulted: true };
  }

  // No such thing as year 0 in the Gregorian calendar
  if (timestamp.startsWith('0000-')) {
    console.warn(`Sanitizing timestamp: ${timestamp}`);
    timestamp = timestamp.replace('0000-', '0001-');
  }

  const date = new Date(timestamp);

  // If the timestamp is not a valid date, return the default timestamp
  if (isNaN(date.getTime())) {
    return { timestamp: defaultTimestamp, wasWeird: true, defaulted: true };
  }

  const LOW_YEAR = 1; // Since Bluesky uses Go, dates don't go back further than 1AD
  const HIGH_YEAR = 294275;

  const SANE_LOW_YEAR = 2022;
  const SANE_HIGH_YEAR = 2025;

  const year = date.getFullYear();
  if (year >= LOW_YEAR && year <= HIGH_YEAR) {
    if (year >= SANE_LOW_YEAR && year <= SANE_HIGH_YEAR) {
      // sane year, valid date
      return { timestamp: date.toISOString(), wasWeird: false, defaulted: false };
    }

    // weird year, but valid date
    return { timestamp: date.toISOString(), wasWeird: true, defaulted: false };
  }

  // invalid date in some way
  return { timestamp: defaultTimestamp, wasWeird: true, defaulted: true };
}

export function sanitizeString(input: string | undefined | null): string {
  if (!input) {
    return '';
  }
  // eslint-disable-next-line no-control-regex
  return input.replace(/[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, '').trim();
}
