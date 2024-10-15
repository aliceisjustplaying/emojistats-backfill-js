// const didquery = `
// SELECT
//    identity.did,
//    atproto_pds.endpoint
// FROM
//    identity
// JOIN
//    plc_log ON identity.identity_id = plc_log.identity
// JOIN
//    atproto_pds ON plc_log.atproto_pds = atproto_pds.pds_id
// WHERE
//    plc_log.entry_id IN (
//       SELECT MAX(entry_id)
//       FROM plc_log
//       GROUP BY identity
//    )
// ORDER BY identity.did ASC
// LIMIT 30000;
// `;

import axios from "axios";
import Database from "better-sqlite3";
// const db = new Database(process.env.HOME + "/src/a/plc/mirror.db");

// console.log("Fetching data...");
// const query = db.prepare(didquery);
// let result = query.all();
// console.log("Data fetched");

const ignoreList = [
  "stems.social",
  "localhost",
  "ignore",
  "https://uwu",
  "boobee.blue",
];

let result = [
  { did: "did:plc:by3jhwdqgbtrcc7q4tkkv3cf", endpoint: "https://bsky.social" },
];

const filteredResult = result.filter(
  ({ endpoint }) => !ignoreList.some((ignore) => endpoint.includes(ignore))
);
console.log(`Filtered out ${result.length - filteredResult.length} results`);
console.log(`Remaining results: ${filteredResult.length}`);

// Group by PDS
const groupedByPDS = filteredResult.reduce((acc, { did, endpoint }) => {
  if (endpoint.includes("bsky.social") || endpoint.includes("bsky.network")) {
    endpoint = "https://relay.pop2.bsky.network";
  }
  if (!acc[endpoint]) {
    acc[endpoint] = [];
  }
  acc[endpoint].push(did);
  return acc;
}, {});

console.log(`Unique PDS count: ${Object.keys(groupedByPDS).length}`);

async function checkPDSHealth(pds: string) {
  try {
    const response = await axios.get(`${pds}/xrpc/_health`, {
      timeout: 15000, // 15 seconds timeout
    });
    return (
      (response.data && response.data.status === "ok") ||
      (response.data && response.data.version !== undefined)
    );
  } catch (error) {
    console.error(`Error checking health for PDS ${pds}:`, error.message);
    return false;
  }
}

console.log("Checking PDS health...");
const healthyPDSes = await Promise.all(
  Object.keys(groupedByPDS).map(async (pds) => {
    const isHealthy = await checkPDSHealth(pds);
    if (!isHealthy) {
      console.log(`Skipping unhealthy PDS: ${pds}`);
    }
    return isHealthy ? pds : null;
  })
);

const filteredGroupedByPDS = Object.fromEntries(
  Object.entries(groupedByPDS).filter(([pds]) => healthyPDSes.includes(pds))
);

console.log(`Healthy PDS count: ${Object.keys(filteredGroupedByPDS).length}`);
result = Object.entries(filteredGroupedByPDS).map(([pds, dids]) => ({
  pds,
  dids,
}));

console.dir(result, { depth: null });

import pLimit from "p-limit";

async function fetchData(did, pds) {
  try {
    const response = await axios.post(
      "http://localhost:8000/fetch",
      {
        did,
        pds,
      },
      {
        responseType: "stream",
      }
    );

    return response.data;
  } catch (error) {
    console.error(
      `Error fetching data for DID ${did} from PDS ${pds}:`,
      error.message
    );
    return null;
  }
}

async function processStream(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function fetchAllData() {
  const limit = pLimit(20); // Adjust the concurrency limit as needed
  const fetchPromises = [];

  for (const { pds, dids } of result) {
    for (const did of dids) {
      fetchPromises.push(
        limit(async () => {
          const stream = await fetchData(did, pds);
          if (stream) {
            const data = await processStream(stream);
            // const lines = data.split("\n");
            // for (const line of lines) {
            //   try {
            //     const jsonData = JSON.parse(line);
            //     console.log(jsonData);
            //   } catch (error) {
            //     console.error(`Error parsing data:`, data);
            //   }
            // }
            return data;
          }
          return null;
        })
      );
    }
  }

  const dataStore = await Promise.all(fetchPromises);
  return dataStore.filter(Boolean);
}

fetchAllData()
  .then((data) => {
    console.log(data.length);
    console.log("---");
    // splitData = data.split("\n");
    // console.dir(data, { depth: null });
    for (const item of data) {
      console.log(typeof item);
      console.log(item.length);
      const splitData = item.split("\n");
      console.log(splitData.length);
      for (const item2 of splitData) {
        console.log(JSON.parse(item2));
      }
    }
    console.log("Data fetched and stored successfully:", data.length);
    console.log(
      "Failed fetches:",
      result.reduce((acc, { dids }) => acc + dids.length, 0) - data.length
    );
  })
  .catch((error) => {
    console.error("Unexpected error while fetching data:", error.message);
  });
