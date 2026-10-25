// followers learn a commit on the leader's next heartbeat (500 ms), so a short poll waits about that long
const POLL_INTERVAL_MS = 50;
const SYNC_TIMEOUT_MS = 30_000;
const FLUSH_TIMEOUT_MS = 30_000;

export async function fetchSingleNode(url: string, options?: RequestInit, port: number = 8108) {
  const res = await fetch(`http://localhost:${port}${url}`, {
    ...options,
    headers: {
      ...options?.headers,
      "X-TYPESENSE-API-KEY": "xyz",
    },
    signal: AbortSignal.timeout(30000),
  });
  return res;
}

export async function fetchMultiNodeRequest(node: number, url: string, options?: RequestInit): Promise<Response> {
  const res = await fetch(`http://localhost:${node + 4}108${url}`, {
    ...options,
    headers: {
      ...options?.headers,
      "X-TYPESENSE-API-KEY": "xyz",
    },
    signal: AbortSignal.timeout(30000),
  });
  return res;
}

export async function fetchMultiNode(node: number, url: string, options?: RequestInit): Promise<Response> {
  await waitForMultiNodeSync();
  const res = await fetchMultiNodeRequest(node, url, options);
  return res;
}

// Waits until all three nodes have committed and applied the same last index.
async function waitForMultiNodeSync() {
  const deadline = Date.now() + SYNC_TIMEOUT_MS;
  let statuses: any[] = [];
  while (true) {
    statuses = await getMultiNodeStatuses();
    if (isCommittedIndexInSync(statuses)) return;
    if (Date.now() >= deadline) {
      throw new Error(`Multi-node cluster did not converge within ${SYNC_TIMEOUT_MS} ms: ${JSON.stringify(statuses)}`);
    }
    await Bun.sleep(POLL_INTERVAL_MS);
  }
}

async function getMultiNodeStatuses(): Promise<any[]> {
  const res = await Promise.all([
    fetchMultiNodeRequest(1, "/status"),
    fetchMultiNodeRequest(2, "/status"),
    fetchMultiNodeRequest(3, "/status"),
  ]);
  return Promise.all(res.map(r => r.json()));
}

export async function checkCommitedIndex() {
  return isCommittedIndexInSync(await getMultiNodeStatuses());
}

function isCommittedIndexInSync(data: any[]) {
  const last_indexes = data.map((d: any) => d.last_index);
  const committed_indexes = data.map((d: any) => d.committed_index);
  const applied_indexes = data.map((d: any) =>
    d.applying_index === 0 ? d.known_applied_index : d.applying_index,
  );

  if(committed_indexes[0] !== committed_indexes[1] || committed_indexes[0] !== committed_indexes[2]) {
    return false;
  }

  return committed_indexes.every((committed_index: number, index: number) => {
    return last_indexes[index] === committed_index && committed_index === applied_indexes[index];
  });
}

function isAnalyticsDrained(status: any) {
  return Object.values(status).every((count) => count === 0);
}

export async function waitForSingleAnalyticsFlush() {
  await fetchSingleNode("/analytics/flush", { method: "POST" });
  const deadline = Date.now() + FLUSH_TIMEOUT_MS;
  while (true) {
    const res = await fetchSingleNode("/analytics/status");
    const status: any = await res.json();
    if (isAnalyticsDrained(status)) return;
    if (Date.now() >= deadline) {
      throw new Error(`Analytics did not drain within ${FLUSH_TIMEOUT_MS} ms: ${JSON.stringify(status)}`);
    }
    await Bun.sleep(POLL_INTERVAL_MS);
  }
}

export async function waitForMultiAnalyticsFlush() {
  await Promise.all([
    fetchMultiNodeRequest(1, "/analytics/flush", { method: "POST" }),
    fetchMultiNodeRequest(2, "/analytics/flush", { method: "POST" }),
    fetchMultiNodeRequest(3, "/analytics/flush", { method: "POST" }),
  ]);

  const deadline = Date.now() + FLUSH_TIMEOUT_MS;
  while (true) {
    const res = await Promise.all([
      fetchMultiNodeRequest(1, "/analytics/status"),
      fetchMultiNodeRequest(2, "/analytics/status"),
      fetchMultiNodeRequest(3, "/analytics/status"),
    ]);
    const statuses: any[] = await Promise.all(res.map(r => r.json()));
    if (statuses.every(isAnalyticsDrained)) return;
    if (Date.now() >= deadline) {
      throw new Error(`Analytics did not drain within ${FLUSH_TIMEOUT_MS} ms: ${JSON.stringify(statuses)}`);
    }
    await Bun.sleep(POLL_INTERVAL_MS);
  }
}
