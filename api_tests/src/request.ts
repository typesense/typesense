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
  await waitForNodeSync(node);
  const res = await fetchMultiNodeRequest(node, url, options);
  return res;
}

// Waits until `node` has applied every entry the leader holds and the leader has committed them, so a request to it
// sees every earlier write. Followers learn a commit on the leader's next heartbeat; the leader needs no wait.
async function waitForNodeSync(node: number) {
  const deadline = Date.now() + SYNC_TIMEOUT_MS;
  let statuses: any[] = [];
  while (true) {
    statuses = await getMultiNodeStatuses();
    if (isNodeInSync(statuses, node - 1)) return;
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

function getAppliedIndex(status: any) {
  return status.applying_index === 0 ? status.known_applied_index : status.applying_index;
}

function isNodeInSync(statuses: any[], nodeIndex: number) {
  const leaders = statuses.filter((status) => status.state === "LEADER");
  const leader = leaders[0];
  if (leaders.length !== 1 || !leader || leader.committed_index !== leader.last_index) {
    return false;
  }
  return getAppliedIndex(statuses[nodeIndex]) >= leader.last_index;
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

// A flush empties a node's analytics buffers before it posts their contents to the leader, so a read right after
// waitFor*AnalyticsFlush can run before those writes land. Reads until `done` accepts the body or FLUSH_TIMEOUT_MS
// passes, and returns the last response either way so the caller's assertions report what was there.
export async function readAfterFlush(read: () => Promise<Response>, done: (body: string) => boolean): Promise<Response> {
  const deadline = Date.now() + FLUSH_TIMEOUT_MS;
  while (true) {
    const res = await read();
    const body = await res.text();
    let accepted = false;
    try {
      accepted = res.ok && done(body);
    } catch {}
    if (accepted || Date.now() >= deadline) {
      return new Response(body, { status: res.status, statusText: res.statusText, headers: res.headers });
    }
    await Bun.sleep(POLL_INTERVAL_MS);
  }
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
