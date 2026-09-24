const DELAY_INTERVALS = [10, 100, 1000, 2000, 3000, 4000];

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
  for(let i = 0; i < DELAY_INTERVALS.length; i++) {
    const isSync = await checkCommitedIndex();
    if(isSync) {
      break;
    }
    await new Promise(resolve => setTimeout(resolve, DELAY_INTERVALS[i]));
  }
  const res = await fetchMultiNodeRequest(node, url, options);
  return res;
}

export async function checkCommitedIndex() {
  const res = await Promise.all([
    fetchMultiNodeRequest(1, "/status"),
    fetchMultiNodeRequest(2, "/status"),
    fetchMultiNodeRequest(3, "/status"),
  ]);

  const data = await Promise.all(res.map(r => r.json()));
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

export async function waitForSingleAnalyticsFlush() {
  await fetchSingleNode("/analytics/flush", { method: "POST" });
  for(let i = 0; i < DELAY_INTERVALS.length; i++) {
    const res = await fetchSingleNode("/analytics/status");
    const data: any = await res.json();
    let isSync = true;
    for(const key in data) {
      if(data[key] !== 0) {
        isSync = false;
        break;
      }
    }
    if(isSync) {
      break;
    }
    await new Promise(resolve => setTimeout(resolve, DELAY_INTERVALS[i]));
  }
}

export async function waitForMultiAnalyticsFlush() {
  await Promise.all([
    fetchMultiNodeRequest(1, "/analytics/flush", { method: "POST" }),
    fetchMultiNodeRequest(2, "/analytics/flush", { method: "POST" }),
    fetchMultiNodeRequest(3, "/analytics/flush", { method: "POST" }),
  ]);

  for(let i = 0; i < DELAY_INTERVALS.length; i++) {
    const res = await Promise.all([
      fetchMultiNodeRequest(1, "/analytics/status"),
      fetchMultiNodeRequest(2, "/analytics/status"),
      fetchMultiNodeRequest(3, "/analytics/status"),
    ]);
    const data: any[] = await Promise.all(res.map(r => r.json()));
    let isSync = true;
    for(const d of data) {
      for(const key in d) {
        if(d[key] !== 0) {
          isSync = false;
          break;
        }
      }
    }
    if(isSync) {
      break;
    }
    await new Promise(resolve => setTimeout(resolve, DELAY_INTERVALS[i]));
  }
}
