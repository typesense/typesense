export async function fetchSingleNode(url: string, options?: RequestInit) {
  const res = await fetch(`http://localhost:8108${url}`, {
    ...options,
    headers: {
      ...options?.headers,
      "X-TYPESENSE-API-KEY": "xyz",
    },
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
  });
  return res;
}

export async function fetchMultiNode(node: number, url: string, options?: RequestInit): Promise<Response> {
  let delay = 10, baseDelay = 10;
  for(let i = 0; i <= 3; i++) {
    const isSync = await checkCommitedIndex();
    if(isSync) {
      break;
    }
    await new Promise(resolve => setTimeout(resolve, delay));
    delay = delay * baseDelay;
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
  const committed_indexes = data.map((d: any) => d.committed_index);
  if(committed_indexes[0] === committed_indexes[1] && committed_indexes[0] === committed_indexes[2]) {
    return true;
  }
  return false;
}