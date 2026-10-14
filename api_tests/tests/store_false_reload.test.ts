import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

// a field declared `store: false` is stripped before the document is written to
// disk. when it is also `optional: false`, the reloaded document used to fail
// validation and the whole collection loaded 0 documents, silently
// the documents must survive a restart and a snapshot restore. SINGLE_SNAPSHOT
// is the important phase. The snapshot truncates the raft log, so the 
// restart is a true cold-load from the on-disk store.
const COLLECTION = "store_false_reload";

async function foundCount(): Promise<number> {
  const res = await fetchSingleNode(
    `/collections/${COLLECTION}/documents/search?q=*&per_page=250`,
    { method: "GET" },
  );
  expect(res.ok).toBe(true);
  const body: any = await res.json();
  return body.found as number;
}

describe(Phases.SINGLE_FRESH, () => {
  it("seed store:false + optional:false fields", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: COLLECTION,
        fields: [
          { name: "title", type: "string" },
          { name: "embedding", type: "float[]", num_dim: 4, optional: false, store: false },
          { name: "secret", type: "string", optional: false, store: false },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    const jsonl = [
      JSON.stringify({ id: "1", title: "alpha", embedding: [0.1, 0.2, 0.3, 0.4], secret: "s1" }),
      JSON.stringify({ id: "2", title: "bravo", embedding: [0.2, 0.3, 0.4, 0.5], secret: "s2" }),
      JSON.stringify({ id: "3", title: "charlie", embedding: [0.3, 0.4, 0.5, 0.6], secret: "s3" }),
    ].join("\n");
    res = await fetchSingleNode(
      `/collections/${COLLECTION}/documents/import?action=create`,
      { method: "POST", body: jsonl },
    );
    expect(res.ok).toBe(true);
    const importBody = await res.text();
    expect(importBody.split("\n").every((l) => l.includes('"success":true'))).toBe(true);

    expect(await foundCount()).toBe(3);
  });

  it("returns the stored field but not the unstored ones", async () => {
    const res = await fetchSingleNode(
      `/collections/${COLLECTION}/documents/search?q=*&per_page=250`,
      { method: "GET" },
    );
    expect(res.ok).toBe(true);
    const body: any = await res.json();
    const doc = body.hits[0].document;
    expect(doc.title).toBeDefined();
    expect(doc.embedding).toBeUndefined();
    expect(doc.secret).toBeUndefined();
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("documents survive a restart", async () => {
    expect(await foundCount()).toBe(3);
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("documents survive a snapshot restore (cold-load from store)", async () => {
    expect(await foundCount()).toBe(3);
  });
});
