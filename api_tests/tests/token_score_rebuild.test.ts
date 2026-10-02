import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode, fetchMultiNode } from "../src/request";

// art token max_score is stamped when a document is indexed and only ever raised
// afterwards, so a collection whose default_sorting_field moves around ends up
// picking prefix candidates on scores that no longer exist. the rebuild is an
// on-demand operation on the collection PATCH route that recomputes them from the
// live sort index.
//
// every assertion below runs with max_candidates=1, which means exactly one token
// under the "ap" prefix gets expanded and the winner is decided purely by max_score.

const COLLECTION = "token_score_rebuild";
const NOSTORE_COLLECTION = "token_score_rebuild_nostore";
const DELETE_COLLECTION = "token_score_rebuild_delete";
const UNRANKED_COLLECTION = "token_score_rebuild_unranked";
const MULTI_COLLECTION = "token_score_rebuild_multi";

const SEARCH = "/documents/search?q=ap&query_by=title&prefix=true&num_typos=0&max_candidates=1";

function schemaFor(name: string, storeTitle: boolean) {
  return {
    name,
    fields: [
      { name: "title", type: "string", store: storeTitle },
      { name: "points", type: "int32" },
    ],
    default_sorting_field: "points",
  };
}

// three distinct tokens share the "ap" prefix, one document each, so every leaf
// max_score is just that document's score
const SEED_DOCS = [
  JSON.stringify({ id: "0", title: "apple", points: 100 }),
  JSON.stringify({ id: "1", title: "apricot", points: 10 }),
  JSON.stringify({ id: "2", title: "apron", points: 5 }),
].join("\n");

async function topHitId(collection: string): Promise<string> {
  const res = await fetchSingleNode(`/collections/${collection}${SEARCH}`);
  expect(res.ok).toBe(true);
  const body: any = await res.json();
  expect(body.hits.length).toBe(1);
  return body.hits[0].document.id as string;
}

async function rebuild(collection: string): Promise<Response> {
  return fetchSingleNode(`/collections/${collection}?rebuild_token_scores=true`, {
    method: "PATCH",
  });
}

async function updatePoints(collection: string, id: string, points: number) {
  const res = await fetchSingleNode(`/collections/${collection}/documents/${id}`, {
    method: "PATCH",
    body: JSON.stringify({ points }),
  });
  expect(res.ok).toBe(true);
}

describe(Phases.SINGLE_FRESH, () => {
  it("seeds a collection whose candidate selection depends on token scores", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify(schemaFor(COLLECTION, true)),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/collections/${COLLECTION}/documents/import?action=create`, {
      method: "POST",
      body: SEED_DOCS,
    });
    expect(res.ok).toBe(true);
    const importBody = await res.text();
    expect(importBody.split("\n").every((l) => l.includes('"success":true'))).toBe(true);

    expect(await topHitId(COLLECTION)).toBe("0");
  });

  it("leaves a sort-only increase stale until a rebuild is asked for", async () => {
    // apricot outranks apple now, but nothing re-indexed its title
    await updatePoints(COLLECTION, "1", 1000);
    expect(await topHitId(COLLECTION)).toBe("0");

    const res = await rebuild(COLLECTION);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ rebuild_token_scores: true });

    expect(await topHitId(COLLECTION)).toBe("1");
  });

  it("brings scores back down, which the insert path can never do", async () => {
    await updatePoints(COLLECTION, "1", 1);

    // still stale-high at 1000 until the rebuild runs
    expect(await topHitId(COLLECTION)).toBe("1");

    const res = await rebuild(COLLECTION);
    expect(res.status).toBe(200);

    // apple at 100 is the highest live score again
    expect(await topHitId(COLLECTION)).toBe("0");
  });

  it("recovers the score a deleted document was holding", async () => {
    // the deleted document has to share its token with a survivor, otherwise the
    // leaf is removed outright and there is no stale score left to observe
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify(schemaFor(DELETE_COLLECTION, true)),
    });
    expect(res.ok).toBe(true);

    const docs = [
      JSON.stringify({ id: "0", title: "apple pie", points: 1000 }),
      JSON.stringify({ id: "1", title: "apple tart", points: 1 }),
      JSON.stringify({ id: "2", title: "apricot", points: 100 }),
    ].join("\n");

    res = await fetchSingleNode(`/collections/${DELETE_COLLECTION}/documents/import?action=create`, {
      method: "POST",
      body: docs,
    });
    expect(res.ok).toBe(true);

    // apple wins the only candidate slot at 1000 and brings both its documents
    res = await fetchSingleNode(`/collections/${DELETE_COLLECTION}${SEARCH}`);
    let body: any = await res.json();
    expect(body.hits.length).toBe(2);
    expect(body.hits[0].document.id).toBe("0");

    res = await fetchSingleNode(`/collections/${DELETE_COLLECTION}/documents/0`, { method: "DELETE" });
    expect(res.ok).toBe(true);

    // removing the document never lowered the leaf, so apple still claims 1000 and
    // keeps the slot even though the only apple document left scores 1
    res = await fetchSingleNode(`/collections/${DELETE_COLLECTION}${SEARCH}`);
    body = await res.json();
    expect(body.hits.length).toBe(1);
    expect(body.hits[0].document.id).toBe("1");

    res = await rebuild(DELETE_COLLECTION);
    expect(res.status).toBe(200);

    // apple drops to 1 and apricot at 100 takes the slot
    expect(await topHitId(DELETE_COLLECTION)).toBe("2");
  });

  it("covers store:false fields, which no update-time fix could reach", async () => {
    // a store:false field is stripped before the document is persisted, so there is
    // no text left to re-tokenize on a later update. walking the tree needs none.
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify(schemaFor(NOSTORE_COLLECTION, false)),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/collections/${NOSTORE_COLLECTION}/documents/import?action=create`, {
      method: "POST",
      body: SEED_DOCS,
    });
    expect(res.ok).toBe(true);

    expect(await topHitId(NOSTORE_COLLECTION)).toBe("0");

    await updatePoints(NOSTORE_COLLECTION, "1", 1000);
    expect(await topHitId(NOSTORE_COLLECTION)).toBe("0");

    res = await rebuild(NOSTORE_COLLECTION);
    expect(res.status).toBe(200);

    expect(await topHitId(NOSTORE_COLLECTION)).toBe("1");

    // and the title really is unstored
    res = await fetchSingleNode(`/collections/${NOSTORE_COLLECTION}${SEARCH}`);
    const body: any = await res.json();
    expect(body.hits[0].document.title).toBeUndefined();
  });

  it("rejects the parameter unless it is sent alone and without a body", async () => {
    let res = await fetchSingleNode(`/collections/${COLLECTION}?rebuild_token_scores=true`, {
      method: "PATCH",
      body: JSON.stringify({ metadata: { a: 1 } }),
    });
    expect(res.status).toBe(400);
    let body: any = await res.json();
    expect(body.message).toContain("without a request body");

    res = await fetchSingleNode(
      `/collections/${COLLECTION}?rebuild_token_scores=true&exclude_fields=title`,
      { method: "PATCH" },
    );
    expect(res.status).toBe(400);
    body = await res.json();
    expect(body.message).toContain("cannot be combined with other parameters");

    res = await fetchSingleNode(`/collections/${COLLECTION}?rebuild_token_scores=false`, {
      method: "PATCH",
    });
    expect(res.status).toBe(400);
    body = await res.json();
    expect(body.message).toContain("must be `true`");

    // a normal alter is untouched by any of this
    res = await fetchSingleNode(`/collections/${COLLECTION}`, {
      method: "PATCH",
      body: JSON.stringify({ metadata: { owner: "search" } }),
    });
    expect(res.status).toBe(200);
  });

  it("rejects a rebuild on a collection ordered by frequency", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: UNRANKED_COLLECTION,
        fields: [{ name: "title", type: "string" }],
      }),
    });
    expect(res.ok).toBe(true);

    // no default_sorting_field means max_score holds document counts, not scores
    res = await rebuild(UNRANKED_COLLECTION);
    expect(res.status).toBe(400);
    const body: any = await res.json();
    expect(body.message).toContain("default_sorting_field");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("scores are recomputed from the reloaded index", async () => {
    // a restart re-indexes every document, so scores start out correct
    expect(await topHitId(COLLECTION)).toBe("0");

    await updatePoints(COLLECTION, "1", 9000);
    expect(await topHitId(COLLECTION)).toBe("0");

    const res = await rebuild(COLLECTION);
    expect(res.status).toBe(200);

    expect(await topHitId(COLLECTION)).toBe("1");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("seeds the cluster", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify(schemaFor(MULTI_COLLECTION, true)),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, `/collections/${MULTI_COLLECTION}/documents/import?action=create`, {
      method: "POST",
      body: SEED_DOCS,
    });
    expect(res.ok).toBe(true);

    for(const node of [1, 2, 3]) {
      const searchRes = await fetchMultiNode(node, `/collections/${MULTI_COLLECTION}${SEARCH}`);
      expect(searchRes.ok).toBe(true);
      const body: any = await searchRes.json();
      expect(body.hits[0].document.id).toBe("0");
    }
  });

  it("a rebuild sent to one node repairs every node", async () => {
    // max_score is per node in-memory state, so this only works because the PATCH
    // route is a replicated write: the parameter has to survive the raft log
    let res = await fetchMultiNode(1, `/collections/${MULTI_COLLECTION}/documents/1`, {
      method: "PATCH",
      body: JSON.stringify({ points: 1000 }),
    });
    expect(res.ok).toBe(true);

    for(const node of [1, 2, 3]) {
      const searchRes = await fetchMultiNode(node, `/collections/${MULTI_COLLECTION}${SEARCH}`);
      const body: any = await searchRes.json();
      expect(body.hits[0].document.id).toBe("0");
    }

    res = await fetchMultiNode(1, `/collections/${MULTI_COLLECTION}?rebuild_token_scores=true`, {
      method: "PATCH",
    });
    expect(res.status).toBe(200);

    for(const node of [1, 2, 3]) {
      const searchRes = await fetchMultiNode(node, `/collections/${MULTI_COLLECTION}${SEARCH}`);
      expect(searchRes.ok).toBe(true);
      const body: any = await searchRes.json();
      expect(body.hits.length).toBe(1);
      expect(body.hits[0].document.id).toBe("1");
    }
  });
});
