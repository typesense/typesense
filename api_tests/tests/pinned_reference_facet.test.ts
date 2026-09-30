import { afterAll, beforeAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { createServer } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { TypesenseProcessManager } from "../src/manager";

setDefaultTimeout(20_000);

const COLLECTION = "pinned_facet_listings";
const OFFERS = "pinned_facet_offers";
const CURATION_SET = "pinned_facet_curations";
const FILTER_BY = `$${OFFERS}(in_stock:=true && price:>0) && is_hidden:=0`;
const FACET_BY = `price, $${OFFERS}(price)`;
const API_KEY = "xyz";
const REQUEST_TIMEOUT_MS = 5_000;

type FacetCount = { value: string; count: number };
type SearchResponse = {
  found: number;
  hits: Array<{ document: { id: string }; curated?: boolean }>;
  facet_counts: Array<{
    field_name: string;
    counts: FacetCount[];
    stats?: { avg: number; min: number; max: number; sum: number; total_values: number };
  }>;
};

let baseDir = "";
let apiPort = 0;
let peeringPort = 0;
let manager: TypesenseProcessManager | undefined;

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Could not allocate a local test port.");
  }
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function apiRequest(path: string, init: RequestInit = {}, timeoutMs = REQUEST_TIMEOUT_MS): Promise<Response> {
  return fetch(`http://127.0.0.1:${apiPort}${path}`, {
    ...init,
    headers: {
      "X-TYPESENSE-API-KEY": API_KEY,
      ...(init.headers ?? {}),
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
}

async function postJson(path: string, body: unknown): Promise<Response> {
  return apiRequest(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

function facetCounts(counts: FacetCount[]): Record<string, number> {
  return Object.fromEntries(counts.map(({ value, count }) => [value, count]));
}

function assertSearch(response: SearchResponse, expectedIds: string[], checkHitOrder = true) {
  expect(response.found).toBe(expectedIds.length);
  const hitIds = response.hits.map((hit) => hit.document.id);
  expect([...hitIds].sort()).toEqual([...expectedIds].sort());
  if (checkHitOrder) expect(hitIds).toEqual(expectedIds);
  expect(response.facet_counts.map((facet) => facet.field_name)).toEqual([
    "price",
    `$${OFFERS}(price)`,
  ]);
  expect(facetCounts(response.facet_counts[0].counts)).toEqual({
    "1010": 1,
    "1020": 1,
    "1030": 1,
  });
  expect(facetCounts(response.facet_counts[1].counts)).toEqual({
    "10": 1,
    "20": 1,
    "30": 1,
  });
  expect(response.facet_counts[1].stats).toEqual({
    avg: 20,
    min: 10,
    max: 30,
    sum: 60,
    total_values: 3,
  });
}

describe(Phases.NO_PHASE, () => {
  beforeAll(async () => {
    baseDir = mkdtempSync(join(tmpdir(), "typesense-pinned-facet-api-"));
    apiPort = await reservePort();
    peeringPort = await reservePort();
    const binaryPath = process.env.TYPESENSE_BINARY_PATH ?? join(process.cwd(), "bazel-bin", "typesense-server");
    manager = new TypesenseProcessManager(baseDir, binaryPath);
    await manager.startSingleNode("data", apiPort, peeringPort, "pinned-facet-api");
  });

  afterAll(async () => {
    await manager?.shutdown();
    if (baseDir) rmSync(baseDir, { recursive: true, force: true });
  });

  it("returns pinned reference facets through multi-search and direct search", async () => {
    let res = await apiRequest(`/curation_sets/${CURATION_SET}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        items: [{
          id: "include-l1",
          rule: { query: "backpack blue", match: "exact" },
          includes: [{ id: "l1", position: 1 }],
          filter_curated_hits: true,
          remove_matched_tokens: true,
        }],
      }),
    });
    expect(res.status).toBe(200);

    res = await postJson("/collections", {
      name: COLLECTION,
      curation_sets: [CURATION_SET],
      fields: [
        { name: "listing_id", type: "string" },
        { name: "title", type: "string" },
        { name: "is_hidden", type: "int32" },
        { name: "price", type: "int32", facet: true },
      ],
    });
    expect(res.status).toBe(201);

    res = await postJson("/collections", {
      name: OFFERS,
      fields: [
        { name: "listing_ref", type: "string", reference: `${COLLECTION}.listing_id` },
        { name: "in_stock", type: "bool" },
        { name: "price", type: "int32", facet: true },
      ],
    });
    expect(res.status).toBe(201);

    for (const listing of [
      { id: "l1", listing_id: "l1", title: "backpack red", is_hidden: 0, price: 1010 },
      { id: "l2", listing_id: "l2", title: "backpack blue", is_hidden: 0, price: 1020 },
      { id: "l3", listing_id: "l3", title: "backpack green", is_hidden: 0, price: 1030 },
    ]) {
      res = await postJson(`/collections/${COLLECTION}/documents`, listing);
      expect(res.status).toBe(201);
    }

    for (const offer of [
      { listing_ref: "l1", in_stock: true, price: 10 },
      { listing_ref: "l2", in_stock: true, price: 20 },
      { listing_ref: "l3", in_stock: true, price: 30 },
    ]) {
      res = await postJson(`/collections/${OFFERS}/documents`, offer);
      expect(res.status).toBe(201);
    }

    const unpinnedMultiSearch = await apiRequest("/multi_search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        searches: [{
          collection: COLLECTION,
          q: "backpack",
          query_by: "title",
          filter_by: FILTER_BY,
          facet_by: FACET_BY,
        }],
      }),
    });
    expect(unpinnedMultiSearch.status).toBe(200);
    const unpinnedBody = await unpinnedMultiSearch.json() as { results: SearchResponse[] };
    assertSearch(unpinnedBody.results[0], ["l1", "l2", "l3"], false);

    const pinnedSearch = {
      collection: COLLECTION,
      q: "backpack",
      query_by: "title",
      filter_by: FILTER_BY,
      facet_by: FACET_BY,
      pinned_hits: "l1:1,l2:2",
      filter_curated_hits: true,
    };
    const multiSearch = await apiRequest("/multi_search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ searches: [pinnedSearch] }),
    });
    expect(multiSearch.status).toBe(200);
    const multiSearchBody = await multiSearch.json() as { results: SearchResponse[] };
    assertSearch(multiSearchBody.results[0], ["l1", "l2", "l3"]);

    const directSearchParams = new URLSearchParams({
      q: "backpack",
      query_by: "title",
      filter_by: FILTER_BY,
      facet_by: FACET_BY,
      pinned_hits: "l1:1,l2:2",
      filter_curated_hits: "true",
    });
    const directSearch = await apiRequest(`/collections/${COLLECTION}/documents/search?${directSearchParams}`);
    expect(directSearch.status).toBe(200);
    assertSearch(await directSearch.json() as SearchResponse, ["l1", "l2", "l3"]);

    const curatedParams = new URLSearchParams({
      q: "backpack blue",
      query_by: "title",
      filter_by: FILTER_BY,
      facet_by: FACET_BY,
    });
    const curatedSearch = await apiRequest(`/collections/${COLLECTION}/documents/search?${curatedParams}`);
    expect(curatedSearch.status).toBe(200);
    const curatedBody = await curatedSearch.json() as SearchResponse;
    assertSearch(curatedBody, ["l1", "l2", "l3"], false);
    expect(curatedBody.hits[0].curated).toBe(true);

    const repeatedSearch = await apiRequest("/multi_search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ searches: [pinnedSearch] }),
    });
    expect(repeatedSearch.status).toBe(200);
    const repeatedBody = await repeatedSearch.json() as { results: SearchResponse[] };
    assertSearch(repeatedBody.results[0], ["l1", "l2", "l3"]);
  });

  it("returns pinned reference-only facets through multi-search and direct search", async () => {
    const listingsCollection = "reference_only_listings";
    const offersCollection = "reference_only_offers";
    let res = await postJson("/collections", {
      name: listingsCollection,
      fields: [
        { name: "listing_id", type: "string" },
        { name: "title", type: "string" },
        { name: "is_hidden", type: "int32" },
      ],
    });
    expect(res.status).toBe(201);

    res = await postJson("/collections", {
      name: offersCollection,
      fields: [
        { name: "listing_ref", type: "string", reference: `${listingsCollection}.listing_id` },
        { name: "in_stock", type: "bool" },
        { name: "price", type: "int32", facet: true },
      ],
    });
    expect(res.status).toBe(201);

    for (const listing of [
      { id: "r1", listing_id: "r1", title: "backpack red", is_hidden: 0 },
      { id: "r2", listing_id: "r2", title: "backpack blue", is_hidden: 0 },
      { id: "r3", listing_id: "r3", title: "backpack green", is_hidden: 0 },
    ]) {
      res = await postJson(`/collections/${listingsCollection}/documents`, listing);
      expect(res.status).toBe(201);
    }

    for (const offer of [
      { listing_ref: "r1", in_stock: true, price: 10 },
      { listing_ref: "r2", in_stock: true, price: 20 },
      { listing_ref: "r3", in_stock: true, price: 30 },
    ]) {
      res = await postJson(`/collections/${offersCollection}/documents`, offer);
      expect(res.status).toBe(201);
    }

    const filterBy = `$${offersCollection}(in_stock:=true && price:>0) && is_hidden:=0`;
    const facetBy = `$${offersCollection}(price)`;
    const searches = [
      apiRequest("/multi_search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          searches: [{
            collection: listingsCollection,
            q: "backpack",
            query_by: "title",
            filter_by: filterBy,
            facet_by: facetBy,
            pinned_hits: "r1:1,r2:2",
          }],
        }),
      }),
      apiRequest(`/collections/${listingsCollection}/documents/search?${new URLSearchParams({
        q: "backpack",
        query_by: "title",
        filter_by: filterBy,
        facet_by: facetBy,
        pinned_hits: "r1:1,r2:2",
      })}`),
    ];
    const responses = await Promise.allSettled(searches);
    expect(responses.map((response) => response.status)).toEqual(["fulfilled", "fulfilled"]);

    const multiSearch = responses[0];
    const directSearch = responses[1];
    if (multiSearch.status !== "fulfilled" || directSearch.status !== "fulfilled") {
      throw new Error("Pinned reference-only facet requests did not both receive HTTP responses.");
    }
    expect(multiSearch.value.status).toBe(200);
    expect(directSearch.value.status).toBe(200);

    const multiSearchBody = await multiSearch.value.json() as { results: SearchResponse[] };
    const searchResults = [multiSearchBody.results[0], await directSearch.value.json() as SearchResponse];
    for (const result of searchResults) {
      expect(result.found).toBe(3);
      expect(result.hits.map((hit) => hit.document.id)).toEqual(["r1", "r2", "r3"]);
      expect(result.facet_counts.map((facet) => facet.field_name)).toEqual([`$${offersCollection}(price)`]);
      expect(facetCounts(result.facet_counts[0].counts)).toEqual({ "10": 1, "20": 1, "30": 1 });
      expect(result.facet_counts[0].stats).toEqual({
        avg: 20,
        min: 10,
        max: 30,
        sum: 60,
        total_values: 3,
      });
    }
  });
});
