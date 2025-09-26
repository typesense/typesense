import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { z } from "zod";
import { fetchMultiNode, fetchSingleNode } from "../src/request";

const CurationObject = z.object({
  id: z.string(),
  rule: z.object({
    query: z.string().optional(),
    match: z.string().optional(),
    filter_by: z.string().optional(),
    tags: z.array(z.string()).optional(),
  }).optional(),
  includes: z.array(z.object({ id: z.string(), position: z.number() })).optional(),
  excludes: z.array(z.object({ id: z.string() })).optional(),
  filter_by: z.string().optional(),
  sort_by: z.string().optional(),
  replace_query: z.string().optional(),
  remove_matched_tokens: z.boolean().optional(),
  filter_curated_hits: z.boolean().optional(),
  stop_processing: z.boolean().optional(),
  metadata: z.record(z.any()).optional(),
});

const CurationSetResponse = z.object({
  name: z.string().optional(),
  items: z.array(CurationObject),
});

const CurationSetListResponse = z.array(z.object({
  name: z.string(),
  items: z.array(CurationObject),
}));

const CurationSetListItemResponse = z.array(CurationObject);

const CurationSetDeleteResponse = z.object({
  name: z.string(),
});

const CollectionSummaryResponse = z.object({
  name: z.string(),
  num_documents: z.number(),
  fields: z.array(
    z.object({
      facet: z.boolean(),
      index: z.boolean(),
      infix: z.boolean(),
      locale: z.string(),
      name: z.string(),
      optional: z.boolean(),
      sort: z.boolean(),
      stem: z.boolean(),
      stem_dictionary: z.string(),
      store: z.boolean(),
      type: z.string(),
    })
  ),
  curation_sets: z.array(z.string()).optional(),
});

const PatchCollectionCurationSetsResponse = z.object({
  curation_sets: z.array(z.string()),
});

const initialCurations = [
  { id: "ov-pin-romance", rule: { query: "romantic", match: "contains" }, includes: [{ id: "1", position: 1 }] },
  { id: "ov-drop-scifi", rule: { query: "sci-fi", match: "contains" }, excludes: [{ id: "2" }] },
];

const updatedCurations = [
  { id: "ov-pin-thriller", rule: { query: "thriller", match: "exact" }, includes: [{ id: "3", position: 1 }] },
];

describe(Phases.SINGLE_FRESH, () => {
  it("create an curation set", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "movies",
        fields: [
          { name: "id", type: "string" },
          { name: "title", type: "string" },
          { name: "points", type: "int32" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/curation_sets/movies-core", {
      method: "PUT",
      body: JSON.stringify({ items: initialCurations }),
    });
    expect(res.ok).toBe(true);
    const ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);
    expect(ov.data?.items.length).toBe(2);
  });

  it("list curation sets", async () => {
    const res = await fetchSingleNode("/curation_sets", { method: "GET" });
    expect(res.ok).toBe(true);
    const list = CurationSetListResponse.safeParse(await res.json());
    expect(list.success).toBe(true);
    const names = list.data?.map((s) => s.name);
    expect(names).toContain("movies-core");
  });

  it("list curation items in a set", async () => {
    const res = await fetchSingleNode("/curation_sets/movies-core/items?limit=10&offset=0", { method: "GET" });
    expect(res.ok).toBe(true);
    const list = CurationSetListItemResponse.safeParse(await res.json());
    expect(list.success).toBe(true);
    const ids = list.data?.map((i) => i.id);
    expect(ids).toContain("ov-pin-romance");
  });

  it("get an curation item by id", async () => {
    const res = await fetchSingleNode("/curation_sets/movies-core/items/ov-pin-romance", { method: "GET" });
    expect(res.ok).toBe(true);
    const item = CurationObject.safeParse(await res.json());
    expect(item.success).toBe(true);
    expect(item.data?.id).toBe("ov-pin-romance");
  });

  it("upsert and delete an curation item", async () => {
    let res = await fetchSingleNode("/curation_sets/movies-core/items/ov-extra", {
      method: "PUT",
      body: JSON.stringify({ rule: { query: "extra", match: "exact" }, includes: [{ id: "1", position: 2 }] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/curation_sets/movies-core/items/ov-extra", { method: "GET" });
    expect(res.ok).toBe(true);
    const item = CurationObject.safeParse(await res.json());
    expect(item.success).toBe(true);
    expect(item.data?.id).toBe("ov-extra");

    res = await fetchSingleNode("/curation_sets/movies-core/items/ov-extra", { method: "DELETE" });
    expect(res.ok).toBe(true);
  });

  it("get an curation set", async () => {
    const res = await fetchSingleNode("/curation_sets/movies-core", { method: "GET" });
    expect(res.ok).toBe(true);
    const ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);
    expect(ov.data?.items.length).toBe(2);
  });

  it("create a collection with curation_sets", async () => {
    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "movies_with_curations",
        fields: [
          { name: "id", type: "string" },
          { name: "title", type: "string" },
          { name: "points", type: "int32" },
        ],
        curation_sets: ["movies-core"],
      }),
    });
    expect(res.ok).toBe(true);
  });

  it("attach curation set to collection", async () => {
    const res = await fetchSingleNode("/collections/movies", {
      method: "PATCH",
      body: JSON.stringify({ curation_sets: ["movies-core"] }),
    });
    expect(res.ok).toBe(true);
    const patch = PatchCollectionCurationSetsResponse.safeParse(await res.json());
    expect(patch.success).toBe(true);
    expect(patch.data?.curation_sets).toContain("movies-core");
  });

  it("get collection reflects curation_sets", async () => {
    const res = await fetchSingleNode("/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.name).toBe("movies");
    expect(coll.data?.curation_sets).toContain("movies-core");
  });

  it("update curation set contents", async () => {
    const res = await fetchSingleNode("/curation_sets/movies-core", {
      method: "PUT",
      body: JSON.stringify({ items: updatedCurations }),
    });
    expect(res.ok).toBe(true);
    const ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);
    const updatedIds = ov.data?.items.map((o) => o.id);
    expect(updatedIds).toContain("ov-pin-thriller");
  });

  it("delete a temporary curation set", async () => {
    let res = await fetchSingleNode("/curation_sets/movies-temp", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "ov-temp", rule: { query: "temp", match: "exact" }, includes: [{ id: "1", position: 1 }] }] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/curation_sets/movies-temp", { method: "DELETE" });
    expect(res.ok).toBe(true);
    const del = CurationSetDeleteResponse.safeParse(await res.json());
    expect(del.success).toBe(true);
    expect(del.data?.name).toBe("movies-temp");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("curation set and collection references persist", async () => {
    let res = await fetchSingleNode("/curation_sets/movies-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);
    const ids = ov.data?.items.map((o) => o.id);
    expect(ids).toContain("ov-pin-thriller");

    res = await fetchSingleNode("/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.curation_sets).toContain("movies-core");
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("curation set and collection references persist in snapshot", async () => {
    let res = await fetchSingleNode("/curation_sets/movies-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);

    res = await fetchSingleNode("/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.curation_sets).toContain("movies-core");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create curation sets", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "movies",
        fields: [
          { name: "id", type: "string" },
          { name: "title", type: "string" },
          { name: "points", type: "int32" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, "/curation_sets/movies-core-2", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "ov-x", rule: { query: "x", match: "exact" }, includes: [{ id: "1", position: 1 }] }] }),
    });
    expect(res.ok).toBe(true);
    let ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);

    res = await fetchMultiNode(1, "/curation_sets/movies-core", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "ov-y", rule: { query: "y", match: "exact" }, includes: [{ id: "1", position: 1 }] }] }),
    });
    expect(res.ok).toBe(true);
    ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);
  });

  it("list curation sets", async () => {
    const res = await fetchMultiNode(3, "/curation_sets", { method: "GET" });
    expect(res.ok).toBe(true);
    const list = CurationSetListResponse.safeParse(await res.json());
    expect(list.success).toBe(true);
    const setNames = list.data?.map((s) => s.name);
    expect(setNames).toContain("movies-core-2");
    expect(setNames).toContain("movies-core");
  });

  it("attach both sets to movies", async () => {
    const res = await fetchMultiNode(1, "/collections/movies", {
      method: "PATCH",
      body: JSON.stringify({ curation_sets: ["movies-core", "movies-core-2"] }),
    });
    expect(res.ok).toBe(true);
    const patch = PatchCollectionCurationSetsResponse.safeParse(await res.json());
    expect(patch.success).toBe(true);
    expect(patch.data?.curation_sets).toContain("movies-core-2");
  });

  it("get collection reflects curation_sets", async () => {
    const res = await fetchMultiNode(2, "/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.curation_sets).toContain("movies-core");
    expect(coll.data?.curation_sets).toContain("movies-core-2");
  });

  it("detach and delete curation set", async () => {
    let res = await fetchMultiNode(1, "/collections/movies", {
      method: "PATCH",
      body: JSON.stringify({ curation_sets: ["movies-core"] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, "/curation_sets/movies-core-2", { method: "DELETE" });
    expect(res.ok).toBe(true);
    const del = CurationSetDeleteResponse.safeParse(await res.json());
    expect(del.success).toBe(true);
    expect(del.data?.name).toBe("movies-core-2");
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("curation sets persist across nodes after restart", async () => {
    let res = await fetchMultiNode(1, "/curation_sets/movies-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.curation_sets).toContain("movies-core");
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("curation sets persist across nodes after snapshot", async () => {
    let res = await fetchMultiNode(3, "/curation_sets/movies-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let ov = CurationSetResponse.safeParse(await res.json());
    expect(ov.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/movies", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.curation_sets).toContain("movies-core");
  });
});
