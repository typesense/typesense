import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { z } from "zod";
import { fetchMultiNode, fetchSingleNode } from "../src/request";

const SynonymObject = z.object({
  id: z.string(),
  root: z.string(),
  synonyms: z.array(z.string()),
  locale: z.string().optional(),
  symbols_to_index: z.array(z.string()).optional(),
});

const SynonymSetListItemResponse = z.array(SynonymObject);

const SynonymSetResponse = z.object({
  name: z.string(),
  items: z.array(SynonymObject),
});

const SynonymSetListResponse = z.array(SynonymSetResponse);

const SynonymSetDeleteResponse = z.object({
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
  synonym_sets: z.array(z.string()).optional(),
});

const PatchCollectionSynonymSetsResponse = z.object({
  synonym_sets: z.array(z.string()),
});

const initialSynonyms = [
  { id: "syn-tv", root: "tv", synonyms: ["television", "smart tv"] },
  { id: "syn-usa", root: "usa", synonyms: ["united states", "united states of america"] },
  { id: "syn-laptop", root: "laptop", synonyms: ["notebook", "ultrabook"] },
];

const updatedSynonyms = [
  { id: "syn-phone", root: "phone", synonyms: ["cellphone", "mobile", "smartphone"] },
  { id: "syn-monitor", root: "monitor", synonyms: ["display", "screen"] },
];

describe(Phases.SINGLE_FRESH, () => {
  it("create a synonym set", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "products",
        fields: [
          { name: "id", type: "string" },
          { name: "product_name", type: "string" },
          { name: "category", type: "string" },
        ],
      }),
    });
    expect(res.ok).toBe(true);
    res = await fetchSingleNode("/synonym_sets/products-core", {
      method: "PUT",
      body: JSON.stringify({ items: initialSynonyms }),
    });
    expect(res.ok).toBe(true);
    const syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);
    expect(syn.data?.items.length).toBe(3);
    const ids = syn.data?.items.map((s) => s.id);
    expect(ids).toContain("syn-tv");
    expect(ids).toContain("syn-usa");
    expect(ids).toContain("syn-laptop");
  });

  it("list synonym sets", async () => {
    const res = await fetchSingleNode("/synonym_sets", { method: "GET" });
    expect(res.ok).toBe(true);
    const synList = SynonymSetListResponse.safeParse(await res.json());
    expect(synList.success).toBe(true);
    const setNames = synList.data?.map((s) => s.name);
    expect(setNames).toContain("products-core");
  });

  it("list synonym items in a set", async () => {
    const res = await fetchSingleNode("/synonym_sets/products-core/items?limit=10&offset=0", { method: "GET" });
    expect(res.ok).toBe(true);
    const list = SynonymSetListItemResponse.safeParse(await res.json());
    expect(list.success).toBe(true);
    const ids = list.data?.map((s) => s.id);
    expect(ids).toContain("syn-tv");
  });

  it("get a synonym item by id", async () => {
    const res = await fetchSingleNode("/synonym_sets/products-core/items/syn-tv", { method: "GET" });
    expect(res.ok).toBe(true);
    const item = SynonymObject.safeParse(await res.json());
    expect(item.success).toBe(true);
    expect(item.data?.id).toBe("syn-tv");
  });

  it("upsert and delete a synonym item", async () => {
    let res = await fetchSingleNode("/synonym_sets/products-core/items/syn-extra", {
      method: "PUT",
      body: JSON.stringify({ root: "extra", synonyms: ["bonus"] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/synonym_sets/products-core/items/syn-extra", { method: "GET" });
    expect(res.ok).toBe(true);
    const item = SynonymObject.safeParse(await res.json());
    expect(item.success).toBe(true);
    expect(item.data?.id).toBe("syn-extra");

    res = await fetchSingleNode("/synonym_sets/products-core/items/syn-extra", { method: "DELETE" });
    expect(res.ok).toBe(true);
  });

  it("get a synonym set", async () => {
    const res = await fetchSingleNode("/synonym_sets/products-core", { method: "GET" });
    expect(res.ok).toBe(true);
    const syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);
    expect(syn.data?.items.length).toBe(3);
  });

  it("create a collection with synonym_sets", async () => {
    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "products_with_synonyms",
        fields: [
          { name: "id", type: "string" },
          { name: "product_name", type: "string" },
          { name: "category", type: "string" },
        ],
        synonym_sets: ["products-core"],
      }),
    });
    expect(res.ok).toBe(true);
  });

  it("attach synonym set to collection", async () => {
    const res = await fetchSingleNode("/collections/products", {
      method: "PATCH",
      body: JSON.stringify({ synonym_sets: ["products-core"] }),
    });
    expect(res.ok).toBe(true);
    const patch = PatchCollectionSynonymSetsResponse.safeParse(await res.json());
    expect(patch.success).toBe(true);
    expect(patch.data?.synonym_sets).toContain("products-core");
  });

  it("get collection reflects synonym_sets", async () => {
    const res = await fetchSingleNode("/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.name).toBe("products");
    expect(coll.data?.synonym_sets).toContain("products-core");
  });

  it("update synonym set contents", async () => {
    const res = await fetchSingleNode("/synonym_sets/products-core", {
      method: "PUT",
      body: JSON.stringify({ items: updatedSynonyms }),
    });
    expect(res.ok).toBe(true);
    const syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);
    const updatedIds = syn.data?.items.map((s) => s.id);
    expect(updatedIds).toContain("syn-phone");
    expect(updatedIds).toContain("syn-monitor");
  });

  it("delete a temporary synonym set", async () => {
    let res = await fetchSingleNode("/synonym_sets/products-temp", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "syn-temp", root: "temp", synonyms: ["temporary"] }] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/synonym_sets/products-temp", { method: "DELETE" });
    expect(res.ok).toBe(true);
    const del = SynonymSetDeleteResponse.safeParse(await res.json());
    expect(del.success).toBe(true);
    expect(del.data?.name).toBe("products-temp");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("synonym set and collection references persist", async () => {
    let res = await fetchSingleNode("/synonym_sets/products-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);
    const ids = syn.data?.items.map((s) => s.id);
    expect(ids).toContain("syn-phone");
    expect(ids).toContain("syn-monitor");

    res = await fetchSingleNode("/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.synonym_sets).toContain("products-core");
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("synonym set and collection references persist in snapshot", async () => {
    let res = await fetchSingleNode("/synonym_sets/products-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);

    res = await fetchSingleNode("/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.synonym_sets).toContain("products-core");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create synonym sets", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "products",
        fields: [
          { name: "id", type: "string" },
          { name: "product_name", type: "string" },
          { name: "category", type: "string" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, "/synonym_sets/products-core-2", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "syn-x", root: "x", synonyms: ["ex"] }] }),
    });
    expect(res.ok).toBe(true);
    let syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);

    res = await fetchMultiNode(1, "/synonym_sets/products-core", {
      method: "PUT",
      body: JSON.stringify({ items: [{ id: "syn-y", root: "y", synonyms: ["ey"] }] }),
    });
    expect(res.ok).toBe(true);
    syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);
  });

  it("list synonym sets", async () => {
    const res = await fetchMultiNode(3, "/synonym_sets", { method: "GET" });
    expect(res.ok).toBe(true);
    const list = SynonymSetListResponse.safeParse(await res.json());
    expect(list.success).toBe(true);
    const setNames = list.data?.map((s) => s.name);
    expect(setNames).toContain("products-core-2");
    expect(setNames).toContain("products-core");
  });

  it("attach both sets to products", async () => {
    const res = await fetchMultiNode(1, "/collections/products", {
      method: "PATCH",
      body: JSON.stringify({ synonym_sets: ["products-core", "products-core-2"] }),
    });
    expect(res.ok).toBe(true);
    const patch = PatchCollectionSynonymSetsResponse.safeParse(await res.json());
    expect(patch.success).toBe(true);
    expect(patch.data?.synonym_sets).toContain("products-core-2");
  });

  it("get collection reflects synonym_sets", async () => {
    const res = await fetchMultiNode(2, "/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.synonym_sets).toContain("products-core");
    expect(coll.data?.synonym_sets).toContain("products-core-2");
  });

  it("detach and delete synonym set", async () => {
    let res = await fetchMultiNode(1, "/collections/products", {
      method: "PATCH",
      body: JSON.stringify({ synonym_sets: ["products-core"] }),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, "/synonym_sets/products-core-2", { method: "DELETE" });
    expect(res.ok).toBe(true);
    const del = SynonymSetDeleteResponse.safeParse(await res.json());
    expect(del.success).toBe(true);
    expect(del.data?.name).toBe("products-core-2");
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("synonym sets persist across nodes after restart", async () => {
    let res = await fetchMultiNode(1, "/synonym_sets/products-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.synonym_sets).toContain("products-core");
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("synonym sets persist across nodes after snapshot", async () => {
    let res = await fetchMultiNode(3, "/synonym_sets/products-core", { method: "GET" });
    expect(res.ok).toBe(true);
    let syn = SynonymSetResponse.safeParse(await res.json());
    expect(syn.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/products", { method: "GET" });
    expect(res.ok).toBe(true);
    const coll = CollectionSummaryResponse.safeParse(await res.json());
    expect(coll.success).toBe(true);
    expect(coll.data?.synonym_sets).toContain("products-core");
  });
});


