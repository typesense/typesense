import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { z } from "zod";
import { fetchMultiNode, fetchSingleNode } from "../src/request";

const DocumentSchema = z.object({
  id: z.string(),
  company_name: z.string(),
  num_employees: z.number(),
  country: z.string(),
});

const SearchHit = z.object({
  document: DocumentSchema,
  highlight: z.record(z.any()).optional(),
  text_match: z.number().optional(),
  text_match_info: z.record(z.any()).optional(),
  curated: z.boolean().optional(),
  hybrid_search_info: z.object({ rank_fusion_score: z.number().optional() }).optional(),
  vector_distance: z.number().optional(),
  geo_distance_meters: z.record(z.number()).optional(),
});

const FacetCountEntry = z.object({
  value: z.any().optional(),
  highlighted: z.string().optional(),
  count: z.number(),
  parent: z.any().optional(),
  facet_filter: z.string().optional(),
});

const FacetResult = z.object({
  field_name: z.string(),
  sampled: z.boolean().optional(),
  counts: z.array(FacetCountEntry),
});

const GroupedHits = z.object({
  group_key: z.array(z.any()),
  found: z.number().optional(),
  hits: z.array(SearchHit),
});

const SearchResponse = z.object({
  found: z.number(),
  out_of: z.number().optional(),
  found_docs: z.number().optional(),
  search_time_ms: z.number().optional(),
  page: z.number().optional(),
  hits: z.array(SearchHit).optional(),
  grouped_hits: z.array(GroupedHits).optional(),
  facet_counts: z.array(FacetResult),
  request_params: z.record(z.any()).optional(),
  parsed_nl_query: z.record(z.any()).optional(),
  union_request_params: z.array(z.record(z.any())).optional(),
});

describe(Phases.SINGLE_FRESH, () => {
  it("create documents", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies_docs_single",
        fields: [
          { name: "id", type: "string" },
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    res = await fetchSingleNode("/collections/companies_docs_single/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "1",
        company_name: "Stark Industries",
        num_employees: 10000,
        country: "US",
      }),
    });
    expect(res.ok).toBe(true);
    const d1 = DocumentSchema.safeParse(await res.json());
    expect(d1.success).toBe(true);
    expect(d1.data?.id).toBe("1");

    res = await fetchSingleNode("/collections/companies_docs_single/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "2",
        company_name: "Acme Corp",
        num_employees: 50,
        country: "DE",
      }),
    });
    expect(res.ok).toBe(true);
    const d2 = DocumentSchema.safeParse(await res.json());
    expect(d2.success).toBe(true);
    expect(d2.data?.id).toBe("2");
  });

  it("get a document by id", async () => {
    const res = await fetchSingleNode("/collections/companies_docs_single/documents/1", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.company_name).toBe("Stark Industries");
  });

  it("search documents", async () => {
    const res = await fetchSingleNode(
      "/collections/companies_docs_single/documents/search?q=stark&query_by=company_name",
      { method: "GET" }
    );
    expect(res.ok).toBe(true);
    const s = SearchResponse.safeParse(await res.json());
    expect(s.success).toBe(true);
    const hitsArray = s.data?.hits ?? s.data?.grouped_hits?.flatMap((g) => g.hits) ?? [];
    expect(hitsArray.length).toBeGreaterThan(0);
    expect(hitsArray[0]?.document.company_name.toLowerCase()).toContain("stark");
  });

  it("update a document via PATCH", async () => {
    const res = await fetchSingleNode("/collections/companies_docs_single/documents/1", {
      method: "PATCH",
      body: JSON.stringify({ num_employees: 12000 }),
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.num_employees).toBe(12000);
  });

  it("delete a document by id", async () => {
    const res = await fetchSingleNode("/collections/companies_docs_single/documents/2", {
      method: "DELETE",
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.id).toBe("2");
  });

  it("import documents with upsert", async () => {
    const jsonl = [
      JSON.stringify({ id: "1", company_name: "Stark Industries", num_employees: 13000, country: "US" }),
      JSON.stringify({ id: "3", company_name: "Umbrella Corp", num_employees: 100, country: "US" }),
    ].join("\n");

    const res = await fetchSingleNode(
      "/collections/companies_docs_single/documents/import?action=upsert",
      {
        method: "POST",
        body: jsonl,
      }
    );
    expect(res.ok).toBe(true);

    let getRes = await fetchSingleNode(
      "/collections/companies_docs_single/documents/1",
      { method: "GET" }
    );
    expect(getRes.ok).toBe(true);
    let doc1 = DocumentSchema.safeParse(await getRes.json());
    expect(doc1.success).toBe(true);
    expect(doc1.data?.num_employees).toBe(13000);

    getRes = await fetchSingleNode(
      "/collections/companies_docs_single/documents/3",
      { method: "GET" }
    );
    expect(getRes.ok).toBe(true);
    const doc3 = DocumentSchema.safeParse(await getRes.json());
    expect(doc3.success).toBe(true);
    expect(doc3.data?.company_name).toBe("Umbrella Corp");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("get a created document after restart", async () => {
    const res = await fetchSingleNode("/collections/companies_docs_single/documents/1", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.id).toBe("1");
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("get a created document after snapshot", async () => {
    const res = await fetchSingleNode("/collections/companies_docs_single/documents/1", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.id).toBe("1");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create and read documents across nodes", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies_docs_multi",
        fields: [
          { name: "id", type: "string" },
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true },
        ],
      }),
    });
    expect(res.ok).toBe(true);
    res = await fetchMultiNode(1, "/collections/companies_docs_multi/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "m1",
        company_name: "Wayne Enterprises",
        num_employees: 5000,
        country: "US",
      }),
    });
    expect(res.ok).toBe(true);
    let d1 = DocumentSchema.safeParse(await res.json());
    expect(d1.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/companies_docs_multi/documents/m1", { method: "GET" });
    expect(res.ok).toBe(true);
    const dGet = DocumentSchema.safeParse(await res.json());
    expect(dGet.success).toBe(true);

    res = await fetchMultiNode(
      3,
      "/collections/companies_docs_multi/documents/search?q=wayne&query_by=company_name",
      { method: "GET" }
    );
    expect(res.ok).toBe(true);
    const s = SearchResponse.safeParse(await res.json());
    expect(s.success).toBe(true);
    const hitsArray = s.data?.hits ?? s.data?.grouped_hits?.flatMap((g) => g.hits) ?? [];
    expect(hitsArray.length).toBeGreaterThan(0);
  });

  it("import documents with upsert", async () => {
    const jsonl = [
      JSON.stringify({ id: "m1", company_name: "Wayne Enterprises", num_employees: 6000, country: "US" }),
      JSON.stringify({ id: "m3", company_name: "Cyberdyne Systems", num_employees: 300, country: "US" }),
    ].join("\n");

    const res = await fetchMultiNode(
      1,
      "/collections/companies_docs_multi/documents/import?action=upsert",
      { method: "POST", body: jsonl }
    );
    expect(res.ok).toBe(true);

    let getRes = await fetchMultiNode(2, "/collections/companies_docs_multi/documents/m1", { method: "GET" });
    expect(getRes.ok).toBe(true);
    let doc1 = DocumentSchema.safeParse(await getRes.json());
    expect(doc1.success).toBe(true);
    expect(doc1.data?.num_employees).toBe(6000);

    getRes = await fetchMultiNode(3, "/collections/companies_docs_multi/documents/m3", { method: "GET" });
    expect(getRes.ok).toBe(true);
    const doc3 = DocumentSchema.safeParse(await getRes.json());
    expect(doc3.success).toBe(true);
    expect(doc3.data?.company_name).toBe("Cyberdyne Systems");
  });

  it("delete document across nodes", async () => {
    const res = await fetchMultiNode(1, "/collections/companies_docs_multi/documents/m1", {
      method: "DELETE",
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.id).toBe("m1");
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("create and persist a document post-restart", async () => {
    let res = await fetchMultiNode(1, "/collections/companies_docs_multi/documents", {
      method: "POST",
      body: JSON.stringify({ id: "m2", company_name: "Oscorp", num_employees: 800, country: "US" }),
    });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);

    res = await fetchMultiNode(2, "/collections/companies_docs_multi/documents/m2", { method: "GET" });
    expect(res.ok).toBe(true);
    const d2 = DocumentSchema.safeParse(await res.json());
    expect(d2.success).toBe(true);
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("get a created document after snapshot", async () => {
    const res = await fetchMultiNode(3, "/collections/companies_docs_multi/documents/m2", { method: "GET" });
    expect(res.ok).toBe(true);
    const d = DocumentSchema.safeParse(await res.json());
    expect(d.success).toBe(true);
    expect(d.data?.id).toBe("m2");
  });
});


