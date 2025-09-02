import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode, fetchMultiNode } from "../src/request";
import { z } from "zod";

const CreateCollectionResponse = z.object({
  created_at: z.number(),
  default_sorting_field: z.string(),
  enable_nested_fields: z.boolean(),
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
  name: z.string(),
  num_documents: z.number(),
  symbols_to_index: z.array(z.string()),
  token_separators: z.array(z.string()),
});

const DocumentResponse = z.object({
  id: z.string(),
  content: z.string(),
});

const MultiSearchResponse = z.object({
  results: z.array(
    z.object({
      facet_counts: z.array(z.any()),
      found: z.number(),
      hits: z.array(
        z.object({
          document: z.object({
            content: z.string(),
            id: z.string(),
          }),
          highlight: z.record(z.any()),
          highlights: z.array(z.any()),
          vector_distance: z.number(),
        })
      ),
      out_of: z.number(),
      page: z.number(),
      request_params: z.object({
        collection_name: z.string(),
        first_q: z.string(),
        per_page: z.number(),
        q: z.string(),
      }),
      search_cutoff: z.boolean(),
      search_time_ms: z.number(),
    })
  ),
});

describe(Phases.SINGLE_FRESH, () => {
  it("create collection with tei embedding", async () => {
    const teiUrl = "http://localhost:8080";

    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "tei_test_collection",
        fields: [
          { name: "content", type: "string" },
          {
            name: "embedding",
            type: "float[]",
            embed: {
              from: ["content"],
              model_config: {
                model_name: "openai/bge-base-en-v1.5",
                api_key: "sk-1234567890abcdefghijklmnopqrstuvwxyz",
                url: teiUrl,
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("tei_test_collection");
  });

  it("adds  documents with tei embedding", async () => {
    let res: Response;
    let data: z.SafeParseReturnType<z.infer<typeof DocumentResponse>, z.infer<typeof DocumentResponse>>;

    res = await fetchSingleNode("/collections/tei_test_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "This is a test document about artificial intelligence and machine learning. It discusses various AI techniques and their applications in modern technology.",
        id: "doc1",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc1");

    res = await fetchSingleNode("/collections/tei_test_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "Another test document covering natural language processing and text embeddings. This document explains how vector embeddings work and their use in search applications.",
        id: "doc2",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc2");

    res = await fetchSingleNode("/collections/tei_test_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "A third document about data science and analytics. This covers topics like data preprocessing, statistical analysis, and visualization techniques.",
        id: "doc3",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc3");
  });

  it("perform vector search with tei embeddings", async () => {
    const res = await fetchSingleNode("/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "tei_test_collection",
            q: "machine learning algorithms",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 2)",
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const searchResult = MultiSearchResponse.safeParse(await res.json());
    expect(searchResult.success).toBe(true);

    if (searchResult.success) {
      expect(searchResult.data.results).toBeDefined();
      expect(searchResult.data.results[0]?.hits).toBeDefined();
      expect(searchResult.data.results[0]?.hits.length).toBeGreaterThan(0);
      expect(searchResult.data.results[0]?.found).toBeGreaterThan(0);

      const hits = searchResult.data.results[0]?.hits;
      expect(hits?.length).toBeGreaterThanOrEqual(2);

      hits?.forEach((hit) => {
        expect(hit.vector_distance).toBeDefined();
        expect(typeof hit.vector_distance).toBe("number");
        expect(hit.vector_distance).toBeGreaterThan(0);
      });
    }
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("get collection with tei embedding after restart", async () => {
    const res = await fetchSingleNode("/collections/tei_test_collection", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("tei_test_collection");
  });

  it("perform vector search with tei embeddings after restart", async () => {
    const res = await fetchSingleNode("/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "tei_test_collection",
            q: "machine learning algorithms",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 2)",
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const searchResult = MultiSearchResponse.safeParse(await res.json());
    expect(searchResult.success).toBe(true);

    if (searchResult.success) {
      expect(searchResult.data.results).toBeDefined();
      expect(searchResult.data.results[0]?.hits).toBeDefined();
      expect(searchResult.data.results[0]?.hits.length).toBeGreaterThan(0);
      expect(searchResult.data.results[0]?.found).toBeGreaterThan(0);

      const hits = searchResult.data.results[0]?.hits;
      expect(hits?.length).toBeGreaterThanOrEqual(2);

      hits?.forEach((hit) => {
        expect(hit.vector_distance).toBeDefined();
        expect(typeof hit.vector_distance).toBe("number");
        expect(hit.vector_distance).toBeGreaterThan(0);
      });
    }
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("get collection with tei embedding after snapshot", async () => {
    const res = await fetchSingleNode("/collections/tei_test_collection", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("tei_test_collection");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create collection with tei embedding in multi-node", async () => {
    const teiUrl = "http://localhost:8080";

    const res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "tei_multi_collection",
        fields: [
          { name: "content", type: "string" },
          {
            name: "embedding",
            type: "float[]",
            embed: {
              from: ["content"],
              model_config: {
                model_name: "openai/bge-base-en-v1.5",
                api_key: "sk-1234567890abcdefghijklmnopqrstuvwxyz",
                url: teiUrl,
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("tei_multi_collection");
  });

  it("adds  documents with tei embedding in multi-node", async () => {
    let res: Response;
    let data: z.SafeParseReturnType<z.infer<typeof DocumentResponse>, z.infer<typeof DocumentResponse>>;

    res = await fetchMultiNode(1, "/collections/tei_multi_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "This is a test document about artificial intelligence and machine learning. It discusses various AI techniques and their applications in modern technology.",
        id: "doc1",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc1");

    res = await fetchMultiNode(1, "/collections/tei_multi_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "Another test document covering natural language processing and text embeddings. This document explains how vector embeddings work and their use in search applications.",
        id: "doc2",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc2");

    res = await fetchMultiNode(1, "/collections/tei_multi_collection/documents", {
      method: "POST",
      body: JSON.stringify({
        content:
          "A third document about data science and analytics. This covers topics like data preprocessing, statistical analysis, and visualization techniques.",
        id: "doc3",
      }),
    });

    expect(res.ok).toBe(true);
    data = DocumentResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("doc3");
  });

  it("perform vector search with tei embeddings in multi-node", async () => {
    const res = await fetchMultiNode(1, "/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "tei_multi_collection",
            q: "machine learning algorithms",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 2)",
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const searchResult = MultiSearchResponse.safeParse(await res.json());
    expect(searchResult.success).toBe(true);

    if (searchResult.success) {
      expect(searchResult.data.results).toBeDefined();
      expect(searchResult.data.results[0]?.hits).toBeDefined();
      expect(searchResult.data.results[0]?.hits.length).toBeGreaterThan(0);
      expect(searchResult.data.results[0]?.found).toBeGreaterThan(0);
    }
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("get collection with tei embedding after multi-node restart", async () => {
    const res = await fetchMultiNode(2, "/collections/tei_multi_collection", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("tei_multi_collection");
  });

  it("perform vector search with tei embeddings after multi-node restart", async () => {
    const res = await fetchMultiNode(1, "/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "tei_multi_collection",
            q: "machine learning algorithms",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 2)",
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const searchResult = MultiSearchResponse.safeParse(await res.json());
    expect(searchResult.success).toBe(true);

    if (searchResult.success) {
      expect(searchResult.data.results).toBeDefined();
      expect(searchResult.data.results[0]?.hits).toBeDefined();
      expect(searchResult.data.results[0]?.hits.length).toBeGreaterThan(0);
      expect(searchResult.data.results[0]?.found).toBeGreaterThan(0);
    }
  });

  describe(Phases.MULTI_SNAPSHOT, () => {
    it("get collection with tei embedding after multi-node snapshot", async () => {
      const res = await fetchMultiNode(3, "/collections/tei_multi_collection", {
        method: "GET",
      });
      expect(res.ok).toBe(true);
      const data = CreateCollectionResponse.safeParse(await res.json());
      expect(data.success).toBe(true);
      expect(data.data?.name).toBe("tei_multi_collection");
    });
  });
});
