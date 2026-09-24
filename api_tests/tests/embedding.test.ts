import { describe, it, expect } from "bun:test";
import { Phases, Filters } from "../src/constants";
import { z } from "zod";
import { fetchSingleNode, fetchMultiNode } from "../src/request";

const ModelConfig = z.object({
  model_name: z.string(),
  api_key: z.string(),
  url: z.string().optional(),
});

const EmbedConfig = z.object({
  from: z.array(z.string()),
  model_config: ModelConfig,
});

const CollectionField = z.object({
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
  num_dim: z.number().optional(),
  embed: EmbedConfig.optional(),
  vec_dist: z.string().optional(),
  hnsw_params: z
    .object({
      M: z.number(),
      ef_construction: z.number(),
    })
    .optional(),
});

const UpdateCollectionField = z.object({
  name: z.string(),
  type: z.string(),
  num_dim: z.number().optional(),
  embed: EmbedConfig.optional(),
});

const CreateCollectionResponse = z.object({
  created_at: z.number(),
  default_sorting_field: z.string(),
  enable_nested_fields: z.boolean(),
  fields: z.array(CollectionField),
  name: z.string(),
  num_documents: z.number(),
  symbols_to_index: z.array(z.string()),
  token_separators: z.array(z.string()),
});

const UpdateCollectionResponse = z.object({
  fields: z.array(UpdateCollectionField),
});

const ErrorResponse = z.object({
  message: z.string(),
});

const DocumentBase = z.object({
  id: z.string(),
});

const DocumentWithEmbedding = DocumentBase.extend({
  embedding: z.array(z.number()).optional(),
  title: z.string().optional(),
  text: z.string().optional(),
  product_name: z.string().optional(),
}).passthrough();

const SearchHitDocument = DocumentBase.extend({
  embedding: z.array(z.number()).optional(),
  title: z.string().optional(),
  text: z.string().optional(),
  product_name: z.string().optional(),
}).passthrough();

const SearchHit = z.object({
  document: SearchHitDocument,
  highlight: z.record(z.string()).optional(),
  text_match: z.number().optional(),
  text_match_info: z.record(z.unknown()).optional(),
  curated: z.boolean().optional(),
  hybrid_search_info: z.object({ rank_fusion_score: z.number().optional() }).optional(),
  vector_distance: z.number().optional(),
  geo_distance_meters: z.record(z.number()).optional(),
});

const FacetCountEntry = z.object({
  value: z.union([z.string(), z.number(), z.boolean()]).optional(),
  highlighted: z.string().optional(),
  count: z.number(),
  parent: z.union([z.string(), z.number(), z.boolean()]).optional(),
  facet_filter: z.string().optional(),
});

const FacetResult = z.object({
  field_name: z.string(),
  sampled: z.boolean().optional(),
  counts: z.array(FacetCountEntry),
});

const GroupedHits = z.object({
  group_key: z.array(z.union([z.string(), z.number(), z.boolean()])),
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
  request_params: z.record(z.unknown()).optional(),
  parsed_nl_query: z.record(z.unknown()).optional(),
  union_request_params: z.array(z.record(z.unknown())).optional(),
});

const MultiSearchHitDocument = DocumentBase.extend({
  embedding: z.array(z.number()).optional(),
  title: z.string().optional(),
  text: z.string().optional(),
  product_name: z.string().optional(),
  content: z.string().optional(),
}).passthrough();

const MultiSearchResult = z.object({
  facet_counts: z.array(z.unknown()),
  found: z.number(),
  hits: z.array(
    z.object({
      document: MultiSearchHitDocument,
      highlight: z.record(z.string()).optional(),
      highlights: z.array(z.unknown()).optional(),
      vector_distance: z.number().optional(),
    })
  ),
  out_of: z.number().optional(),
  page: z.number().optional(),
  request_params: z.record(z.unknown()).optional(),
  search_cutoff: z.boolean().optional(),
  search_time_ms: z.number().optional(),
});

const MultiSearchResponse = z.object({
  results: z.array(MultiSearchResult),
});

const DocumentResponse = z.object({
  id: z.string(),
});

const COLLECTION_DIMENSIONS = {
  matryoshka_512: 512,
  matryoshka_1024: 1024,
  matryoshka_256: 256,
  matryoshka_search_512: 512,
  matryoshka_search_1024: 1024,
  azure_matryoshka_512: 512,
  azure_matryoshka_1024: 1024,
  matryoshka_multi_512: 512,
  matryoshka_multi_1024: 1024,
} as const;

describe(Phases.SINGLE_FRESH, () => {
  it(`${Filters.SECRETS} create a collection with openai embedding`, async () => {
    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "openai_collection",
        fields: [
          {
            name: "product_name",
            type: "string",
            facet: false,
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1536,
            embed: {
              from: ["product_name"],
              model_config: {
                model_name: "openai/text-embedding-3-small",
                api_key: Bun.env.OPEN_AI_API_KEY ?? "sk-random",
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const resJson = await res.json();
    const data = CreateCollectionResponse.safeParse(resJson);
    expect(data.data?.name).toBe("openai_collection");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields.length).toBe(2);
    expect(data.data?.fields[1]?.num_dim).toBe(1536);
    expect(data.data?.fields[1]?.embed?.from).toEqual(["product_name"]);
    expect(data.data?.fields[1]?.embed?.model_config?.model_name).toBe("openai/text-embedding-3-small");
  });

  it(`${Filters.SECRETS} should reject update with no changes to embedding field`, async () => {
    const checkRes = await fetchSingleNode("/collections/openai_collection");

    const res = await fetchSingleNode("/collections/openai_collection", {
      method: "PATCH",
      body: JSON.stringify({
        fields: [
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1536,
            embed: {
              from: ["product_name"],
              model_config: {
                model_name: "openai/text-embedding-3-small",
                api_key: Bun.env.OPEN_AI_API_KEY ?? "sk-random",
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(false);
    const resJson = await res.json();
    const errorData = ErrorResponse.safeParse(resJson);
    expect(errorData.success).toBe(true);
    expect(errorData.data?.message).toBe(
      "Field `embedding` is already part of the schema: To change this field, drop it first before adding it back to the schema."
    );
  });

  it(`${Filters.SECRETS} should reject update when changing from fields in embedding`, async () => {
    const res = await fetchSingleNode("/collections/openai_collection", {
      method: "PATCH",
      body: JSON.stringify({
        fields: [
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1536,
            embed: {
              from: ["product_name", "description"], // Added description
              model_config: {
                model_name: "openai/text-embedding-3-small",
                api_key: Bun.env.OPEN_AI_API_KEY ?? "sk-random",
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(false);
    const resJson = await res.json();
    const errorData = ErrorResponse.safeParse(resJson);
    expect(errorData.success).toBe(true);
    expect(errorData.data?.message).toBe(
      "Field `embedding` is already part of the schema: To change this field, drop it first before adding it back to the schema."
    );
  });

  it(`${Filters.SECRETS} should reject update when changing model_name in embedding`, async () => {
    const res = await fetchSingleNode("/collections/openai_collection", {
      method: "PATCH",
      body: JSON.stringify({
        fields: [
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1536,
            embed: {
              from: ["product_name"],
              model_config: {
                model_name: "openai/text-embedding-3-large", // Changed from small to large
                api_key: Bun.env.OPEN_AI_API_KEY ?? "sk-random",
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(false);
    const resJson = await res.json();
    const errorData = ErrorResponse.safeParse(resJson);
    expect(errorData.success).toBe(true);
    expect(errorData.data?.message).toBe(
      "Field `embedding` is already part of the schema: To change this field, drop it first before adding it back to the schema."
    );
  });

  it(`${Filters.SECRETS} should allow update when only changing api_key in embedding`, async () => {
    const newApiKey = "new-api-key";
    const res = await fetchSingleNode("/collections/openai_collection", {
      method: "PATCH",
      body: JSON.stringify({
        fields: [
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1536,
            embed: {
              from: ["product_name"],
              model_config: {
                model_name: "openai/text-embedding-3-small",
                api_key: newApiKey, // Only changed the API key
              },
            },
          },
        ],
      }),
    });

    expect(res.ok).toBe(true);
    const resJson = await res.json();
    const updateData = UpdateCollectionResponse.safeParse(resJson);
    expect(updateData.success).toBe(true);

    const getRes = await fetchSingleNode("/collections/openai_collection", {
      method: "GET",
    });
    expect(getRes.ok).toBe(true);
    const getResJson = await getRes.json();
    const getData = CreateCollectionResponse.safeParse(getResJson);
    expect(getData.success).toBe(true);
    expect(getData.data?.fields[1]?.embed).toBeDefined();
    const startsWith = getData.data?.fields[1]?.embed?.model_config?.api_key.startsWith(newApiKey.slice(0, 5));
    expect(startsWith).toBe(true);
  });

  it(`${Filters.SECRETS} should support multiple collections with same model but different dimensions`, async () => {
    const apiKey = Bun.env.OPEN_AI_API_KEY ?? "sk-random";
    const modelName = "openai/text-embedding-3-large";

    const res1 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_512",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 512,
            embed: {
              from: ["text"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(res1.ok).toBe(true);
    const res1Json = await res1.json();
    const data1 = CreateCollectionResponse.safeParse(res1Json);
    expect(data1.success).toBe(true);
    expect(data1.data?.name).toBe("matryoshka_512");
    expect(data1.data?.fields[1]?.num_dim).toBe(512);
    expect(data1.data?.fields[1]?.embed?.model_config?.model_name).toBe(modelName);

    const res2 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_1024",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1024,
            embed: {
              from: ["text"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(res2.ok).toBe(true);
    const res2Json = await res2.json();
    const data2 = CreateCollectionResponse.safeParse(res2Json);
    expect(data2.success).toBe(true);
    expect(data2.data?.name).toBe("matryoshka_1024");
    expect(data2.data?.fields[1]?.num_dim).toBe(1024);
    expect(data2.data?.fields[1]?.embed?.model_config?.model_name).toBe(modelName);

    const res3 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_256",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 256,
            embed: {
              from: ["text"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(res3.ok).toBe(true);
    const res3Json = await res3.json();
    const data3 = CreateCollectionResponse.safeParse(res3Json);
    expect(data3.success).toBe(true);
    expect(data3.data?.name).toBe("matryoshka_256");
    expect(data3.data?.fields[1]?.num_dim).toBe(256);
    expect(data3.data?.fields[1]?.embed?.model_config?.model_name).toBe(modelName);

    const getRes1 = await fetchSingleNode("/collections/matryoshka_512");
    expect(getRes1.ok).toBe(true);
    const getData1 = CreateCollectionResponse.safeParse(await getRes1.json());
    expect(getData1.success).toBe(true);
    expect(getData1.data?.fields[1]?.num_dim).toBe(512);

    const getRes2 = await fetchSingleNode("/collections/matryoshka_1024");
    expect(getRes2.ok).toBe(true);
    const getData2 = CreateCollectionResponse.safeParse(await getRes2.json());
    expect(getData2.success).toBe(true);
    expect(getData2.data?.fields[1]?.num_dim).toBe(1024);

    const getRes3 = await fetchSingleNode("/collections/matryoshka_256");
    expect(getRes3.ok).toBe(true);
    const getData3 = CreateCollectionResponse.safeParse(await getRes3.json());
    expect(getData3.success).toBe(true);
    expect(getData3.data?.fields[1]?.num_dim).toBe(256);

    const collectionsToTest = (["matryoshka_512", "matryoshka_1024", "matryoshka_256"] as const).map((name) => ({
      name,
      expectedDim: COLLECTION_DIMENSIONS[name],
    }));

    for (const { name, expectedDim } of collectionsToTest) {
      const addRes = await fetchSingleNode(`/collections/${name}/documents`, {
        method: "POST",
        body: JSON.stringify({
          id: "1",
          text: "Test document for dimension verification",
        }),
      });

      expect(addRes.ok).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const getDocRes = await fetchSingleNode(`/collections/${name}/documents/1`);
      expect(getDocRes.ok).toBe(true);
      const docData = DocumentWithEmbedding.safeParse(await getDocRes.json());
      expect(docData.success).toBe(true);
      if (docData.success && docData.data.embedding) {
        expect(Array.isArray(docData.data.embedding)).toBe(true);
        expect(docData.data.embedding.length).toBe(expectedDim);
      }
    }
  });

  it(`${Filters.SECRETS} should index and search documents with different dimensions using same model`, async () => {
    const apiKey = Bun.env.OPEN_AI_API_KEY ?? "sk-random";
    const modelName = "openai/text-embedding-3-large";

    const createRes = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_search_512",
        fields: [
          {
            name: "title",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 512,
            embed: {
              from: ["title"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(createRes.ok).toBe(true);

    const addRes = await fetchSingleNode("/collections/matryoshka_search_512/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "1",
        title: "Machine learning and artificial intelligence",
      }),
    });

    expect(addRes.ok).toBe(true);
    const addData = DocumentResponse.safeParse(await addRes.json());
    expect(addData.success).toBe(true);
    if (addData.success) {
      expect(addData.data.id).toBe("1");
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const searchRes = await fetchSingleNode(
      `/collections/matryoshka_search_512/documents/search?q=*&vector_query=${encodeURIComponent(
        "embedding:([], k:1)"
      )}`,
      { method: "GET" }
    );

    expect(searchRes.ok).toBe(true);
    const searchData = SearchResponse.safeParse(await searchRes.json());
    expect(searchData.success).toBe(true);
    if (searchData.success) {
      expect(searchData.data.found).toBeGreaterThanOrEqual(0);
    }

    const getDocRes = await fetchSingleNode("/collections/matryoshka_search_512/documents/1");
    expect(getDocRes.ok).toBe(true);
    const docData = DocumentWithEmbedding.safeParse(await getDocRes.json());
    expect(docData.success).toBe(true);
    if (docData.success && docData.data.embedding) {
      expect(Array.isArray(docData.data.embedding)).toBe(true);
      expect(docData.data.embedding.length).toBe(512);
    }

    const createRes2 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_search_1024",
        fields: [
          {
            name: "title",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1024,
            embed: {
              from: ["title"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(createRes2.ok).toBe(true);

    const addRes2 = await fetchSingleNode("/collections/matryoshka_search_1024/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "1",
        title: "Deep learning and neural networks",
      }),
    });

    expect(addRes2.ok).toBe(true);
    const addData2 = DocumentResponse.safeParse(await addRes2.json());
    expect(addData2.success).toBe(true);
    if (addData2.success) {
      expect(addData2.data.id).toBe("1");
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const searchRes2 = await fetchSingleNode(
      `/collections/matryoshka_search_1024/documents/search?q=*&vector_query=${encodeURIComponent(
        "embedding:([], k:1)"
      )}`,
      { method: "GET" }
    );

    expect(searchRes2.ok).toBe(true);
    const searchData2 = SearchResponse.safeParse(await searchRes2.json());
    expect(searchData2.success).toBe(true);
    if (searchData2.success) {
      expect(searchData2.data.found).toBeGreaterThanOrEqual(0);
    }

    const getDocRes2 = await fetchSingleNode("/collections/matryoshka_search_1024/documents/1");
    expect(getDocRes2.ok).toBe(true);
    const docData2 = DocumentWithEmbedding.safeParse(await getDocRes2.json());
    expect(docData2.success).toBe(true);
    if (docData2.success && docData2.data.embedding) {
      expect(Array.isArray(docData2.data.embedding)).toBe(true);
      expect(docData2.data.embedding.length).toBe(1024);
    }

    const getRes1 = await fetchSingleNode("/collections/matryoshka_search_512");
    const getData1 = CreateCollectionResponse.safeParse(await getRes1.json());
    expect(getData1.success).toBe(true);
    expect(getData1.data?.fields[1]?.num_dim).toBe(512);

    const getRes2 = await fetchSingleNode("/collections/matryoshka_search_1024");
    const getData2 = CreateCollectionResponse.safeParse(await getRes2.json());
    expect(getData2.success).toBe(true);
    expect(getData2.data?.fields[1]?.num_dim).toBe(1024);
  });

  it(`${Filters.SECRETS} should support Azure embedder with different dimensions`, async () => {
    const apiKey = Bun.env.AZURE_OPENAI_API_KEY ?? "test-azure-key";
    const modelName = "azure/text-embedding-3-large";
    const azureUrl = Bun.env.AZURE_OPENAI_URL;

    const modelConfig1: z.infer<typeof ModelConfig> = {
      model_name: modelName,
      api_key: apiKey,
      ...(azureUrl && { url: azureUrl }),
    };

    const res1 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "azure_matryoshka_512",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 512,
            embed: {
              from: ["text"],
              model_config: modelConfig1,
            },
          },
        ],
      }),
    });

    expect(res1.ok).toBe(true);
    const res1Json = await res1.json();
    const data1 = CreateCollectionResponse.safeParse(res1Json);
    expect(data1.success).toBe(true);
    expect(data1.data?.name).toBe("azure_matryoshka_512");
    expect(data1.data?.fields[1]?.num_dim).toBe(512);

    const modelConfig2: z.infer<typeof ModelConfig> = {
      model_name: modelName,
      api_key: apiKey,
      ...(azureUrl && { url: azureUrl }),
    };

    const res2 = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "azure_matryoshka_1024",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1024,
            embed: {
              from: ["text"],
              model_config: modelConfig2,
            },
          },
        ],
      }),
    });

    expect(res2.ok).toBe(true);
    const res2Json = await res2.json();
    const data2 = CreateCollectionResponse.safeParse(res2Json);
    expect(data2.success).toBe(true);
    expect(data2.data?.name).toBe("azure_matryoshka_1024");
    expect(data2.data?.fields[1]?.num_dim).toBe(1024);

    const getRes1 = await fetchSingleNode("/collections/azure_matryoshka_512");
    expect(getRes1.ok).toBe(true);
    const getData1 = CreateCollectionResponse.safeParse(await getRes1.json());
    expect(getData1.success).toBe(true);
    expect(getData1.data?.fields[1]?.num_dim).toBe(512);

    const getRes2 = await fetchSingleNode("/collections/azure_matryoshka_1024");
    expect(getRes2.ok).toBe(true);
    const getData2 = CreateCollectionResponse.safeParse(await getRes2.json());
    expect(getData2.success).toBe(true);
    expect(getData2.data?.fields[1]?.num_dim).toBe(1024);
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it(`${Filters.SECRETS} create a collection with openai embedding`, async () => {
    const res = await fetchSingleNode("/collections/openai_collection", {
      method: "GET",
    });

    expect(res.ok).toBe(true);
    const resJson = await res.json();
    const data = CreateCollectionResponse.safeParse(resJson);
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("openai_collection");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields.length).toBe(2);
    expect(data.data?.fields[1]?.num_dim).toBe(1536);
    expect(data.data?.fields[1]?.embed?.from).toEqual(["product_name"]);
    expect(data.data?.fields[1]?.embed?.model_config?.model_name).toBe("openai/text-embedding-3-small");
  });

  it(`${Filters.SECRETS} should persist matryoshka collections with different dimensions after restart`, async () => {
    const collections = (["matryoshka_512", "matryoshka_1024", "matryoshka_256"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchSingleNode(`/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });

  it(`${Filters.SECRETS} should persist search collections with different dimensions after restart`, async () => {
    const collections = (["matryoshka_search_512", "matryoshka_search_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchSingleNode(`/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });

  it(`${Filters.SECRETS} should persist Azure matryoshka collections with different dimensions after restart`, async () => {
    const collections = (["azure_matryoshka_512", "azure_matryoshka_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchSingleNode(`/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it(`${Filters.SECRETS} should persist matryoshka collections with different dimensions after snapshot`, async () => {
    const collections = (["matryoshka_512", "matryoshka_1024", "matryoshka_256"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchSingleNode(`/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });

  it(`${Filters.SECRETS} should persist Azure matryoshka collections with different dimensions after snapshot`, async () => {
    const collections = (["azure_matryoshka_512", "azure_matryoshka_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchSingleNode(`/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });
});

describe(Phases.MULTI_FRESH, () => {
  it(`${Filters.SECRETS} should create matryoshka collections with different dimensions in multi-node`, async () => {
    const apiKey = Bun.env.OPEN_AI_API_KEY ?? "sk-random";
    const modelName = "openai/text-embedding-3-large";

    const res1 = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_multi_512",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 512,
            embed: {
              from: ["text"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(res1.ok).toBe(true);
    const res1Json = await res1.json();
    const data1 = CreateCollectionResponse.safeParse(res1Json);
    expect(data1.success).toBe(true);
    expect(data1.data?.name).toBe("matryoshka_multi_512");
    expect(data1.data?.fields[1]?.num_dim).toBe(512);

    const res2 = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "matryoshka_multi_1024",
        fields: [
          {
            name: "text",
            type: "string",
          },
          {
            name: "embedding",
            type: "float[]",
            num_dim: 1024,
            embed: {
              from: ["text"],
              model_config: {
                model_name: modelName,
                api_key: apiKey,
              },
            },
          },
        ],
      }),
    });

    expect(res2.ok).toBe(true);
    const res2Json = await res2.json();
    const data2 = CreateCollectionResponse.safeParse(res2Json);
    expect(data2.success).toBe(true);
    expect(data2.data?.name).toBe("matryoshka_multi_1024");
    expect(data2.data?.fields[1]?.num_dim).toBe(1024);
  });

  it(`${Filters.SECRETS} should add documents with matryoshka embeddings in multi-node`, async () => {
    const collections = (["matryoshka_multi_512", "matryoshka_multi_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const addRes = await fetchMultiNode(1, `/collections/${collectionName}/documents`, {
        method: "POST",
        body: JSON.stringify({
          id: "1",
          text: "Test document for multi-node dimension verification",
        }),
      });

      expect(addRes.ok).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const getDocRes = await fetchMultiNode(1, `/collections/${collectionName}/documents/1`);
      expect(getDocRes.ok).toBe(true);
      const docData = DocumentWithEmbedding.safeParse(await getDocRes.json());
      expect(docData.success).toBe(true);
      if (docData.success && docData.data.embedding) {
        expect(Array.isArray(docData.data.embedding)).toBe(true);
        expect(docData.data.embedding.length).toBe(expectedDim);
      }
    }
  });

  it(`${Filters.SECRETS} should perform vector search with matryoshka embeddings in multi-node`, async () => {
    const res = await fetchMultiNode(1, "/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "matryoshka_multi_512",
            q: "*",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 1)",
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
  it(`${Filters.SECRETS} should persist matryoshka collections with different dimensions after multi-node restart`, async () => {
    const collections = (["matryoshka_multi_512", "matryoshka_multi_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchMultiNode(2, `/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });

  it(`${Filters.SECRETS} should perform vector search with matryoshka embeddings after multi-node restart`, async () => {
    const res = await fetchMultiNode(1, "/multi_search", {
      method: "POST",
      body: JSON.stringify({
        searches: [
          {
            collection: "matryoshka_multi_512",
            q: "*",
            query_by: "embedding",
            exclude_fields: "embedding",
            prefix: "false",
            vector_query: "embedding:([], k: 1)",
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

describe(Phases.MULTI_SNAPSHOT, () => {
  it(`${Filters.SECRETS} should persist matryoshka collections with different dimensions after multi-node snapshot`, async () => {
    const collections = (["matryoshka_multi_512", "matryoshka_multi_1024"] as const).reduce((acc, name) => {
      acc[name] = COLLECTION_DIMENSIONS[name];
      return acc;
    }, {} as Record<string, number>);

    for (const [collectionName, expectedDim] of Object.entries(collections)) {
      const res = await fetchMultiNode(3, `/collections/${collectionName}`, {
        method: "GET",
      });

      expect(res.ok).toBe(true);
      const resJson = await res.json();
      const data = CreateCollectionResponse.safeParse(resJson);
      expect(data.success).toBe(true);
      if (data.success) {
        expect(data.data.name).toBe(collectionName);
        const embeddingField = data.data.fields[1];
        expect(embeddingField).toBeDefined();
        if (embeddingField && embeddingField.num_dim !== undefined) {
          expect(embeddingField.num_dim).toBe(expectedDim);
        }
      }
    }
  });
});
