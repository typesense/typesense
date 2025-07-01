import { describe, it, expect } from "bun:test";
import { Phases, Filters } from "../src/constants";
import { z } from "zod";
import { fetchSingleNode } from "../src/request";

const CreateCollectionResponse = z.object({
  created_at: z.number(),
  default_sorting_field: z.string(),
  enable_nested_fields: z.boolean(),
  fields: z.array(z.object({
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
    embed: z.object({
      from: z.array(z.string()),
      model_config: z.object({
        model_name: z.string(),
        api_key: z.string(),
      }),
    }).optional(),
    vec_dist: z.string().optional(),
    hnsw_params: z.object({
      M: z.number(),
      ef_construction: z.number(),
    }).optional(),
  })),
  name: z.string(),
  num_documents: z.number(),
  symbols_to_index: z.array(z.string()),
  token_separators: z.array(z.string()),
});

describe(Phases.SINGLE_FRESH, () => {
  it(Filters.SECRETS + "create a collection with openai embedding", async () => {
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
      })
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
});

describe(Phases.SINGLE_RESTARTED, () => {
  it(Filters.SECRETS + "create a collection with openai embedding", async () => {
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
})