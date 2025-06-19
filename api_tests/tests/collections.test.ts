import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { z } from "zod";
import { fetchMultiNode, fetchSingleNode } from "../src/request";

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
  })),
  name: z.string(),
  num_documents: z.number(),
  symbols_to_index: z.array(z.string()),
  token_separators: z.array(z.string()),
});

const ListCollectionsResponse = z.array(CreateCollectionResponse)

describe(Phases.SINGLE_FRESH, () => {
  it("create a collection", async () => {
    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true }
        ]
      })
    });

    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());

    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });

  it("get a collection", async () => {
    const res = await fetchSingleNode("/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });

  it("get all collections", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies-1",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true }
        ]
      })
    });
    res = await fetchSingleNode("/collections", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = ListCollectionsResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.length).toBe(2);
    expect(data.data?.[0]?.name).toBe("companies-1");
    expect(data.data?.[1]?.name).toBe("companies");
    expect(data.data?.[0]?.num_documents).toBe(0);
    expect(data.data?.[1]?.num_documents).toBe(0);
  });

  it("delete a collection", async () => {
    const res = await fetchSingleNode("/collections/companies-1", {
      method: "DELETE",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies-1");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("get a created collection", async () => {
    const res = await fetchSingleNode("/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("get a created collection", async () => {
    const res = await fetchSingleNode("/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create a collection", async () => {
    const res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true }
        ]
      })
    });

    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());

    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });

  it("get a collection", async () => {
    const res = await fetchMultiNode(2, "/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });

  it("get all collections", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies-1",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true }
        ]
      })
    });
    res = await fetchMultiNode(3, "/collections", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = ListCollectionsResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.length).toBe(2);
    expect(data.data?.[0]?.name).toBe("companies-1");
    expect(data.data?.[1]?.name).toBe("companies");
    expect(data.data?.[0]?.num_documents).toBe(0);
    expect(data.data?.[1]?.num_documents).toBe(0);
  });

  it("delete a collection", async () => {
    const res = await fetchMultiNode(1, "/collections/companies-1", {
      method: "DELETE",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies-1");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("get a created collection", async () => {
    const res = await fetchMultiNode(1, "/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("get a created collection", async () => {
    const res = await fetchMultiNode(3, "/collections/companies", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateCollectionResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("companies");
    expect(data.data?.num_documents).toBe(0);
    expect(data.data?.fields?.[0]?.name).toEqual("company_name");
    expect(data.data?.fields?.[0]?.type).toEqual("string");
    expect(data.data?.fields?.[1]?.name).toEqual("num_employees");
    expect(data.data?.fields?.[1]?.type).toEqual("int32");
    expect(data.data?.fields?.[2]?.name).toEqual("country");
    expect(data.data?.fields?.[2]?.type).toEqual("string");
    expect(data.data?.fields?.[2]?.facet).toEqual(true);
  });
});