import { describe, it, expect, beforeAll } from "bun:test";
import { Phases } from "../src/constants";
import { fetchMultiNode, fetchSingleNode, waitForMultiAnalyticsFlush, waitForSingleAnalyticsFlush } from "../src/request";
import { z } from "zod";

const MULTI_NODE_ANALYTICS_FLUSH_WAIT = 9000;

const AnalyticsRule = z.object({
  name: z.string(),
  type: z.string(),
  collection: z.string(),
  event_type: z.string(),
  rule_tag: z.string().optional(),
  params: z.object({
    capture_search_requests: z.boolean().optional(),
    meta_fields: z.array(z.string()).optional(),
    expand_query: z.boolean().optional(),
    destination_collection: z.string().optional(),
    limit: z.number().optional(),
    counter_field: z.string().optional(),
    weight: z.number().optional(),
  }).optional(),
});

const AnalyticsEvent = z.object({
  name: z.string(),
  event_type: z.string(),
  collection: z.string(),
  timestamp: z.number(),
  doc_id: z.string().optional(),
  doc_ids: z.array(z.string()).optional(),
  query: z.string().optional(),
  user_id: z.string(),
});

const AnalyticsEventList = z.object({
  events: z.array(AnalyticsEvent),
});
const AnalyticsRuleList = z.array(AnalyticsRule);
const SuccessResponse = z.object({ success: z.boolean() });
const OkResponse = z.object({ ok: z.boolean() });

describe(Phases.SINGLE_FRESH, () => {
  it("create a document log analytics rule", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "analytics_products",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true },
          { name: "popularity", type: "int32", optional: true },
        ],
        default_sorting_field: "num_employees",
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "analytics_queries",
        fields: [
          { name: "q", type: "string" },
          { name: "count", type: "int32" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    await fetchSingleNode("/collections/analytics_products/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "1",
        company_name: "Typesense",
        num_employees: 10,
        country: "US",
        popularity: 0,
      }),
    });

    await fetchSingleNode("/collections/analytics_products/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "2",
        company_name: "ACME",
        num_employees: 20,
        country: "US",
        popularity: 0,
      }),
    });

    res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        type: "log",
        collection: "analytics_products",
        event_type: "click",
        rule_tag: "tag1",
      }),
    });
    expect(res.ok).toBe(true);
    const data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_clicks");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("click");
    expect(data.data?.rule_tag).toBe("tag1");
  });

  it("create a query log analytics rule", async () => {
    let res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_with_capture",
        type: "log",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag2",
        params: {
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_queries_with_capture");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag2");
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
    expect(data.data?.params?.expand_query).toBe(true);

    res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_without_capture",
        type: "log",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag2",
        params: {
          capture_search_requests: false,
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_queries_without_capture");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag2");
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
    expect(data.data?.params?.expand_query).toBe(true);
    expect(data.data?.params?.capture_search_requests).toBe(false);
  });

  it("upsert a query log analytics rule", async () => {
    let res = await fetchSingleNode("/analytics/rules/product_queries_without_capture", {
      method: "PUT",
      body: JSON.stringify({
        name: "product_queries_without_capture",
        type: "log",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag2",
        params: {
          capture_search_requests: false,
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_queries_without_capture");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag2");
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
    expect(data.data?.params?.expand_query).toBe(true);
    expect(data.data?.params?.capture_search_requests).toBe(false);
  });

  it("create a document counter analytics rule", async () => {
    let res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        type: "counter",
        collection: "analytics_products",
        event_type: "click",
        rule_tag: "tag1",
        params: {
          counter_field: "popularity",
          weight: 1,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_clicks_counter");
    expect(data.data?.type).toBe("counter");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("click");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.counter_field).toBe("popularity");
    expect(data.data?.params?.weight).toBe(1);

    res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_conversion_counter",
        type: "counter",
        collection: "analytics_products",
        event_type: "conversion",
        rule_tag: "tag1",
        params: {
          counter_field: "popularity",
          weight: 2,
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_conversion_counter");
    expect(data.data?.type).toBe("counter");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("conversion");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.counter_field).toBe("popularity");
    expect(data.data?.params?.weight).toBe(2);
  });

  it("create popular/nohits queries analytics rule", async () => {
    let res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_popular_queries",
        type: "popular_queries",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag1",
        params: {
          destination_collection: "analytics_queries",
          limit: 10,
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_popular_queries");
    expect(data.data?.type).toBe("popular_queries");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.destination_collection).toBe("analytics_queries");
    expect(data.data?.params?.limit).toBe(10);
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);

    res = await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_nohits_queries",
        type: "nohits_queries",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag1",
        params: {
          destination_collection: "analytics_queries",
          limit: 10,
          meta_fields: ["analytics_tag"],
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_nohits_queries");
    expect(data.data?.type).toBe("nohits_queries");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.destination_collection).toBe("analytics_queries");
    expect(data.data?.params?.limit).toBe(10);
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
  });

  it("get analytics rule", async () => {
    const res = await fetchSingleNode("/analytics/rules/product_clicks");
    expect(res.ok).toBe(true);
    const data = AnalyticsRule.parse(await res.json());
    expect(data.name).toBe("product_clicks");
  });

  it("get all analytics rules", async () => {
    const res = await fetchSingleNode("/analytics/rules");
    expect(res.ok).toBe(true);
    const data = AnalyticsRuleList.safeParse(await res.json());
    expect(data.success).toBe(true);
  });

  it("delete an analytics rule", async () => {
    await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_temp",
        type: "log",
        collection: "analytics_products",
        event_type: "conversion",
        rule_tag: "tag2",
      }),
    });
    
    const res = await fetchSingleNode("/analytics/rules/product_clicks_temp", {
      method: "DELETE",
    });
    expect(res.ok).toBe(true);
    const data = SuccessResponse.parse(await res.json());
    expect(data.success).toBe(true);
  });

  it("update an analytics rule using /rules", async () => {
    await fetchSingleNode("/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_temp",
        type: "log",
        collection: "analytics_products",
        event_type: "conversion",
        rule_tag: "tag2",
      }),
    });
    
    const res = await fetchSingleNode("/analytics/rules/product_clicks_temp", {
      method: "PUT",
      body: JSON.stringify({
        rule_tag: "tag3",
      }),
    });
    expect(res.ok).toBe(true);
    const data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.rule_tag).toBe("tag3");
  });

  it("add an document log analytics event", async () => {
    let res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.doc_id).toBe("1");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a query log analytics event in /events", async () => {
    let res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_without_capture",
        event_type: "query",
        data: {
          q: "product",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_queries_without_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_without_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("product");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a query log analytics event using /search", async () => {
    await fetchSingleNode("/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForSingleAnalyticsFlush();
    let res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_queries_with_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_with_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("typesense");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a document counter analytics event", async () => {
    let res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);
    res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_conversion_counter",
        event_type: "conversion",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    await waitForSingleAnalyticsFlush();
    res = await fetchSingleNode("/collections/analytics_products/documents/1");
    expect(res.ok).toBe(true);
    const data1 = (await res.json()) as any;
    expect(data1.popularity).toBe(3);
  });

  it("add a query counter analytics event", async () => {
    await fetchSingleNode("/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForSingleAnalyticsFlush();
    let res = await fetchSingleNode("/collections/analytics_queries/documents/export");
    expect(res.ok).toBe(true);
    const res_json = (await res.text()) as any;
    let data = res_json.split("\n").map((item: any) => JSON.parse(item));
    expect(data.length).toBe(1);
    expect(data[0].count).toBe(2);
    expect(data[0].q).toBe("typesense");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("check all the rules are persisted after restart", async () => {
    const res = await fetchSingleNode("/analytics/rules");
    expect(res.ok).toBe(true);
    const data = AnalyticsRuleList.parse(await res.json());
    expect(data.find((rule) => rule.name === "product_clicks")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_without_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_with_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_popular_queries")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_nohits_queries")).toBeDefined();
  });

  it("add one more document counter analytics event", async () => {
    let res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    await waitForSingleAnalyticsFlush(); 
    res = await fetchSingleNode("/collections/analytics_products/documents/1");
    expect(res.ok).toBe(true);
    const data1 = (await res.json()) as any;
    expect(data1.popularity).toBe(4);
  });

  it("add one more query counter analytics event", async () => {
    await fetchSingleNode("/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchSingleNode("/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForSingleAnalyticsFlush() 
    let res = await fetchSingleNode("/collections/analytics_queries/documents/export");
    expect(res.ok).toBe(true);
    const res_json = (await res.text()) as any;
    let data = res_json.split("\n").map((item: any) => JSON.parse(item));
    expect(data.length).toBe(1);
    expect(data[0].count).toBe(3);
    expect(data[0].q).toBe("typesense");
  });

  it("add an document log analytics event", async () => {
    let res = await fetchSingleNode("/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        event_type: "click",
        data: {
          doc_ids: ["2", "1"],
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(2);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
    expect(data1.data?.events?.[0]?.doc_ids).toEqual(["2", "1"]);
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("check all the rules are persisted after snapshot restore", async () => {
    const res = await fetchSingleNode("/analytics/rules");
    expect(res.ok).toBe(true);
    const data = AnalyticsRuleList.parse(await res.json());
    expect(data.find((rule) => rule.name === "product_clicks")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_without_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_with_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_popular_queries")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_nohits_queries")).toBeDefined();
  });

  it("get the added query log event", async () => {
    let res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_queries_with_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(3);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_with_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("typesense");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("get the added document log event", async () => {
    let res = await fetchSingleNode("/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.doc_id).toEqual("1");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("create a document log analytics rule", async () => {
    let res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "analytics_products",
        fields: [
          { name: "company_name", type: "string" },
          { name: "num_employees", type: "int32" },
          { name: "country", type: "string", facet: true },
          { name: "popularity", type: "int32", optional: true },
        ],
        default_sorting_field: "num_employees",
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "analytics_queries",
        fields: [
          { name: "q", type: "string" },
          { name: "count", type: "int32" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    await fetchMultiNode(1, "/collections/analytics_products/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "1",
        company_name: "Typesense",
        num_employees: 10,
        country: "US",
        popularity: 0,
      }),
    });

    await fetchMultiNode(1, "/collections/analytics_products/documents", {
      method: "POST",
      body: JSON.stringify({
        id: "2",
        company_name: "ACME",
        num_employees: 20,
        country: "US",
        popularity: 0,
      }),
    });

    res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        type: "log",
        collection: "analytics_products",
        event_type: "click",
        rule_tag: "tag1",
      }),
    });
    expect(res.ok).toBe(true);
    const data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_clicks");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("click");
    expect(data.data?.rule_tag).toBe("tag1");
  });

  it("create a query log analytics rule", async () => {
    let res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_with_capture",
        type: "log",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag2",
        params: {
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_queries_with_capture");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag2");
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
    expect(data.data?.params?.expand_query).toBe(true);

    res = await fetchMultiNode(2, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_without_capture",
        type: "log",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag2",
        params: {
          capture_search_requests: false,
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_queries_without_capture");
    expect(data.data?.type).toBe("log");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag2");
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
    expect(data.data?.params?.expand_query).toBe(true);
    expect(data.data?.params?.capture_search_requests).toBe(false);
  });

  it("create a document counter analytics rule", async () => {
    let res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        type: "counter",
        collection: "analytics_products",
        event_type: "click",
        rule_tag: "tag1",
        params: {
          counter_field: "popularity",
          weight: 1,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_clicks_counter");
    expect(data.data?.type).toBe("counter");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("click");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.counter_field).toBe("popularity");
    expect(data.data?.params?.weight).toBe(1);

    res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_conversion_counter",
        type: "counter",
        collection: "analytics_products",
        event_type: "conversion",
        rule_tag: "tag1",
        params: {
          counter_field: "popularity",
          weight: 2,
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_conversion_counter");
    expect(data.data?.type).toBe("counter");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("conversion");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.counter_field).toBe("popularity");
    expect(data.data?.params?.weight).toBe(2);
  });

  it("create popular/nohits queries analytics rule", async () => {
    let res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_popular_queries",
        type: "popular_queries",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag1",
        params: {
          destination_collection: "analytics_queries",
          limit: 10,
          meta_fields: ["analytics_tag"],
          expand_query: true,
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_popular_queries");
    expect(data.data?.type).toBe("popular_queries");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.destination_collection).toBe("analytics_queries");
    expect(data.data?.params?.limit).toBe(10);
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);

    res = await fetchMultiNode(1, "/analytics/rules", {
      method: "POST",
      body: JSON.stringify({
        name: "product_nohits_queries",
        type: "nohits_queries",
        collection: "analytics_products",
        event_type: "query",
        rule_tag: "tag1",
        params: {
          destination_collection: "analytics_queries",
          limit: 10,
          meta_fields: ["analytics_tag"],
        },
      }),
    });
    expect(res.ok).toBe(true);
    data = AnalyticsRule.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.name).toBe("product_nohits_queries");
    expect(data.data?.type).toBe("nohits_queries");
    expect(data.data?.collection).toBe("analytics_products");
    expect(data.data?.event_type).toBe("query");
    expect(data.data?.rule_tag).toBe("tag1");
    expect(data.data?.params?.destination_collection).toBe("analytics_queries");
    expect(data.data?.params?.limit).toBe(10);
    expect(data.data?.params?.meta_fields).toEqual(["analytics_tag"]);
  });

  it("add an document log analytics event", async () => {
    let res = await fetchMultiNode(2,"/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    await waitForMultiAnalyticsFlush(); 
    res = await fetchMultiNode(3, "/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.doc_id).toBe("1");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a query log analytics event in /events", async () => {
    let res = await fetchMultiNode(2, "/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_queries_without_capture",
        event_type: "query",
        data: {
          q: "product",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    await waitForMultiAnalyticsFlush(); 
    res = await fetchMultiNode(3, "/analytics/events?user_id=user1&name=product_queries_without_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_without_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("product");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a query log analytics event using /search", async () => {
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForMultiAnalyticsFlush(); 
    let res = await fetchMultiNode(3, "/analytics/events?user_id=user1&name=product_queries_with_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(1);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_with_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("typesense");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("add a query counter analytics event", async () => {
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForMultiAnalyticsFlush(); 
    let res = await fetchMultiNode(3, "/collections/analytics_queries/documents/export");
    expect(res.ok).toBe(true);
    const res_json = (await res.text()) as any;
    let data = res_json.split("\n").map((item: any) => JSON.parse(item));
    expect(data.length).toBe(1);
    expect(data[0].count).toBe(2);
    expect(data[0].q).toBe("typesense");
  });

  it("add a document counter analytics event", async () => {
    let res = await fetchMultiNode(2, "/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);
    res = await fetchMultiNode(3, "/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_conversion_counter",
        event_type: "conversion",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    await waitForMultiAnalyticsFlush(); 
    res = await fetchMultiNode(1, "/collections/analytics_products/documents/1");
    expect(res.ok).toBe(true);
    const data1 = (await res.json()) as any;
    expect(data1.popularity).toBe(3);

  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("check all the rules are persisted after restart", async () => {
    const res = await fetchMultiNode(1, "/analytics/rules");
    expect(res.ok).toBe(true);
    const data = AnalyticsRuleList.parse(await res.json());
    expect(data.find((rule) => rule.name === "product_clicks")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_without_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_with_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_popular_queries")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_nohits_queries")).toBeDefined();
  });

  it("add one more document counter analytics event", async () => {
    let res = await fetchMultiNode(2, "/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks_counter",
        event_type: "click",
        data: {
          doc_id: "1",
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    await waitForMultiAnalyticsFlush(); 
    res = await fetchMultiNode(3, "/collections/analytics_products/documents/1");
    expect(res.ok).toBe(true);
    const data1 = (await res.json()) as any;
    expect(data1.popularity).toBe(4);
  });

  it("add one more query counter analytics event", async () => {
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=type&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesen&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });
    await fetchMultiNode(2, "/collections/analytics_products/documents/search?q=typesense&query_by=company_name", { headers: { "x-typesense-user-id": "user1" } });

    await waitForMultiAnalyticsFlush() 
    let res = await fetchMultiNode(1, "/collections/analytics_queries/documents/export");
    expect(res.ok).toBe(true);
    const res_json = (await res.text()) as any;
    let data = res_json.split("\n").map((item: any) => JSON.parse(item));
    expect(data.length).toBe(1);
    expect(data[0].count).toBe(3);
    expect(data[0].q).toBe("typesense");
  });

  it("add an document log analytics event", async () => {
    let res = await fetchMultiNode(2, "/analytics/events", {
      method: "POST",
      body: JSON.stringify({
        name: "product_clicks",
        event_type: "click",
        data: {
          doc_ids: ["2", "1"],
          user_id: "user1",
        },
      }),
    });
    expect(res.ok).toBe(true);
    let data = OkResponse.parse(await res.json());
    expect(data.ok).toBe(true);

    await waitForMultiAnalyticsFlush(); 
    res = await fetchMultiNode(3, "/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(2);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
    expect(data1.data?.events?.[0]?.doc_ids).toEqual(["2", "1"]);
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("check all the rules are persisted after snapshot restore", async () => {
    const res = await fetchMultiNode(1, "/analytics/rules");
    expect(res.ok).toBe(true);
    const data = AnalyticsRuleList.parse(await res.json());
    expect(data.find((rule) => rule.name === "product_clicks")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_without_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_queries_with_capture")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_popular_queries")).toBeDefined();
    expect(data.find((rule) => rule.name === "product_nohits_queries")).toBeDefined();
  });

  it("get the added query log event", async () => {
    let res = await fetchMultiNode(2, "/analytics/events?user_id=user1&name=product_queries_with_capture&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(3);
    expect(data1.data?.events?.[0]?.name).toBe("product_queries_with_capture");
    expect(data1.data?.events?.[0]?.event_type).toBe("query");
    expect(data1.data?.events?.[0]?.query).toBe("typesense");
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });

  it("get the added document log event", async () => {
    let res = await fetchMultiNode(3, "/analytics/events?user_id=user1&name=product_clicks&n=10");
    expect(res.ok).toBe(true);
    let data1 = AnalyticsEventList.safeParse(await res.json());
    expect(data1.success).toBe(true);
    expect(data1.data?.events?.length).toBe(2);
    expect(data1.data?.events?.[0]?.name).toBe("product_clicks");
    expect(data1.data?.events?.[0]?.event_type).toBe("click");
    expect(data1.data?.events?.[0]?.doc_ids).toEqual(["2", "1"]);
    expect(data1.data?.events?.[0]?.user_id).toBe("user1");
  });
});