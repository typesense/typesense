import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { TypesenseProcessManager } from "../src/manager";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";
import { join } from "path";
import { rmSync, mkdirSync } from "node:fs";

describe(Phases.NO_PHASE, () => {
  beforeAll(() => {
    rmSync(join(process.cwd(), "./data/v29-typesense-data"), { recursive: true, force: true });
    mkdirSync(join(process.cwd(), "./data/v29-typesense-data"), { recursive: true });
    rmSync(join(process.cwd(), "./data/snapshot/v29-snapshot"), { recursive: true, force: true });
    mkdirSync(join(process.cwd(), "./data/snapshot/v29-snapshot"), { recursive: true });
    rmSync(join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"), { recursive: true, force: true });
    mkdirSync(join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"), { recursive: true });
  });

  it("create analytics rules in v29", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/v29-typesense-data"), process.env.TYPESENSE_V29_BINARY_PATH!);
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const createCollection = async(body: any) => {
        const res = await fetchSingleNode("/collections", { method: "POST", body: JSON.stringify(body) }, 8109);
        expect(res.ok).toBe(true);
        return res.json();
      }

      await Promise.all([
        createCollection({
          name: "products",
          fields: [
            { name: "company_name", type: "string" },
            { name: "num_employees", type: "int32" },
            { name: "country", type: "string", facet: true },
            { name: "popularity", type: "int32", optional: true },
          ],
          default_sorting_field: "num_employees",
        }),
        createCollection({
          name: "products_1",
          fields: [
            { name: "company_name", type: "string" },
            { name: "num_employees", type: "int32" },
            { name: "country", type: "string", facet: true },
            { name: "popularity", type: "int32", optional: true },
          ],
          default_sorting_field: "num_employees",
        }),
        createCollection({
          name: "queries",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
        createCollection({
          name: "queries_1",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
        createCollection({
          name: "no_hits_queries",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
        createCollection({
          name: "no_hits_queries_1",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
        createCollection({
          name: "product_queries",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
        createCollection({
          name: "product_queries_1",
          fields: [
            { name: "q", type: "string" },
            { name: "count", type: "int32" },
          ],
        }),
      ]);

      await fetchSingleNode(
        "/collections/products/documents/import?action=upsert",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body:
            "{" +
            '"company_name": "Typesense", "num_employees": 100, "country": "USA"' +
            "}\n{" +
            '"company_name": "Stark Industries", "num_employees": 1000, "country": "USA"' +
            "}\n{" +
            '"company_name": "Far Cry Industries", "num_employees": 200, "country": "USA"' +
            "}",
        },
        8109
      );

      const postLegacyRule = async (body: any) => {
        const res = await fetchSingleNode("/analytics/rules", { method: "POST", body: JSON.stringify(body) }, 8109);
        expect(res.ok).toBe(true);
        return res.json();
      }

      await postLegacyRule({
        name: "product_queries_aggregation",
        type: "popular_queries",
        params: {
          source: { collections: ["products"] },
          destination: { collection: "queries" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_queries_aggregation_1",
        type: "popular_queries",
        params: {
          source: {
            collections: ["products"],
            enable_auto_aggregation: false,
            events: [{ type: "search", name: "products_search_event" }],
          },
          destination: { collection: "product_queries" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_no_hits",
        type: "nohits_queries",
        params: {
          source: { collections: ["products"] },
          destination: { collection: "no_hits_queries" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_clicks",
        type: "counter",
        params: {
          source: {
            collections: ["products"],
            events: [{ type: "click", weight: 1, name: "products_click_event" }],
          },
          destination: { collection: "products", counter_field: "popularity" },
        },
      });

      await postLegacyRule({
        name: "product_queries_aggregation_2",
        type: "popular_queries",
        params: {
          source: { collections: ["products", "products_1"] },
          destination: { collection: "queries_1" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_queries_aggregation_3",
        type: "popular_queries",
        params: {
          source: {
            collections: ["products", "products_1"],
            enable_auto_aggregation: false,
            events: [
              { type: "search", name: "products_search_event_1" },
              { type: "search", name: "products_search_event_2" },
            ],
          },
          destination: { collection: "product_queries_1" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_no_hits_1",
        type: "nohits_queries",
        params: {
          source: { collections: ["products", "products_1"] },
          destination: { collection: "no_hits_queries_1" },
          limit: 1000,
        },
      });

      await postLegacyRule({
        name: "product_clicks_1",
        type: "counter",
        params: {
          source: {
            collections: ["products", "products_1"],
            events: [
              { type: "click", weight: 1, name: "products_click_event_1" },
              { type: "conversion", weight: 2, name: "products_conversion_event" },
            ],
          },
          destination: { collection: "products_1", counter_field: "popularity" },
        },
      });
      await manager.createSnapshot(8109, join(process.cwd(), "./data/snapshot/v29-snapshot"));
    } finally {
      await manager.shutdown();
    }
  });

  it("create synonyms in v29", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/v29-typesense-data"), process.env.TYPESENSE_V29_BINARY_PATH!);
    await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
    try {
      // Synonyms: use older per-collection endpoints
      const coat_synonyms = await fetchSingleNode(
        "/collections/products/synonyms/coat-synonyms",
        {
          method: "PUT",
          body: JSON.stringify({ synonyms: ["blazer", "coat", "jacket"] }),
        },
        8109
      );
      expect(coat_synonyms.ok).toBe(true);

      const smart_phone_synonyms = await fetchSingleNode(
        "/collections/products/synonyms/smart-phone-synonyms",
        {
          method: "PUT",
          body: JSON.stringify({ root: "smart phone", synonyms: ["iphone", "android"] }),
        },
        8109
      );
      expect(smart_phone_synonyms.ok).toBe(true);
    } finally {
      await manager.shutdown();
    }
  });

  it("create override sets in v29", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/v29-typesense-data"), process.env.TYPESENSE_V29_BINARY_PATH!);
    await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
    try {
      const customize_apple_override = await fetchSingleNode(
        "/collections/products/overrides/customize-apple",
        {
          method: "PUT",
          body: JSON.stringify({
            rule: {
              query: "apple",
              match: "exact"
            },
            includes: [
              {id: "422", position: 1},
              {id: "54", position: 2}
            ],
            excludes: [
              {id: "287"}
            ]
          }),
        },
        8109
      );
      expect(customize_apple_override.ok).toBe(true);
      const brand_filter_override = await fetchSingleNode(
        "/collections/products/overrides/brand-filter",
        {
          method: "PUT",
          body: JSON.stringify({
            rule: {
              query: "{brand} phone",
              match: "contains"
            },
            filter_by: "brand:={brand}",
            remove_matched_tokens: true
          }),
        },
        8109
      );
      expect(brand_filter_override.ok).toBe(true);
      const dynamic_sort_override = await fetchSingleNode(
        "/collections/products/overrides/dynamic-sort",
        {
          method: "PUT",
          body: JSON.stringify({
            rule: {
              query: "{store}",
              match: "exact"
            },
            remove_matched_tokens: true,
            sort_by: "sales.{store}:desc, inventory.{store}:desc"
          }),
        },
        8109
      );
      expect(dynamic_sort_override.ok).toBe(true);
      await manager.createSnapshot(8109, join(process.cwd(), "./data/snapshot/v29-snapshot"));
    } finally {
      await manager.shutdown();
    }
  });

  it("validate analytics rules", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot"));
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const res = await fetchSingleNode("/analytics/rules", { method: "GET" }, 8109);
      const data: any = await res.json();
      const expected_rules = [
        {
          collection: "products_1",
          event_type: "conversion",
          name: "products_conversion_event_products_1",
          params: {
            counter_field: "popularity",
            destination_collection: "products_1",
            weight: 2,
          },
          rule_tag: "product_clicks_1",
          type: "counter",
        }, {
          collection: "products",
          event_type: "conversion",
          name: "products_conversion_event_products",
          params: {
            counter_field: "popularity",
            destination_collection: "products_1",
            weight: 2,
          },
          rule_tag: "product_clicks_1",
          type: "counter",
        }, {
          collection: "products_1",
          event_type: "click",
          name: "products_click_event_1_products_1",
          params: {
            counter_field: "popularity",
            destination_collection: "products_1",
            weight: 1,
          },
          rule_tag: "product_clicks_1",
          type: "counter",
        }, {
          collection: "products",
          event_type: "click",
          name: "products_click_event_1_products",
          params: {
            counter_field: "popularity",
            destination_collection: "products_1",
            weight: 1,
          },
          rule_tag: "product_clicks_1",
          type: "counter",
        }, {
          collection: "products",
          event_type: "click",
          name: "products_click_event",
          params: {
            counter_field: "popularity",
            destination_collection: "products",
            weight: 1,
          },
          rule_tag: "product_clicks",
          type: "counter",
        }, {
          collection: "products_1",
          event_type: "search",
          name: "product_queries_aggregation_2_products_1",
          params: {
            capture_search_requests: true,
            destination_collection: "queries_1",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_queries_aggregation_2",
          type: "popular_queries",
        }, {
          collection: "products",
          event_type: "search",
          name: "product_queries_aggregation_2_products",
          params: {
            capture_search_requests: true,
            destination_collection: "queries_1",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_queries_aggregation_2",
          type: "popular_queries",
        }, {
          collection: "products",
          event_type: "search",
          name: "product_queries_aggregation",
          params: {
            capture_search_requests: true,
            destination_collection: "queries",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_queries_aggregation",
          type: "popular_queries",
        }, {
          collection: "products_1",
          event_type: "search",
          name: "product_no_hits_1_products_1",
          params: {
            capture_search_requests: true,
            destination_collection: "no_hits_queries_1",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_no_hits_1",
          type: "nohits_queries",
        }, {
          collection: "products",
          event_type: "search",
          name: "products_search_event",
          params: {
            capture_search_requests: false,
            destination_collection: "product_queries",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_queries_aggregation_1",
          type: "popular_queries",
        }, {
          collection: "products",
          event_type: "search",
          name: "product_no_hits_1_products",
          params: {
            capture_search_requests: true,
            destination_collection: "no_hits_queries_1",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_no_hits_1",
          type: "nohits_queries",
        }, {
          collection: "products",
          event_type: "search",
          name: "product_no_hits",
          params: {
            capture_search_requests: true,
            destination_collection: "no_hits_queries",
            expand_query: false,
            limit: 1000,
          },
          rule_tag: "product_no_hits",
          type: "nohits_queries",
        }
      ];

      for (const rule of expected_rules) {
        expect(data.find((r: any) => r.name === rule.name)).toBeDefined();
        expect(data.find((r: any) => r.name === rule.name).params).toEqual(rule.params);
        expect(data.find((r: any) => r.name === rule.name).rule_tag).toEqual(rule.rule_tag);
        expect(data.find((r: any) => r.name === rule.name).type).toEqual(rule.type);
        expect(data.find((r: any) => r.name === rule.name).event_type).toEqual(rule.event_type);
        expect(data.find((r: any) => r.name === rule.name).collection).toEqual(rule.collection);
      }
      const delete_res = await fetchSingleNode("/analytics/rules/products_conversion_event_products_1", { method: "DELETE" }, 8109);
      expect(delete_res.ok).toBe(true);
      const put_res = await fetchSingleNode("/analytics/rules/products_conversion_event_products", { method: "PUT", body: JSON.stringify( {
        collection: "products",
        event_type: "conversion",
        name: "products_conversion_event_products",
        params: {
          counter_field: "num_employees",
          destination_collection: "products_1",
          weight: 2,
        },
        rule_tag: "product_clicks_1",
        type: "counter",
      }) }, 8109);
      expect(put_res.ok).toBe(true);
    } finally {
      await manager.shutdown();
    }
  });

  it("validate synonyms", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot"));
    try {
    await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
    const res = await fetchSingleNode("/synonym_sets", { method: "GET" }, 8109);
    const data: any = await res.json();
    expect(data.length).toEqual(1);
    expect(data[0].name).toEqual("products_synonyms_index");
    expect(data[0].items[0].id).toEqual("coat-synonyms");
    expect(data[0].items[0].synonyms).toEqual(["blazer", "coat", "jacket"]);
    expect(data[0].items[1].id).toEqual("smart-phone-synonyms");
    expect(data[0].items[1].synonyms).toEqual(["iphone", "android"]);
    expect(data[0].items[1].root).toEqual("smart phone");
    const delete_res = await fetchSingleNode("/synonym_sets/products_synonyms_index", { method: "DELETE" }, 8109);
    expect(delete_res.ok).toBe(true);
    const put_res = await fetchSingleNode("/synonym_sets/products_synonyms_index", { method: "PUT", body: JSON.stringify({ items: [{ id: "coat-synonyms", synonyms: ["blazer", "coat"] }, { id: "smart-phone-synonyms", root: "smart phone", synonyms: ["iphone"] }] }) }, 8109);
    expect(put_res.ok).toBe(true);
    await manager.createSnapshot(8109, join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"));
    } finally {
      await manager.shutdown();
    }
  });

  it("validate override sets", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot"));
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const res = await fetchSingleNode("/override_sets", { method: "GET" }, 8109);
      const data: any = await res.json();
      expect(data.length).toEqual(1);
      expect(data[0].name).toEqual("products_overrides_index");
      expect(data[0].items[0].id).toEqual("brand-filter");
      expect(data[0].items[0].rule.query).toEqual("{brand} phone");
      expect(data[0].items[0].rule.match).toEqual("contains");
      expect(data[0].items[0].filter_by).toEqual("brand:={brand}");
      expect(data[0].items[0].remove_matched_tokens).toEqual(true);
      expect(data[0].items[1].id).toEqual("customize-apple");
      expect(data[0].items[1].rule.query).toEqual("apple");
      expect(data[0].items[1].rule.match).toEqual("exact");
      expect(data[0].items[1].includes).toEqual([{ id: "422", position: 1 }, { id: "54", position: 2 }]);
      expect(data[0].items[1].excludes).toEqual([{ id: "287" }]);
      expect(data[0].items[2].id).toEqual("dynamic-sort");
      expect(data[0].items[2].rule.query).toEqual("{store}");
      expect(data[0].items[2].rule.match).toEqual("exact");
      expect(data[0].items[2].remove_matched_tokens).toEqual(true);
      expect(data[0].items[2].sort_by).toEqual("sales.{store}:desc, inventory.{store}:desc");

      const delete_res = await fetchSingleNode("/override_sets/products_overrides_index", { method: "DELETE" }, 8109);
      expect(delete_res.ok).toBe(true);
      const put_res = await fetchSingleNode("/override_sets/products_overrides_index", { method: "PUT", body: JSON.stringify({ items: [{ id: "brand-filter", rule: { query: "{brand} phone", match: "contains" }, filter_by: "brand:={brand}", remove_matched_tokens: true }, { id: "customize-apple", rule: { query: "apple", match: "exact" }, includes: [{ id: "422", position: 1 }, { id: "54", position: 2 }], excludes: [{ id: "287" }] }, { id: "dynamic-sort", rule: { query: "{store}", match: "exact" }, remove_matched_tokens: true, sort_by: "sales.{store}:desc, inventory.{store}:desc" }] }) }, 8109);
      expect(put_res.ok).toBe(true);
      await manager.createSnapshot(8109, join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"));
    } finally {
      await manager.shutdown();
    }
  });

  it("rollback to v29 and validate analytics rules", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"), process.env.TYPESENSE_V29_BINARY_PATH!);
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const product_queries_aggregation = await fetchSingleNode("/analytics/rules/product_queries_aggregation", { method: "GET" }, 8109);
      expect(product_queries_aggregation.ok).toBe(true);
      const product_queries_aggregation_data: any = await product_queries_aggregation.json();
      expect(product_queries_aggregation_data.name).toEqual("product_queries_aggregation");
      expect(product_queries_aggregation_data.type).toEqual("popular_queries");
      expect(product_queries_aggregation_data.params.source.collections).toEqual(["products"]);
      expect(product_queries_aggregation_data.params.destination.collection).toEqual("queries");
      expect(product_queries_aggregation_data.params.limit).toEqual(1000);
    } finally {
      await manager.shutdown();
    }
  });

  it("rollback to v29 and validate synonyms", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"), process.env.TYPESENSE_V29_BINARY_PATH!);
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const synonyms = await fetchSingleNode("/collections/products/synonyms/coat-synonyms", { method: "GET" }, 8109);
      expect(synonyms.ok).toBe(true);
      const synonyms_data: any = await synonyms.json();
      expect(synonyms_data.synonyms).toEqual(["blazer", "coat", "jacket"]);
      const smart_phone_synonyms = await fetchSingleNode("/collections/products/synonyms/smart-phone-synonyms", { method: "GET" }, 8109);
      expect(smart_phone_synonyms.ok).toBe(true);
      const smart_phone_synonyms_data: any = await smart_phone_synonyms.json();
      expect(smart_phone_synonyms_data.synonyms).toEqual(["iphone", "android"]);
      expect(smart_phone_synonyms_data.root).toEqual("smart phone");
    } finally {
      await manager.shutdown();
    }
  });

  it("rollback to v29 and validate override sets", async () => {
    let manager: TypesenseProcessManager;
    manager = new TypesenseProcessManager(join(process.cwd(), "./data/snapshot/v29-snapshot-rollback"), process.env.TYPESENSE_V29_BINARY_PATH!);
    try {
      await manager.startSingleNode("", 8109, 8106, "v29-snapshot-server");
      const brands_filter = await fetchSingleNode("/collections/products/overrides/brand-filter", { method: "GET" }, 8109);
      expect(brands_filter.ok).toBe(true);
      const brands_filter_overrides_data: any = await brands_filter.json();
      expect(brands_filter_overrides_data.rule.query).toEqual("{brand} phone");
      expect(brands_filter_overrides_data.rule.match).toEqual("contains");
      expect(brands_filter_overrides_data.filter_by).toEqual("brand:={brand}");
      expect(brands_filter_overrides_data.remove_matched_tokens).toEqual(true);

      const customize_apple = await fetchSingleNode("/collections/products/overrides/customize-apple", { method: "GET" }, 8109);
      expect(customize_apple.ok).toBe(true);
      const customize_apple_overrides_data: any = await customize_apple.json();
      expect(customize_apple_overrides_data.rule.query).toEqual("apple");
      expect(customize_apple_overrides_data.rule.match).toEqual("exact");
      expect(customize_apple_overrides_data.includes).toEqual([{ id: "422", position: 1 }, { id: "54", position: 2 }]);
      expect(customize_apple_overrides_data.excludes).toEqual([{ id: "287" }]);

      const dynamic_sort = await fetchSingleNode("/collections/products/overrides/dynamic-sort", { method: "GET" }, 8109);
      expect(dynamic_sort.ok).toBe(true);
      const dynamic_sort_overrides_data: any = await dynamic_sort.json();
      expect(dynamic_sort_overrides_data.rule.query).toEqual("{store}");
      expect(dynamic_sort_overrides_data.rule.match).toEqual("exact");
      expect(dynamic_sort_overrides_data.remove_matched_tokens).toEqual(true);
      expect(dynamic_sort_overrides_data.sort_by).toEqual("sales.{store}:desc, inventory.{store}:desc");
    } finally {
      await manager.shutdown();
    }
  });
});