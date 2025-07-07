import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { TypesenseProcessManager } from "../src/manager";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";
import { join } from "path";

describe(Phases.NO_PHASE, () => {
  it("validate analytics rules", async () => {
    const manager = new TypesenseProcessManager(join(process.cwd(), "./artifacts/v29-snapshot"));
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
          event_type: "query",
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
          event_type: "query",
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
          event_type: "query",
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
          event_type: "query",
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
          event_type: "query",
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
          event_type: "query",
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
          event_type: "query",
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
    } finally {
      await manager.shutdown();
    }
  });
});