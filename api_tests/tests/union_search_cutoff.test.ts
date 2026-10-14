import { beforeAll, describe, expect, it } from "bun:test";
import { z } from "zod";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

const UnionResponse = z.object({
  found: z.number(),
  search_cutoff: z.boolean(),
  hits: z.array(
    z.object({
      document: z.object({ id: z.string() }),
      curated: z.boolean().optional(),
    }),
  ),
});

const ErrorResponse = z.object({ message: z.string() });

const COLLECTIONS = ["union_cutoff_0", "union_cutoff_1"];
const DOCS_PER_COLLECTION = 50;

// A search's cutoff is measured from the time the server received the request,
// so a budget of 0 ms is spent before the search starts. Each sub-search of a
// union resets the cutoff flag, so the union has to carry an earlier
// sub-search's cutoff past the ones that run after it.
function unionSearch(
  firstSearchParams: Record<string, unknown> = {},
  secondSearchParams: Record<string, unknown> = {},
) {
  return JSON.stringify({
    union: true,
    searches: [
      {
        collection: COLLECTIONS[0],
        q: "widget",
        query_by: "title",
        filter_by: "category:=kitchen",
        ...firstSearchParams,
      },
      {
        collection: COLLECTIONS[1],
        q: "widget",
        query_by: "title",
        filter_by: "category:=kitchen",
        ...secondSearchParams,
      },
    ],
  });
}

describe(Phases.SINGLE_FRESH, () => {
  beforeAll(async () => {
    for (const name of COLLECTIONS) {
      const created = await fetchSingleNode("/collections", {
        method: "POST",
        body: JSON.stringify({
          name,
          fields: [
            { name: "title", type: "string" },
            { name: "category", type: "string" },
          ],
        }),
      });
      expect(created.status).toBe(201);

      const jsonl = Array.from({ length: DOCS_PER_COLLECTION }, (_, i) =>
        JSON.stringify({ id: String(i), title: `widget ${i}`, category: "kitchen" }),
      ).join("\n");
      const imported = await fetchSingleNode(
        `/collections/${name}/documents/import?action=create`,
        { method: "POST", body: jsonl },
      );
      expect(imported.ok).toBe(true);
      const lines = (await imported.text()).trim().split("\n");
      expect(lines.length).toBe(DOCS_PER_COLLECTION);
      expect(lines.every((line) => line.includes('"success":true'))).toBe(true);
    }
  });

  it("serves the union in full when no budget is spent", async () => {
    const res = await fetchSingleNode("/multi_search", {
      method: "POST",
      body: unionSearch(),
    });
    expect(res.status).toBe(200);
    const data = UnionResponse.parse(await res.json());
    expect(data.search_cutoff).toBe(false);
    expect(data.found).toBe(2 * DOCS_PER_COLLECTION);
  });

  it("reports search_cutoff when an earlier sub-search was cut off", async () => {
    // Only the first sub-search has its budget spent; a pinned hit is all it
    // returns. The second runs with the default budget and returns real hits,
    // so the union succeeds but still has to report the cutoff.
    const res = await fetchSingleNode("/multi_search", {
      method: "POST",
      body: unionSearch({ search_cutoff_ms: 0, pinned_hits: "0:1" }),
    });
    expect(res.status).toBe(200);
    const data = UnionResponse.parse(await res.json());
    expect(data.search_cutoff).toBe(true);
    expect(data.found).toBe(1 + DOCS_PER_COLLECTION);
    expect(data.hits[0]?.document.id).toBe("0");
    expect(data.hits[0]?.curated).toBe(true);
  });

  it("answers 408 when a cut off union has nothing to return", async () => {
    const res = await fetchSingleNode("/multi_search", {
      method: "POST",
      body: unionSearch({ search_cutoff_ms: 0 }, { filter_by: "category:=office" }),
    });
    expect(res.status).toBe(408);
    expect(res.statusText).toBe("Request Timeout");
    const data = ErrorResponse.parse(await res.json());
    expect(data.message).toBe("Request Timeout");
  });
});
