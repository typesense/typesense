import { describe, expect, it } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

const COLLECTION_NAME = "show_nested_curly_repro";
const ALIAS_NAME = "show_v2_repro";
const PERFORMANCE_COUNTS = [54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 60];

function buildDocuments() {
  const docs: Array<Record<string, unknown>> = [];
  const sameObjectTitles = new Set<string>();
  const splitObjectTitles = new Set<string>();

  for (let showIndex = 0; showIndex < PERFORMANCE_COUNTS.length; showIndex++) {
    const showNum = showIndex + 1;
    const title = `Show ${showNum.toString().padStart(2, "0")}`;
    const performances: Array<Record<string, unknown>> = [];
    const matchMode = showIndex % 3;

    if (matchMode === 0) {
      sameObjectTitles.add(title);
    } else if (matchMode === 1) {
      splitObjectTitles.add(title);
    }

    for (let perfIndex = 1; perfIndex <= PERFORMANCE_COUNTS[showIndex]!; perfIndex++) {
      let localTimeHour: number;
      let minPrice: number;

      if (matchMode === 0 && perfIndex === 1) {
        localTimeHour = 19;
        minPrice = 4500;
      } else if (matchMode === 1 && perfIndex === 1) {
        localTimeHour = 19;
        minPrice = 6500;
      } else if (matchMode === 1 && perfIndex === 2) {
        localTimeHour = 18;
        minPrice = 4500;
      } else {
        switch ((perfIndex + showNum) % 6) {
          case 0:
            localTimeHour = 16;
            break;
          case 1:
            localTimeHour = 17;
            break;
          case 2:
            localTimeHour = 18;
            break;
          case 3:
            localTimeHour = 20;
            break;
          case 4:
            localTimeHour = 21;
            break;
          default:
            localTimeHour = 22;
            break;
        }

        minPrice = 6000 + showNum * 100 + perfIndex * 10;
      }

      performances.push({
        id: `show-${showNum.toString().padStart(2, "0")}-perf-${perfIndex.toString().padStart(3, "0")}`,
        startDate: { localTimeHour },
        pricing: {
          minPrice,
          maxPrice: minPrice + 1200,
        },
      });
    }

    docs.push({
      id: `show-${showNum.toString().padStart(2, "0")}`,
      title,
      performances,
    });
  }

  return { docs, sameObjectTitles, splitObjectTitles };
}

async function search(filterBy: string) {
  const res = await fetchSingleNode(
    `/collections/${ALIAS_NAME}/documents/search?q=show&query_by=title&include_fields=title&per_page=50&filter_by=${encodeURIComponent(filterBy)}`,
    { method: "GET" },
  );
  expect(res.ok).toBe(true);
  return (await res.json()) as {
    found: number;
    hits: Array<{ document: { title: string } }>;
  };
}

async function searchWithTimeout(filterBy: string, timeoutMs: number) {
  const url = new URL(`http://localhost:8108/collections/${ALIAS_NAME}/documents/search`);
  url.searchParams.set("q", "show");
  url.searchParams.set("query_by", "title");
  url.searchParams.set("include_fields", "title");
  url.searchParams.set("per_page", "50");
  url.searchParams.set("filter_by", filterBy);

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-TYPESENSE-API-KEY": "xyz",
    },
    signal: AbortSignal.timeout(timeoutMs),
  });

  expect(res.ok).toBe(true);
  return (await res.json()) as {
    found: number;
    hits: Array<{ document: { title: string } }>;
  };
}

describe(Phases.SINGLE_FRESH, () => {
  it("repro query should return promptly for deep same-object curly filters", async () => {
    const totalPerformances = PERFORMANCE_COUNTS.reduce((sum, count) => sum + count, 0);
    expect(PERFORMANCE_COUNTS.length).toBe(16);
    expect(totalPerformances).toBe(975);

    await fetchSingleNode(`/aliases/${ALIAS_NAME}`, { method: "DELETE" });
    await fetchSingleNode(`/collections/${COLLECTION_NAME}`, { method: "DELETE" });

    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: COLLECTION_NAME,
        enable_nested_fields: true,
        fields: [
          { name: "title", type: "string" },
          { name: "performances", type: "object[]" },
          { name: "performances.startDate.localTimeHour", type: "int32[]", range_index: true },
          { name: "performances.pricing.minPrice", type: "int64[]" },
          { name: "performances.pricing.maxPrice", type: "int64[]" },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/aliases/${ALIAS_NAME}`, {
      method: "PUT",
      body: JSON.stringify({
        collection_name: COLLECTION_NAME,
      }),
    });
    expect(res.ok).toBe(true);

    const { docs, sameObjectTitles, splitObjectTitles } = buildDocuments();
    const importBody = docs.map((doc) => JSON.stringify(doc)).join("\n");

    res = await fetchSingleNode(`/collections/${ALIAS_NAME}/documents/import?action=upsert`, {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body: importBody,
    });
    expect(res.ok).toBe(true);

    const nonCurly = await search("performances.startDate.localTimeHour:19 && performances.pricing.minPrice:<5000");
    expect(nonCurly.found).toBe(11);
    expect(new Set(nonCurly.hits.map((hit) => hit.document.title))).toEqual(
      new Set([...sameObjectTitles, ...splitObjectTitles]),
    );

    const hourOnly = await search("performances.{startDate.localTimeHour:19}");
    expect(hourOnly.found).toBe(11);

    const priceOnly = await search("performances.{pricing.minPrice:<5000}");
    expect(priceOnly.found).toBe(11);

    const sameObject = await searchWithTimeout(
      "performances.{startDate.localTimeHour:19 && pricing.minPrice:<5000}",
      5000,
    );
    expect(sameObject.found).toBe(sameObjectTitles.size);
    expect(new Set(sameObject.hits.map((hit) => hit.document.title))).toEqual(sameObjectTitles);
  });
});
