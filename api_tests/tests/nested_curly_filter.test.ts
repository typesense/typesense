import { describe, expect, it } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

const COLLECTION_NAME = "show_nested_curly_repro";
const ALIAS_NAME = "show_v2_repro";
const PERFORMANCE_COUNTS = [54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 60];
const HIGH_LOAD_COLLECTION_NAME = "performance_nested_curly_high_load";
const HIGH_LOAD_QUERY_TIMEOUT_MS = 20_000;
const HIGH_LOAD_SHOWS = 200;
const HIGH_LOAD_PERFS_PER_SHOW = 25;
const HIGH_LOAD_BANDS_PER_PERF_MIN = 4;
const HIGH_LOAD_BANDS_PER_PERF_MAX = 7;

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

function rngFactory(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0xffffffff;
  };
}

function buildHighLoadPerformances() {
  const rng = rngFactory(42);
  const pick = <T>(arr: T[]) => arr[Math.floor(rng() * arr.length)]!;
  const intBetween = (lo: number, hi: number) => lo + Math.floor(rng() * (hi - lo + 1));
  const seatTypes = ["Stalls", "Circle", "Upper", "Boxes", "Premium"];
  const performances: Array<{
    id: string;
    showSlug: string;
    sourceType: string;
    sourceId: string;
    "startDate.iso": string;
    "startDate.unix": number;
    "startDate.timezone": string;
    "startDate.localTimeHour": number;
    "startDate.localTimeDay": number;
    bands: Array<{
      seatType: string;
      minSeatPrice: number;
      maxSeatPrice: number;
      sizes: number[];
    }>;
  }> = [];
  let id = 0;

  for (let showIndex = 0; showIndex < HIGH_LOAD_SHOWS; showIndex++) {
    const showSlug = `show-${showIndex.toString().padStart(4, "0")}`;
    for (let perfIndex = 0; perfIndex < HIGH_LOAD_PERFS_PER_SHOW; perfIndex++) {
      const bandCount = intBetween(HIGH_LOAD_BANDS_PER_PERF_MIN, HIGH_LOAD_BANDS_PER_PERF_MAX);
      const bands = [];
      for (let bandIndex = 0; bandIndex < bandCount; bandIndex++) {
        const minSeatPrice = intBetween(1500, 9000);
        const maxSeatPrice = minSeatPrice + intBetween(500, 5000);
        const sizes = [
          ...new Set(
            Array.from({ length: intBetween(1, 3) }, () => intBetween(1, 6)),
          ),
        ].sort((a, b) => a - b);
        bands.push({
          seatType: pick(seatTypes),
          minSeatPrice,
          maxSeatPrice,
          sizes,
        });
      }

      const startUnix = 1_770_000_000 + intBetween(0, 60 * 24 * 60 * 60);
      performances.push({
        id: `perf-${(id++).toString().padStart(6, "0")}`,
        showSlug,
        sourceType: "lineup",
        sourceId: `src-${id}`,
        "startDate.iso": new Date(startUnix * 1000).toISOString(),
        "startDate.unix": startUnix,
        "startDate.timezone": "Europe/London",
        "startDate.localTimeHour": intBetween(10, 22),
        "startDate.localTimeDay": intBetween(0, 6),
        bands,
      });
    }
  }

  return performances;
}

function countSameObjectMatches(
  performances: ReturnType<typeof buildHighLoadPerformances>,
  matcher: (band: ReturnType<typeof buildHighLoadPerformances>[number]["bands"][number]) => boolean,
) {
  return performances.filter((performance) => performance.bands.some(matcher)).length;
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

async function highLoadSearchWithTimeout(filterBy: string, timeoutMs: number) {
  const url = new URL(`http://localhost:8108/collections/${HIGH_LOAD_COLLECTION_NAME}/documents/search`);
  url.searchParams.set("q", "*");
  url.searchParams.set("include_fields", "id");
  url.searchParams.set("per_page", "1");
  url.searchParams.set("filter_by", filterBy);

  let res: Response;
  try {
    res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        "X-TYPESENSE-API-KEY": "xyz",
      },
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    throw new Error(`High-load same-object filter timed out after ${timeoutMs}ms: ${filterBy}`, {
      cause: error,
    });
  }

  expect(res.ok).toBe(true);
  return (await res.json()) as { found: number };
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

  it("high-load same-object filters over array child fields should return promptly", async () => {
    await fetchSingleNode(`/collections/${HIGH_LOAD_COLLECTION_NAME}`, { method: "DELETE" });

    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: HIGH_LOAD_COLLECTION_NAME,
        enable_nested_fields: true,
        fields: [
          { name: "id", type: "string" },
          { name: "showSlug", type: "string", facet: true, index: true, sort: false },
          { name: "sourceType", type: "string", index: true },
          { name: "sourceId", type: "string", index: true },
          { name: "startDate.iso", type: "string", index: false },
          { name: "startDate.unix", type: "int64", index: true, sort: true },
          { name: "startDate.timezone", type: "string", index: false },
          { name: "startDate.localTimeHour", type: "int32", index: true, range_index: true },
          { name: "startDate.localTimeDay", type: "int32", index: true, range_index: true },
          { name: "bands", type: "object[]", optional: true, index: true, facet: false },
          { name: "bands.seatType", type: "string[]", optional: true, facet: true, index: true },
          { name: "bands.minSeatPrice", type: "int32[]", optional: true, index: true, range_index: true },
          { name: "bands.maxSeatPrice", type: "int32[]", optional: true, index: true, range_index: true },
          { name: "bands.sizes", type: "int32[]", optional: true, index: true, range_index: true },
        ],
      }),
    });
    expect(res.ok).toBe(true);

    const performances = buildHighLoadPerformances();
    expect(performances.length).toBe(5000);

    const importBody = performances.map((doc) => JSON.stringify(doc)).join("\n");
    res = await fetchSingleNode(`/collections/${HIGH_LOAD_COLLECTION_NAME}/documents/import?action=create`, {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body: importBody,
    });
    expect(res.ok).toBe(true);

    const twoPredicateExpected = countSameObjectMatches(
      performances,
      (band) => band.sizes.some((size) => size >= 2) && band.minSeatPrice <= 7500,
    );
    const threePredicateExpected = countSameObjectMatches(
      performances,
      (band) => band.sizes.some((size) => size >= 2) && band.minSeatPrice <= 7500 && band.maxSeatPrice >= 4000,
    );

    const twoPredicateResult = await highLoadSearchWithTimeout(
      "bands.{sizes:>=2 && minSeatPrice:<=7500}",
      HIGH_LOAD_QUERY_TIMEOUT_MS,
    );
    expect(twoPredicateResult.found).toBe(twoPredicateExpected);

    const threePredicateResult = await highLoadSearchWithTimeout(
      "bands.{sizes:>=2 && minSeatPrice:<=7500 && maxSeatPrice:>=4000}",
      HIGH_LOAD_QUERY_TIMEOUT_MS,
    );
    expect(threePredicateResult.found).toBe(threePredicateExpected);
  });
});
