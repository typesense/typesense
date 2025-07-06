import { describe, expect, it } from "bun:test";
import { Phases } from "../src/constants";
import { z } from "zod/v4";
import { fetchMultiNode, fetchSingleNode } from "../src/request";

const HealthResponse = z.object({
  ok: z.boolean(),
});

describe(Phases.SINGLE_FRESH, () => {
  it("should be healthy", async () => {
    const res = await fetchSingleNode("/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("should be healthy", async () => {
    const res = await fetchSingleNode("/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  })
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("should be healthy", async () => {
    const res = await fetchSingleNode("/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  })
});

describe(Phases.MULTI_FRESH, () => {
  it("should be health", async () => {
    const res = await fetchMultiNode(1, "/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  })
});

describe(Phases.MULTI_RESTARTED, () => {
  it("should be healthy", async () => {
    const res = await fetchMultiNode(2, "/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  })
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("should be healthy", async () => {
    const res = await fetchMultiNode(3, "/health");
    expect(res.ok).toBe(true);
    const data = HealthResponse.parse(await res.json());
    expect(res.status).toBe(200);
    expect(data.ok).toBe(true);
  })
});