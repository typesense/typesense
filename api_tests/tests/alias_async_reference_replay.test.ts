import { describe, expect, it } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

const ALIAS = "parent_alias_replay";
const PARENT_V1 = "parent_v1_alias_replay";
const PARENT_BAD = "parent_bad_alias_replay";
const CHILDREN = "children_alias_replay";

async function joinedChildCount(): Promise<number> {
  const filterBy = `$${ALIAS}(code:=\`ok\`)`;
  const res = await fetchSingleNode(
    `/collections/${CHILDREN}/documents/search?q=*&query_by=parent_code&filter_by=${encodeURIComponent(filterBy)}`,
  );
  expect(res.ok).toBe(true);
  return (await res.json() as { found: number }).found;
}

describe(Phases.SINGLE_FRESH, () => {
  it("rejects an incompatible async-reference alias swap", async () => {
    let res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: PARENT_V1,
        fields: [{ name: "code", type: "string", facet: true }],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/aliases/${ALIAS}`, {
      method: "PUT",
      body: JSON.stringify({ collection_name: PARENT_V1 }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: CHILDREN,
        fields: [{
          name: "parent_code",
          type: "string",
          reference: `${ALIAS}.code`,
          async_reference: true,
        }],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/collections/${PARENT_V1}/documents`, {
      method: "POST",
      body: JSON.stringify({ id: "p-1", code: "ok" }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/collections/${CHILDREN}/documents`, {
      method: "POST",
      body: JSON.stringify({ id: "c-1", parent_code: "ok" }),
    });
    expect(res.ok).toBe(true);
    expect(await joinedChildCount()).toBe(1);

    res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: PARENT_BAD,
        fields: [{ name: "other", type: "string" }],
      }),
    });
    expect(res.ok).toBe(true);

    res = await fetchSingleNode(`/aliases/${ALIAS}`, {
      method: "PUT",
      body: JSON.stringify({ collection_name: PARENT_BAD }),
    });
    expect(res.status).toBe(500);
    expect((await res.json() as { message: string }).message).toContain("Referenced field `code` not found");
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("keeps the prior alias target and async join after restart", async () => {
    const res = await fetchSingleNode(`/aliases/${ALIAS}`);
    expect(res.ok).toBe(true);
    expect((await res.json() as { collection_name: string }).collection_name).toBe(PARENT_V1);
    expect(await joinedChildCount()).toBe(1);
  });
});
