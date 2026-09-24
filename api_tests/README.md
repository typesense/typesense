# Typesense API Tests

A fully‑featured API test harness that spins up Typesense in both single‑ and multi‑node modes, executes phase‑scoped test suites, and tears everything down again. Each logical phase of testing (fresh start, restart, snapshot restore, etc.) is completely isolated and reproducible.

## Phase Matrix

| Path | Phase | Description |
|------|-------|-------------|
| **Single-node** | `SINGLE_FRESH` | A brand-new Typesense server is started. |
| | `SINGLE_RESTARTED` | Same server is stopped and started again using the *same* data directory. |
| | `SINGLE_SNAPSHOT` | A snapshot is taken, the server is restarted, and the snapshot is restored. |
| **Multi-node (3-node RAFT)** | `MULTI_FRESH` | A fresh 3-node cluster is started. |
| | `MULTI_RESTARTED` | Every node is gracefully stopped and restarted. |
| | `MULTI_SNAPSHOT` | A snapshot is taken on the leader, the cluster is restarted, and the snapshot is restored on all nodes. |

The phases within a given path run **sequentially** because each later phase re‑uses the data produced by the previous one. The single‑node and multi‑node paths run **in parallel** to reduce build time.

> [!NOTE]
> Single-node server is always run on 8108 when running api_tests

> [!NOTE]
> Multi-node server is always run on 5108, 6108, 7108 when running api_tests

## Writing a New Test Suite

Create a `*.test.ts` file under `api_tests/tests/` and declare only the `describe` blocks you need. For instance, if your feature must persist across a single‑node restart but does not require snapshots or clustering, you might only target `SINGLE_FRESH` + `SINGLE_RESTARTED`.

```ts
import { describe, it, expect } from "bun:test";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

// 1️⃣ Seed data in the fresh phase
describe(Phases.SINGLE_FRESH, () => {
  it("create collection", async () => {
    const res = await fetchSingleNode("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "companies",
        fields: [{ name: "company_name", type: "string" }],
      }),
    });
    expect(res.ok).toBe(true);
  });
});

// 2️⃣ Assert persistence after restart
describe(Phases.SINGLE_RESTARTED, () => {
  it("collection should persist", async () => {
    const res = await fetchSingleNode("/collections/companies");
    expect(res.status).toBe(200);
  });
```

An template file containing all the phases,  
```ts
describe(Phases.SINGLE_FRESH, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

describe(Phases.SINGLE_RESTARTED, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

describe(Phases.SINGLE_SNAPSHOT, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

describe(Phases.MULTI_FRESH, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});

```
Also, you can use a describe with `NO_PHASE` when you can entirely manage the lifecyle of the typesense-server using `TypesenseProcessManager`
```ts
describe(Phases.NO_PHASE, () => {
  it("test 1", async () => {
   ...
  });

  it("test 2", async () => {
   ...
  });
});
```

Please refer to `collections.test.ts` and `health.test.ts` for reference tests. These examples are the quickest way to copy‑paste a skeleton for new features.

### ⚠️ Data dependencies between phases

All *cluster‑level* operations (snapshot creation, server restart, cluster restart) are performed **between** phases by `TypesenseProcessManager`.  That means **any state you need in a later phase must be created in an earlier one** under the same path:

* Want to verify a collection still exists after `SINGLE_RESTARTED`? Create it during `SINGLE_FRESH`.
* Need documents to appear in search results after `MULTI_SNAPSHOT`? Index them during `MULTI_FRESH` or `MULTI_RESTARTED`.

Phases are therefore best thought of as a *progressive timeline* rather than independent test shards.
## Request Helpers

### `fetchSingleNode(path: string, init?: RequestInit): Promise<Response>`

* Targets `http://localhost:8108`.
* Automatically injects the header `X-TYPESENSE-API-KEY: xyz`.
* Use it for all **single-node** phases.

```ts
const res = await fetchSingleNode("/health");
```

### `fetchMultiNode(node: 1 | 2 | 3, path: string, init?: RequestInit): Promise<Response>`

* Maps node → port as **1 ⇒ 5108**, **2 ⇒ 6108**, **3 ⇒ 7108**.
* Polls the RAFT *commit-index* across all three nodes until they match, guaranteeing cluster‑wide consistency before your request runs.
* Same API‑key injection as above.

```ts
// Query the leader (node 1)
await fetchMultiNode(1, "/collections/companies");
```

Happy testing! 🎉
