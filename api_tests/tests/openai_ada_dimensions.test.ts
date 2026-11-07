import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:http";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

// fixed port, the url is persisted in the schema and called again after the restart
const MOCK_PORT = 18731;
const MOCK_URL = `http://127.0.0.1:${MOCK_PORT}`;
const NUM_DIM = 1536;
const ADA = "text-embedding-ada-002";
const V3_SMALL = "text-embedding-3-small";
const FIXED_SIZE = "fixed-size-embedder";
const ADA_COLLECTION = "openai_ada_002_dims";
const V3_SMALL_COLLECTION = "openai_3_small_dims";
const FIXED_SIZE_COLLECTION = "openai_fixed_size_dims";
const AZURE_COLLECTION = "azure_ada_002_dims";
const AZURE_PATH = "/openai/deployments/ada-002/embeddings";
const AZURE_URL = `${MOCK_URL}${AZURE_PATH}?api-version=2024-06-01`;

type EmbeddingRequest = {
  model?: string;
  input: string | string[];
  dimensions?: number;
};

type RecordedRequest = EmbeddingRequest & { path: string };

let mockServer: Server | undefined;
let requests: RecordedRequest[] = [];

// deterministic vector seeded from an fnv-1a hash of the text
function embed(text: string): number[] {
  let seed = 2166136261;
  for (const ch of text) {
    seed = Math.imul(seed ^ ch.codePointAt(0)!, 16777619);
  }

  const vector: number[] = [];
  for (let i = 0; i < NUM_DIM; i++) {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), seed | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    vector.push((((t ^ (t >>> 14)) >>> 0) / 4294967296) * 2 - 1);
  }
  return vector;
}

// azure requests carry no model, the deployment is in the path
function rejectsDimensions(path: string, body: EmbeddingRequest) {
  return path === AZURE_PATH || body.model === ADA || body.model === FIXED_SIZE;
}

function respond(path: string, body: EmbeddingRequest) {
  if (rejectsDimensions(path, body) && "dimensions" in body) {
    return {
      status: 400,
      json: {
        error: {
          message: "This model does not support specifying dimensions.",
          type: "invalid_request_error",
        },
      },
    };
  }

  const inputs = Array.isArray(body.input) ? body.input : [body.input];
  return {
    status: 200,
    json: {
      object: "list",
      model: body.model,
      data: inputs.map((text, index) => ({ object: "embedding", index, embedding: embed(text) })),
    },
  };
}

async function startMock() {
  requests = [];
  mockServer = createServer(async (req, res) => {
    let raw = "";
    for await (const chunk of req) {
      raw += chunk;
    }

    const path = new URL(req.url!, MOCK_URL).pathname;
    const body = JSON.parse(raw) as EmbeddingRequest;
    requests.push({ ...body, path });

    const { status, json } = respond(path, body);
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(json));
  });

  await new Promise<void>((resolve, reject) => {
    mockServer!.once("error", reject);
    mockServer!.listen(MOCK_PORT, "127.0.0.1", resolve);
  });
}

async function stopMock() {
  if (!mockServer) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    mockServer!.close((error) => (error ? reject(error) : resolve()));
  });
  mockServer = undefined;
}

function requestsFor(model: string) {
  return requests.filter((r) => r.model === model);
}

function requestsTo(path: string) {
  return requests.filter((r) => r.path === path);
}

async function createCollection(name: string, modelConfig: Record<string, string>) {
  return fetchSingleNode("/collections", {
    method: "POST",
    body: JSON.stringify({
      name,
      fields: [
        { name: "title", type: "string" },
        { name: "embedding", type: "float[]", embed: { from: ["title"], model_config: modelConfig } },
      ],
    }),
  });
}

async function createEmbeddingCollection(name: string, model: string) {
  return createCollection(name, { model_name: `openai/${model}`, api_key: "test-key", url: MOCK_URL });
}

async function createAzureCollection(name: string) {
  return createCollection(name, { model_name: "azure/ada-002-deployment", api_key: "test-key", url: AZURE_URL });
}

async function importDocs(collection: string, docs: { id: string; title: string }[]) {
  const res = await fetchSingleNode(`/collections/${collection}/documents/import?action=create`, {
    method: "POST",
    body: docs.map((doc) => JSON.stringify(doc)).join("\n"),
  });
  expect(res.status).toBe(200);
  const lines = (await res.text()).split("\n").filter((line) => line.length > 0);
  return lines.map((line) => JSON.parse(line));
}

async function semanticSearch(collection: string, q: string) {
  const params = new URLSearchParams({ q, query_by: "embedding", prefix: "false", exclude_fields: "embedding" });
  const res = await fetchSingleNode(`/collections/${collection}/documents/search?${params}`);
  const body: any = await res.json();
  return { status: res.status, message: body.message, hits: body.hits ?? [] };
}

const SEED_DOCS = [
  { id: "1", title: "rockets and orbital launches" },
  { id: "2", title: "sourdough bread baking" },
  { id: "3", title: "alpine hiking trails" },
];

// seed after the restarted boot, which replays the raft log and would re-validate the model against a stopped mock
describe(Phases.SINGLE_RESTARTED, () => {
  beforeAll(startMock);
  afterAll(stopMock);

  it("creates an ada-002 collection without num_dim and persists the model size", async () => {
    const res = await createEmbeddingCollection(ADA_COLLECTION, ADA);
    expect(res.status).toBe(201);
    const body: any = await res.json();
    const field = body.fields.find((f: any) => f.name === "embedding");
    expect(field.num_dim).toBe(NUM_DIM);
  });

  it("embeds and searches ada-002 documents", async () => {
    const results = await importDocs(ADA_COLLECTION, SEED_DOCS);
    expect(results.filter((r) => !r.success)).toEqual([]);

    const search = await semanticSearch(ADA_COLLECTION, "space travel");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBe(SEED_DOCS.length);
  });

  it("creates and searches a text-embedding-3-small collection the same way", async () => {
    const res = await createEmbeddingCollection(V3_SMALL_COLLECTION, V3_SMALL);
    expect(res.status).toBe(201);

    const results = await importDocs(V3_SMALL_COLLECTION, SEED_DOCS);
    expect(results.filter((r) => !r.success)).toEqual([]);

    const search = await semanticSearch(V3_SMALL_COLLECTION, "space travel");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBe(SEED_DOCS.length);
  });

  it("creates and searches a fixed-size openai model that rejects dimensions", async () => {
    const res = await createEmbeddingCollection(FIXED_SIZE_COLLECTION, FIXED_SIZE);
    expect(res.status).toBe(201);
    const body: any = await res.json();
    const field = body.fields.find((f: any) => f.name === "embedding");
    expect(field.num_dim).toBe(NUM_DIM);

    const results = await importDocs(FIXED_SIZE_COLLECTION, SEED_DOCS);
    expect(results.filter((r) => !r.success)).toEqual([]);

    const search = await semanticSearch(FIXED_SIZE_COLLECTION, "space travel");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBe(SEED_DOCS.length);
  });

  it("creates and searches an azure ada-002 deployment", async () => {
    const res = await createAzureCollection(AZURE_COLLECTION);
    expect(res.status).toBe(201);
    const body: any = await res.json();
    const field = body.fields.find((f: any) => f.name === "embedding");
    expect(field.num_dim).toBe(NUM_DIM);

    const results = await importDocs(AZURE_COLLECTION, SEED_DOCS);
    expect(results.filter((r) => !r.success)).toEqual([]);

    const search = await semanticSearch(AZURE_COLLECTION, "space travel on azure");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBe(SEED_DOCS.length);
  });
});

// the snapshot boot is the cold load where the persisted num_dim marks the embedder as custom dims
describe(Phases.SINGLE_SNAPSHOT, () => {
  beforeAll(startMock);
  afterAll(stopMock);

  it("semantic search on ada-002 still works", async () => {
    const search = await semanticSearch(ADA_COLLECTION, "bread recipes after a restart");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBeGreaterThan(0);
  });

  it("importing into ada-002 still embeds the document", async () => {
    const results = await importDocs(ADA_COLLECTION, [{ id: "4", title: "deep sea diving" }]);
    expect(results).toEqual([{ success: true }]);

    const res = await fetchSingleNode(`/collections/${ADA_COLLECTION}/documents/4`);
    expect(res.status).toBe(200);
    const doc: any = await res.json();
    expect(doc.embedding).toHaveLength(NUM_DIM);
  });

  it("never sends dimensions to ada-002", () => {
    const adaRequests = requestsFor(ADA);
    expect(adaRequests.length).toBeGreaterThan(0);
    expect(adaRequests.filter((r) => "dimensions" in r)).toEqual([]);
  });

  it("keeps sending dimensions to text-embedding-3-small", async () => {
    const search = await semanticSearch(V3_SMALL_COLLECTION, "mountain walks after a restart");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });

    const v3Requests = requestsFor(V3_SMALL);
    expect(v3Requests.length).toBeGreaterThan(0);
    for (const r of v3Requests) {
      expect(r.dimensions).toBe(NUM_DIM);
    }
  });

  // query strings stay unique in the file, the azure query cache key is the bare text
  it("semantic search on a fixed-size openai model still works", async () => {
    const search = await semanticSearch(FIXED_SIZE_COLLECTION, "fixed size vectors after a restart");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBeGreaterThan(0);
  });

  it("importing into a fixed-size openai model still embeds the document", async () => {
    const results = await importDocs(FIXED_SIZE_COLLECTION, [{ id: "4", title: "deep sea diving" }]);
    expect(results).toEqual([{ success: true }]);

    const res = await fetchSingleNode(`/collections/${FIXED_SIZE_COLLECTION}/documents/4`);
    expect(res.status).toBe(200);
    const doc: any = await res.json();
    expect(doc.embedding).toHaveLength(NUM_DIM);
  });

  it("stops sending dimensions to a fixed-size openai model after the first rejection", () => {
    const fixedSizeRequests = requestsFor(FIXED_SIZE);
    expect(fixedSizeRequests.length).toBeGreaterThan(1);
    expect(fixedSizeRequests.slice(1).filter((r) => "dimensions" in r)).toEqual([]);
  });

  it("semantic search on an azure ada-002 deployment still works", async () => {
    const search = await semanticSearch(AZURE_COLLECTION, "azure deployments after a restart");
    expect({ status: search.status, message: search.message }).toEqual({ status: 200, message: undefined });
    expect(search.hits.length).toBeGreaterThan(0);
  });

  it("importing into an azure ada-002 deployment still embeds the document", async () => {
    const results = await importDocs(AZURE_COLLECTION, [{ id: "4", title: "deep sea diving" }]);
    expect(results).toEqual([{ success: true }]);

    const res = await fetchSingleNode(`/collections/${AZURE_COLLECTION}/documents/4`);
    expect(res.status).toBe(200);
    const doc: any = await res.json();
    expect(doc.embedding).toHaveLength(NUM_DIM);
  });

  it("stops sending dimensions to an azure ada-002 deployment after the first rejection", () => {
    const azureRequests = requestsTo(AZURE_PATH);
    expect(azureRequests.length).toBeGreaterThan(1);
    expect(azureRequests.slice(1).filter((r) => "dimensions" in r)).toEqual([]);
  });
});
