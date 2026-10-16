// Reproduce the interaction between a stalled import response and a single
// batched-indexer worker. Run against an isolated local Typesense server.
import assert from "node:assert/strict";
import net from "node:net";
import http2 from "node:http2";
import fs from "node:fs";

const base = process.env.TYPESENSE_URL ?? "http://127.0.0.1:18108";
const apiKey = process.env.TYPESENSE_API_KEY ?? "xyz";
const documentCount = Number(process.env.REPRO_DOCUMENTS ?? 256);
const payloadSize = Number(process.env.REPRO_PAYLOAD_BYTES ?? 90_000);
const waitForServerTimeout = process.env.REPRO_WAIT_FOR_TIMEOUT === "1";
const useHttp2 = process.env.REPRO_HTTP2 === "1";
const collection = `response_stall_${Date.now()}`;
const headers = { "X-TYPESENSE-API-KEY": apiKey };

async function request(path, init = {}) {
  const defaultContentType = init.body === undefined ? {} : { "Content-Type": "application/json" };
  return fetch(`${base}${path}`, {
    ...init,
    headers: { ...headers, ...defaultContentType, ...init.headers },
    signal: init.signal ?? AbortSignal.timeout(30_000),
  });
}

async function collectionCount() {
  const response = await request(`/collections/${collection}`);
  const result = await response.json();
  assert.equal(response.status, 200, JSON.stringify(result));
  return result.num_documents;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function sessionHealth(session) {
  const sibling = session.request({ ":path": "/health" });
  let status;
  sibling.on("response", headers => { status = headers[":status"]; });
  const body = await new Promise((resolve, reject) => {
    let text = "";
    sibling.setEncoding("utf8");
    sibling.on("data", chunk => { text += chunk; });
    sibling.on("end", () => resolve(text));
    sibling.on("error", reject);
    sibling.setTimeout(5_000, () => sibling.destroy(new Error("sibling health timed out")));
    sibling.end();
  });
  assert.equal(status, 200, body);
  assert.equal(JSON.parse(body).ok, true);
}

async function waitFor(predicate, timeoutMs, label) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    const value = await predicate();
    if (value) return value;
    await delay(100);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function main() {
  let session;
  let socket;
  let keepSessionActive;
  let closeTimeout;
  const pendingRequests = [];
  const cleanupAbort = new AbortController();
  let watchdogLogOffset;
  const serverLog = process.env.REPRO_SERVER_LOG;
  if (waitForServerTimeout && !useHttp2) {
    const fixtureToken = process.env.REPRO_FIXTURE_TOKEN;
    assert(serverLog && fixtureToken,
      "HTTP/1 watchdog validation requires the isolated extended-H2O-timeout fixture: " +
      "bash api_tests/reproductions/import_response_stall_http1_watchdog.sh");
    const log = fs.readFileSync(serverLog, "utf8");
    assert(log.includes(`[import-response-stall-fixture:${fixtureToken}] req_timeout_ms=180000 req_io_timeout_ms=180000`),
      "missing verified extended H2O HTTP/1 timeouts; run import_response_stall_http1_watchdog.sh");
    watchdogLogOffset = Buffer.byteLength(log);
  }
  try {
    const health = await request("/health");
    assert.equal(health.status, 200, "debug server is unavailable");

    let response = await request("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: collection,
        fields: [{ name: "payload", type: "string", index: false }],
      }),
    });
    assert.equal(response.status, 201, await response.text());

    const rows = Array.from({ length: documentCount }, (_, index) => JSON.stringify({
      id: `doc_${index}`,
      payload: "x".repeat(payloadSize),
    }));
    const body = `${rows.join("\n")}\n`;
    const estimatedResponseBytes = Buffer.byteLength(body);

    // Control: the same large import completes when the client consumes output.
    const controlCollection = `${collection}_control`;
    response = await request("/collections", {
      method: "POST",
      body: JSON.stringify({
        name: controlCollection,
        fields: [{ name: "payload", type: "string", index: false }],
      }),
    });
    assert.equal(response.status, 201, await response.text());
    const controlStart = Date.now();
    response = await request(`/collections/${controlCollection}/documents/import?action=upsert&batch_size=1&return_doc=true`, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body,
      signal: AbortSignal.timeout(120_000),
    });
    const controlText = await response.text();
    assert.equal(response.status, 200, controlText.slice(0, 500));
    const controlRows = controlText.trimEnd().split("\n").map(line => JSON.parse(line));
    assert.equal(controlRows.length, documentCount);
    controlRows.forEach((row, index) => {
      assert.equal(row.success, true, JSON.stringify(row));
      assert.deepEqual(row.document, JSON.parse(rows[index]));
    });
    console.log(JSON.stringify({
      phase: "consuming-control",
      documents: documentCount,
      responseBytes: Buffer.byteLength(controlText),
      elapsedMs: Date.now() - controlStart,
    }));

    // Leave the response unread. Content-Length completes the HTTP/1 upload
    // without half-closing TCP, so peer disconnect cannot release the server wait.
    const url = new URL(`/collections/${collection}/documents/import?action=upsert&batch_size=1&return_doc=true`, base);
    session = useHttp2 ? http2.connect(base) : null;
    if (session) session.on("error", error => console.error("HTTP/2 session:", error));
    const siblingErrors = [];
    let siblingChecks = 0;
    keepSessionActive = session ? setInterval(() => {
      sessionHealth(session).then(() => { siblingChecks++; }).catch(error => siblingErrors.push(error));
    }, 1_000) : null;
    keepSessionActive?.unref();
    socket = session ? session.request({
      ":method": "POST", ":path": `${url.pathname}${url.search}`,
      "x-typesense-api-key": apiKey, "content-type": "text/plain",
      "content-length": Buffer.byteLength(body),
    }) : net.connect(Number(url.port), url.hostname);
    const transportErrors = [];
    socket.on("error", error => transportErrors.push(error));
    let peerEnded = false;
    socket.on("end", () => { peerEnded = true; });
    const peerClosed = new Promise(resolve => socket.once("close", resolve));
    socket.pause();
    socket.setTimeout(120_000, () => socket.destroy(new Error("probe transport timed out")));
    if (!session) await new Promise((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    const requestBytes = Buffer.from(
      `POST ${url.pathname}${url.search} HTTP/1.1\r\n` +
      `Host: ${url.host}\r\n` +
      `X-TYPESENSE-API-KEY: ${apiKey}\r\n` +
      "Content-Type: text/plain\r\n" +
      `Content-Length: ${Buffer.byteLength(body)}\r\n` +
      "Connection: keep-alive\r\n\r\n" + body,
    );
    await new Promise((resolve, reject) => {
      socket.once("error", reject);
      if (session) socket.end(body, resolve);
      else socket.write(requestBytes, resolve);
    });
    const uploadedAt = Date.now();

    const counts = [];
    const observationDeadline = Date.now() + 15_000;
    let stalledAt = null;
    while (Date.now() < observationDeadline) {
      const count = await collectionCount();
      counts.push(count);
      if (count > 0 && count < documentCount && counts.length >= 5 &&
          counts.slice(-5).every(value => value === count)) {
        stalledAt = count;
        break;
      }
      if (count === documentCount) break;
      await delay(500);
    }

    console.log(JSON.stringify({
      phase: "unread-response-observation",
      uploadedDocuments: documentCount,
      estimatedResponseBytes,
      observedCounts: counts,
      stalledAt,
    }));
    assert.notEqual(stalledAt, null,
      "the current debug server did not stall with this response size and unread client");

    // A subsequent write to the same collection shares the single indexer lane.
    let probeSettled = false;
    const probePromise = request(`/collections/${collection}/documents?action=upsert`, {
      method: "POST",
      body: JSON.stringify({ id: "after_stall", payload: "probe" }),
      signal: AbortSignal.any([AbortSignal.timeout(90_000), cleanupAbort.signal]),
    }).then(async result => {
      probeSettled = true;
      return { status: result.status, body: await result.text() };
    });
    pendingRequests.push(probePromise);
    probePromise.catch(() => {}); // It may reject before the main flow reaches its await.
    await delay(1_000);
    const writeBlocked = !probeSettled;

    // Trigger an on-demand Raft snapshot while the handler may still own the
    // pause lock. The snapshot path is isolated under /tmp.
    let snapshotSettled = false;
    const snapshotPath = `/tmp/issue3032-snapshot-${Date.now()}`;
    const snapshotPromise = request(`/operations/snapshot?snapshot_path=${encodeURIComponent(snapshotPath)}`, {
      method: "POST",
      body: "",
      signal: AbortSignal.any([AbortSignal.timeout(90_000), cleanupAbort.signal]),
    }).then(async result => {
      snapshotSettled = true;
      return { status: result.status, body: await result.text() };
    });
    pendingRequests.push(snapshotPromise);
    snapshotPromise.catch(() => {});
    await delay(1_000);
    const snapshotBlocked = !snapshotSettled;
    console.log(JSON.stringify({ phase: "blocked-work", writeBlocked, snapshotBlocked }));
    assert.equal(writeBlocked, true, "queued write must first be blocked by the stalled response");
    assert.equal(snapshotBlocked, true, "snapshot must first wait for the stalled handler");

    // The server should abort a response that makes no progress and continue
    // indexing the already received import documents.
    if (waitForServerTimeout) {
      await waitFor(async () => {
        const count = await collectionCount();
        return count >= documentCount ? count : null;
      }, 75_000, "remaining import documents after the response timeout");
    } else {
      // Generator disposal on connection close should notify the response wait.
      socket.destroy();
    }
    const probeResult = await probePromise;
    const snapshotResult = await snapshotPromise;
    const completedCount = await waitFor(async () => {
      const count = await collectionCount();
      return count === documentCount + 1 ? count : null;
    }, 30_000, "all import documents and the completed probe write");
    const exported = await request(`/collections/${collection}/documents/export`);
    assert.equal(exported.status, 200);
    const documents = (await exported.text()).trimEnd().split("\n").map(line => JSON.parse(line));
    const byId = new Map(documents.map(document => [document.id, document]));
    assert.equal(byId.size, documentCount + 1);
    rows.forEach(row => {
      const expected = JSON.parse(row);
      assert.deepEqual(byId.get(expected.id), expected);
    });
    assert.deepEqual(byId.get("after_stall"), { id: "after_stall", payload: "probe" });

    if (waitForServerTimeout) {
      // A paused readable cannot observe FIN behind buffered response bytes.
      // Drain only after recovery, and require actual peer termination.
      const received = [];
      socket.on("data", chunk => received.push(chunk));
      socket.resume();
      await Promise.race([
        peerClosed,
        new Promise((resolve, reject) => {
          closeTimeout = setTimeout(() => reject(new Error("server did not terminate stalled response")), 10_000);
        }),
      ]);
      if (session) {
        assert.equal(socket.rstCode, http2.constants.NGHTTP2_CANCEL);
        assert(transportErrors.every(error => error.code === "ERR_HTTP2_STREAM_ERROR"));
        await sessionHealth(session);
        assert(siblingChecks > 0);
        assert.deepEqual(siblingErrors, []);
      } else {
        assert(peerEnded || transportErrors.some(error => error.code === "ECONNRESET"),
          "server must close or reset the response");
        assert(transportErrors.every(error => error.code === "ECONNRESET"));
        const rawResponse = Buffer.concat(received).toString();
        assert.match(rawResponse, /^HTTP\/1\.1 200 /);
        assert.match(rawResponse, /transfer-encoding: chunked\r\n/i);
        assert(!rawResponse.endsWith("\r\n0\r\n\r\n"), "aborted response must lack final chunk");
        assert(rawResponse.length < Buffer.byteLength(controlText), "aborted response must be truncated");
        const newLog = fs.readFileSync(serverLog).subarray(watchdogLogOffset).toString();
        assert(newLog.includes(`Aborting stalled batched response for POST /collections/${collection}/documents/import after 60000 ms without response progress.`),
          "this request has no batch-indexer watchdog warning; generic H2O timeout is not watchdog evidence");
      }
      console.log(JSON.stringify({ phase: "peer-abort", protocol: session ? "h2" : "http1", peerEnded,
        resetCode: session ? socket.rstCode : null, responseBytes: Buffer.concat(received).length,
        errors: transportErrors.map(error => error.code), siblingChecks }));
    }
    console.log(JSON.stringify({
      phase: waitForServerTimeout ? "after-response-timeout" : "after-disconnect",
      completedCount,
      elapsedSinceUploadMs: Date.now() - uploadedAt,
      writeResult: probeResult,
      snapshotResult,
    }));
    assert.equal(completedCount, documentCount + 1, "all import documents and the queued probe write should be indexed");
    assert.equal(probeResult.status, 201, probeResult.body);
    assert.equal(snapshotResult.status, 201, snapshotResult.body);
    assert.equal(JSON.parse(snapshotResult.body).success, true);
    console.log(waitForServerTimeout ?
      "RECOVERED: server response timeout released the stalled import lane." :
      "REPRODUCED: unread import response stalled the single indexer lane; disconnect released the work.");
  } finally {
    clearInterval(keepSessionActive);
    clearTimeout(closeTimeout);
    cleanupAbort.abort();
    socket?.destroy();
    session?.destroy();
    await Promise.allSettled(pendingRequests);
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
