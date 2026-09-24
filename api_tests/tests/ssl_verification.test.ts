import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:https";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { fetchSingleNode } from "../src/request";

let tempDir: string;
let mockServer: Server;
let mockPort: number;

function createSelfSignedCert() {
  tempDir = mkdtempSync(join(tmpdir(), "typesense-api-ssl-"));
  const configPath = join(tempDir, "openssl.cnf");
  const keyPath = join(tempDir, "key.pem");
  const certPath = join(tempDir, "cert.pem");

  writeFileSync(configPath, [
    "[req]",
    "default_bits = 2048",
    "prompt = no",
    "default_md = sha256",
    "distinguished_name = dn",
    "x509_extensions = v3_req",
    "",
    "[dn]",
    "CN = 127.0.0.1",
    "",
    "[v3_req]",
    "subjectAltName = @alt_names",
    "",
    "[alt_names]",
    "IP.1 = 127.0.0.1",
    "DNS.1 = localhost",
    "",
  ].join("\n"));

  execFileSync("openssl", [
    "req",
    "-x509",
    "-newkey", "rsa:2048",
    "-keyout", keyPath,
    "-out", certPath,
    "-days", "2",
    "-nodes",
    "-config", configPath,
  ], { stdio: "ignore" });

  return {
    key: readFileSync(keyPath),
    cert: readFileSync(certPath),
  };
}

function proxyToMock(sslVerify: boolean) {
  return fetchSingleNode("/proxy", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      url: `https://127.0.0.1:${mockPort}/ssl-check`,
      method: "GET",
      headers: {},
      ssl_verify: sslVerify,
    }),
  });
}

function proxySseToMock(sslVerify: boolean) {
  return fetchSingleNode("/proxy_sse", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      url: `https://127.0.0.1:${mockPort}/ssl-check`,
      method: "POST",
      body: "{}",
      headers: { "content-type": "application/json" },
      ssl_verify: sslVerify,
    }),
  });
}

describe(Phases.SINGLE_FRESH, () => {
  beforeAll(async () => {
    const credentials = createSelfSignedCert();
    mockServer = createServer(credentials, (_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });

    await new Promise<void>((resolve) => mockServer.listen(0, "127.0.0.1", resolve));
    mockPort = (mockServer.address() as AddressInfo).port;
  });

  afterAll(async () => {
    if (mockServer) {
      await new Promise<void>((resolve, reject) => {
        mockServer.close((error) => error ? reject(error) : resolve());
      });
    }

    if (tempDir) {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("should reject self-signed upstream certificates when ssl_verify is enabled", async () => {
    const relaxedRes = await proxyToMock(false);
    expect(relaxedRes.status).toBe(200);
    expect(await relaxedRes.json()).toEqual({ ok: true });

    const verifiedRes = await proxyToMock(true);
    expect(verifiedRes.status).toBe(500);
  });

  it("should return a deterministic SSE proxy error when SSL verification fails", async () => {
    const verifiedRes = await proxySseToMock(true);
    expect(verifiedRes.status).toBe(500);

    const body = await verifiedRes.json();
    expect(body.message).toBe("Server error on remote server. Please try again later.");
  });
});
