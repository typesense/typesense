import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Phases } from "../src/constants";

// Ports that no other phase or test suite uses.
const API_PORT = 8110;
const PEERING_PORT = 8105;
const START_TIMEOUT_MS = 20000;

type Certificate = { certPath: string; keyPath: string };

let tempDir: string;
let server: Bun.Subprocess | null = null;

function createCertificate(bits: number): Certificate {
  const certPath = join(tempDir, `cert-${bits}.pem`);
  const keyPath = join(tempDir, `key-${bits}.pem`);

  execFileSync("openssl", [
    "req",
    "-x509",
    "-newkey", `rsa:${bits}`,
    "-sha256",
    "-nodes",
    "-days", "2",
    "-subj", "/CN=localhost",
    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    "-keyout", keyPath,
    "-out", certPath,
  ], { stdio: "ignore" });

  return { certPath, keyPath };
}

function startHttpsServer({ certPath, keyPath }: Certificate) {
  const dataDir = join(tempDir, "data");
  rmSync(dataDir, { recursive: true, force: true });
  mkdirSync(dataDir, { recursive: true });

  const proc = Bun.spawn([
    process.env.TYPESENSE_BINARY_PATH!,
    `--data-dir=${dataDir}`,
    "--api-key=xyz",
    `--api-port=${API_PORT}`,
    `--peering-port=${PEERING_PORT}`,
    `--ssl-certificate=${certPath}`,
    `--ssl-certificate-key=${keyPath}`,
  ], { stdout: "pipe", stderr: "pipe" });

  server = proc;
  return proc;
}

async function waitForHttpsHealth(ca: string) {
  const start = Date.now();
  while (Date.now() - start < START_TIMEOUT_MS) {
    try {
      const res = await fetch(`https://localhost:${API_PORT}/health`, { tls: { ca } });
      const body = (await res.json()) as { ok?: boolean };
      if (res.ok && body.ok === true) return true;
    } catch {}
    await Bun.sleep(250);
  }
  return false;
}

describe(Phases.NO_PHASE, () => {
  beforeAll(() => {
    tempDir = mkdtempSync(join(tmpdir(), "typesense-api-https-"));
  });

  afterEach(async () => {
    if (server) {
      server.kill("SIGINT");
      await server.exited;
      server = null;
    }
  });

  afterAll(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("should serve HTTPS with a 2048-bit RSA certificate", async () => {
    const certificate = createCertificate(2048);
    startHttpsServer(certificate);

    expect(await waitForHttpsHealth(readFileSync(certificate.certPath, "utf8"))).toBe(true);
  });

  // OpenSSL 3.2+ defaults to security level 2, which rejects RSA keys under 2048 bits.
  it("should refuse to start with a 1024-bit RSA certificate", async () => {
    const proc = startHttpsServer(createCertificate(1024));

    const exitCode = await Promise.race([proc.exited, Bun.sleep(START_TIMEOUT_MS).then(() => null)]);
    expect(exitCode).not.toBeNull();
    expect(exitCode).not.toBe(0);

    const output = (await new Response(proc.stdout).text()) + (await new Response(proc.stderr).text());
    const certError = output.split("\n").find((line) => line.includes("server certificate file"));
    expect(certError).toContain("ee key too small");
  });
});
