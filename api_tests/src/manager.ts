import { rmSync, mkdirSync, existsSync, writeFileSync, openSync, closeSync } from "node:fs";
import { join } from "node:path";
import { env } from "bun";
import { networkInterfaces } from "node:os";
import { fetchMultiNode } from "./request";

type ServerInstance = {
  process: Bun.Subprocess;
  name: string;
  port: number;
  logDir: string;
};

// a graceful stop takes under 2 s; one still running after this is hung
const STOP_TIMEOUT_MS = 30_000;

export type MultiNodeConfig = {
  name: string;
  port: number;
  peerPort: number;
  dataDir: string;
  logDir: string;
  analyticsDir: string;
};

export class TypesenseProcessManager {
  baseDir: string;
  binaryPath: string;
  ipAddress: string;
  nodesFile: string;
  processes: Map<string, ServerInstance> = new Map();
  static multiNodeConfigs: MultiNodeConfig[] = [
    { name: "multi-node1", port: 5108, peerPort: 5107, dataDir: "typesense-data-1", logDir: "typesense-1", analyticsDir: "typesense-data-1/analytics_db" },
    { name: "multi-node2", port: 6108, peerPort: 6107, dataDir: "typesense-data-2", logDir: "typesense-2", analyticsDir: "typesense-data-2/analytics_db" },
    { name: "multi-node3", port: 7108, peerPort: 7107, dataDir: "typesense-data-3", logDir: "typesense-3", analyticsDir: "typesense-data-3/analytics_db" },
  ];
  static additionalConfigs = [
    "--enable-cors",
    "--enable-search-analytics",
    "--analytics-flush-interval=3600",
    "--analytics-minute-rate-limit=1000"
  ]

  constructor(
    baseDir: string = env.TYPESENSE_DATA_DIR!,
    binaryPath: string = env.TYPESENSE_BINARY_PATH!,
  ) {
    this.baseDir = baseDir;
    this.binaryPath = binaryPath;
    this.ipAddress = this.getIpAddress();
    this.nodesFile = join(this.baseDir, "nodes");

    this.installSignalHandlers();
  }

  cleanDataDirs() {
    const dirs = [
      "typesense-data",
      "typesense-data/analytics_db",
      "typesense-data-1", "typesense-data-2", "typesense-data-3",
      "typesense-data-1/analytics_db", "typesense-data-2/analytics_db", "typesense-data-3/analytics_db",
      "logs/typesense",
      "logs/typesense-1", "logs/typesense-2", "logs/typesense-3",
      "snapshot/single-node",
      "snapshot/multi-node"
    ];

    for (const dir of dirs) {
      const fullPath = join(this.baseDir, dir);
      if (existsSync(fullPath)) rmSync(fullPath, { recursive: true, force: true });
      mkdirSync(fullPath, { recursive: true });
    }
  }

  private spawnServer(name: string, args: string[], port: number, logDir: string) {
    // appended, so a restarted server keeps the earlier output next to its log
    const stdout = openSync(join(logDir, `${name}.stdout`), "a");
    const stderr = openSync(join(logDir, `${name}.stderr`), "a");
    try {
      const proc = Bun.spawn([this.binaryPath, ...args], {
        stdout,
        stderr,
        env: process.env,
        killSignal: "SIGINT",
      });
      this.processes.set(name, { process: proc, name, port, logDir });
    } finally {
      closeSync(stdout);
      closeSync(stderr);
    }
  }

  async startSingleNode(dataDir: string = "typesense-data", port: number = 8108, peeringPort: number = 8107, name: string = "single-node") {
    dataDir = join(this.baseDir, dataDir);
    const analyticsDir = join(dataDir, "analytics_db");
    const logDir = join(this.baseDir, "logs", "typesense");
    mkdirSync(logDir, { recursive: true });
    mkdirSync(analyticsDir, { recursive: true });
    const args = [
      `--data-dir=${dataDir}`,
      `--api-key=xyz`,
      `--api-port=${port}`,
      `--api-address=0.0.0.0`,
      `--log-dir=${join(this.baseDir, "logs", "typesense")}`,
      `--analytics-dir=${analyticsDir}`,
      `--peering-address=${this.ipAddress}`,
      `--peering-port=${peeringPort}`,
      ...TypesenseProcessManager.additionalConfigs,
    ];
    this.spawnServer(name, args, port, logDir);
    return this.waitForHealth(port);
  }

  getNodesConfigString(configs: MultiNodeConfig[] = TypesenseProcessManager.multiNodeConfigs) {
    return configs
      .map((node) => `${this.ipAddress}:${node.peerPort}:${node.port}`)
      .join(",");
  }

  writeNodesConfig(configs: MultiNodeConfig[] = TypesenseProcessManager.multiNodeConfigs, nodesFile: string = this.nodesFile) {
    writeFileSync(nodesFile, this.getNodesConfigString(configs));
  }

  async startClusterNode(node: MultiNodeConfig, nodesFile: string = this.nodesFile, waitForHealth: boolean = true) {
    const dataDir = join(this.baseDir, node.dataDir);
    const analyticsDir = join(this.baseDir, node.analyticsDir);
    const logDir = join(this.baseDir, "logs", node.logDir);
    mkdirSync(logDir, { recursive: true });
    mkdirSync(analyticsDir, { recursive: true });
    mkdirSync(dataDir, { recursive: true });

    const args = [
      `--nodes=${nodesFile}`,
      `--peering-address=${this.ipAddress}`,
      `--data-dir=${dataDir}`,
      `--api-key=xyz`,
      `--api-port=${node.port}`,
      `--api-address=0.0.0.0`,
      `--peering-port=${node.peerPort}`,
      `--log-dir=${logDir}`,
      `--analytics-dir=${analyticsDir}`,
      ...TypesenseProcessManager.additionalConfigs,
    ];

    this.spawnServer(node.name, args, node.port, logDir);
    if (waitForHealth) {
      await this.waitForHealth(node.port);
    }
  }

  async startMultiNode(configs: MultiNodeConfig[] = TypesenseProcessManager.multiNodeConfigs, nodesFile: string = this.nodesFile) {
    const firstNode = configs[0];
    if (!firstNode) {
      throw new Error("At least one node config is required");
    }

    this.writeNodesConfig(configs, nodesFile);

    for (const node of configs) {
      await this.startClusterNode(node, nodesFile, false);
    }

    await this.waitForHealth(firstNode.port);
    await this.electLeader(firstNode.port);

    for (const node of configs) {
      await this.waitForHealth(node.port);
    }
  }

  async stopServer(name: string) {
    const instance = this.processes.get(name);
    if (!instance) return;
    this.processes.delete(name);
    await this.stopProcess(instance, "SIGINT");
  }

  // Sends the signal and waits for exit. A server still running after STOP_TIMEOUT_MS gets every thread's stack
  // written next to its log, is killed, and fails the caller.
  private async stopProcess(instance: ServerInstance, signal: "SIGINT" | "SIGTERM") {
    instance.process.kill(signal);
    let timer: Timer | undefined;
    const exited = await Promise.race([
      instance.process.exited.then(() => true),
      new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(false), STOP_TIMEOUT_MS); }),
    ]);
    clearTimeout(timer);
    if (exited) return;

    const pid = instance.process.pid;
    const stackFile = join(instance.logDir, `${instance.name}-${pid}.stacks`);
    let stacks: string;
    try {
      const gdb = Bun.spawnSync({
        cmd: ["sudo", "-n", "gdb", "-p", `${pid}`, "-batch", "-ex", "set pagination off", "-ex", "thread apply all bt"],
        stdout: "pipe",
        stderr: "pipe",
      });
      stacks = `${gdb.stdout.toString()}${gdb.stderr.toString()}`;
    } catch (err) {
      stacks = `gdb could not run: ${err}`;
    }
    writeFileSync(stackFile, stacks);
    instance.process.kill("SIGKILL");
    await instance.process.exited;
    throw new Error(`${instance.name} (pid ${pid}, port ${instance.port}) did not exit within ${STOP_TIMEOUT_MS} ms ` +
      `of ${signal}; thread stacks are in ${stackFile}`);
  }

  async restartSingleNode() {
    await this.stopServer("single-node");
    await this.startSingleNode();
  }

  async restartMultiNode(configs: MultiNodeConfig[] = TypesenseProcessManager.multiNodeConfigs, nodesFile: string = this.nodesFile) {
    await fetchMultiNode(1, "/status");
    for (const node of [...configs].reverse()) {
      await this.stopServer(node.name);
    }
    await this.startMultiNode(configs, nodesFile);
  }

  async electLeader(port: number = 5108) {
    const res = await fetch(`http://localhost:${port}/operations/vote`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-TYPESENSE-API-KEY": "xyz",
      },
    });
    if (!res.ok) throw new Error(`Elect leader failed: ${res.statusText}`);
  }

  async createSnapshot(port: number, snapshot_path: string = "") {
    const snapshotPath = snapshot_path || join(this.baseDir, "snapshot", (port === 8108 ? "single-node" : "multi-node"));
    const url = `http://localhost:${port}/operations/snapshot?snapshot_path=${snapshotPath}`;
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-TYPESENSE-API-KEY": "xyz",
      },
    });

    if (!res.ok) throw new Error(`Snapshot failed: ${res.statusText}`);
  }

  private async waitForHealth(port: number, timeout = 20000) {
    const start = Date.now();
    while (Date.now() - start < timeout) {
      try {
        const res = await fetch(`http://localhost:${port}/health`);
        if (res.ok) return;
      } catch {}
      await new Promise((r) => setTimeout(r, 250));
    }
    throw new Error(`Timed out waiting for /health on port ${port}`);
  }

  async shutdown() {
    console.log("🔻 Cleaning up Typesense processes...");
    const instances = [...this.processes.values()];
    this.processes.clear();
    let firstError: unknown;
    for (const instance of instances) {
      console.log(`➡️ Gracefully stopping ${instance.name} (port ${instance.port})`);
      try {
        await this.stopProcess(instance, "SIGTERM");
      } catch (err) {
        console.error(err);
        firstError ??= err;
      }
    }
    if (firstError) throw firstError;
  };

  private installSignalHandlers() {
    process.on("SIGINT", () => this.shutdown());
    process.on("SIGTERM", () => this.shutdown());
    process.on("exit", () => this.shutdown());

    process.on("uncaughtException", async (err) => {
      console.error("❌ Uncaught Exception:", err);
      await this.shutdown();
      process.exit(1);
    });

    process.on("unhandledRejection", async (reason) => {
      console.error("❌ Unhandled Rejection:", reason);
      await this.shutdown();
      process.exit(1);
    });
  }

  private getIpAddress() {
    const interfaces = networkInterfaces();
    for (const interfaceName in interfaces) {
      const addresses = interfaces[interfaceName];
      if (!addresses) continue;
      for (const address of addresses) {
        if (address.family === "IPv4" && !address.internal) {
          return address.address;
        }
      }
    }
    throw new Error("No IP address found");
  }
}
