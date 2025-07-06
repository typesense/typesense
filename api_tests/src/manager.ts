import { rmSync, mkdirSync, existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { env } from "bun";
import { networkInterfaces } from "node:os";

type ServerInstance = {
  process: Bun.Subprocess;
  name: string;
  port: number;
};

export class TypesenseProcessManager {
  baseDir: string;
  binaryPath: string;
  ipAddress: string;
  nodesFile: string;
  processes: Map<string, ServerInstance> = new Map();
  static multiNodeConfigs = [
    { name: "multi-node1", port: 5108, peerPort: 5107, dataDir: "typesense-data-1", logDir: "typesense-1", analyticsDir: "typesense-data-1/analytics_db" },
    { name: "multi-node2", port: 6108, peerPort: 6107, dataDir: "typesense-data-2", logDir: "typesense-2", analyticsDir: "typesense-data-2/analytics_db" },
    { name: "multi-node3", port: 7108, peerPort: 7107, dataDir: "typesense-data-3", logDir: "typesense-3", analyticsDir: "typesense-data-3/analytics_db" },
  ];
  static additionalConfigs = [
    "--enable-cors",
    "--enable-search-analytics",
    "--analytics-flush-interval=2",
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

  private spawnServer(name: string, args: string[], port: number) {
    const proc = Bun.spawn([this.binaryPath, ...args], {
      stdout: "pipe",
      stderr: "pipe",
      env: process.env,
      killSignal: "SIGINT",
    });

    this.processes.set(name, { process: proc, name, port });
  }

  async startSingleNode() {
    const dataDir = join(this.baseDir, "typesense-data");
    const analyticsDir = join(dataDir, "analytics_db");
    const args = [
      `--data-dir=${dataDir}`,
      `--api-key=xyz`,
      `--api-port=8108`,
      `--api-address=0.0.0.0`,
      `--log-dir=${join(this.baseDir, "logs", "typesense")}`,
      `--analytics-dir=${analyticsDir}`,
      ...TypesenseProcessManager.additionalConfigs,
    ];
    this.spawnServer("single-node", args, 8108);
    return this.waitForHealth(8108);
  }

  async startMultiNode() {
    const clusterStr = [
      `${this.ipAddress}:5107:5108`,
      `${this.ipAddress}:6107:6108`,
      `${this.ipAddress}:7107:7108`,
    ].join(",");

    writeFileSync(this.nodesFile, clusterStr);

    for (const node of TypesenseProcessManager.multiNodeConfigs) {
      const args = [
        `--nodes=${this.nodesFile}`,
        `--peering-address=${this.ipAddress}`,
        `--data-dir=${join(this.baseDir, node.dataDir)}`,
        `--api-key=xyz`,
        `--api-port=${node.port}`,
        `--api-address=0.0.0.0`,
        `--peering-port=${node.peerPort}`,
        `--log-dir=${join(this.baseDir, "logs", node.logDir)}`,
        `--analytics-dir=${join(this.baseDir, node.analyticsDir)}`,
        ...TypesenseProcessManager.additionalConfigs,
      ];
      this.spawnServer(node.name, args, node.port);
    }

    for (const node of TypesenseProcessManager.multiNodeConfigs) {
      await this.waitForHealth(node.port);
    }
  }

  async stopServer(name: string) {
    const instance = this.processes.get(name);
    if (!instance) return;
    instance.process.kill("SIGINT");
    await instance.process.exited;
    this.processes.delete(name);
  }

  async restartSingleNode() {
    await this.stopServer("single-node");
    await this.startSingleNode();
  }

  async restartMultiNode() {
    for (const name of ["multi-node1", "multi-node2", "multi-node3"]) {
      await this.stopServer(name);
    }
    await this.startMultiNode();
  }

  async createSnapshot(port: number) {
    const snapshotPath = join(this.baseDir, "snapshot", port === 8108 ? "single-node" : "multi-node");
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
    for (const [name, instance] of this.processes.entries()) {
      console.log(`➡️ Gracefully stopping ${name} (port ${instance.port})`);
      instance.process.kill("SIGINT");
      await instance.process.exited;
    }
    this.processes.clear();
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