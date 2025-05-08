import EventEmitter from "events";
import { constants, writeFile } from "fs/promises";
import { networkInterfaces } from "os";
import path from "path";
import type { ErrorWithMessage } from "@/utils/error";
import type { ChildProcess } from "child_process";
import type { Options as ExecaOptions } from "execa";
import type { Result } from "neverthrow";
import type { Ora } from "ora";
import type { HealthResponse } from "typesense/lib/Typesense/Health";

import { execa } from "execa";
import { err, errAsync, ok, okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { Client } from "typesense";

import { toErrorWithMessage } from "@/utils/error";
import { exists, safeEmptyDir, safeMakeOrEmptyDir } from "@/utils/fs";
import { logger } from "@/utils/logger";
import { isStringifiable } from "@/utils/stringifiable";

export const DEFAULT_IP_ADDRESS = "192.168.2.25";

interface AddressError {
  message: string;
  type: "address";
}

export interface SetupNodesOptions {
  skipCleanup?: boolean;
}

export interface NodeConfig {
  grpc: number;
  http: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"];
  dataDir: string;
}

/**
 * This is a helper type to ensure that the number of elements in the tuple is the same as the number of elements in the array.
 * It is used to ensure that the number of directories is the same as the number of nodes.
 */
type StringTupleOfLength<T extends readonly unknown[], V = string> = {
  [K in keyof T]: V;
} & { length: T["length"] };

export class TypesenseProcessController extends EventEmitter {
  process: ChildProcess | null;
  exitCode: number | null = null;
  http: number;
  private readonly apiKey: string;
  error: Error | null = null;
  client: Client;
  node: NodeConfig;

  constructor(process: ChildProcess, http: number, apiKey: string, node: NodeConfig) {
    super();
    this.http = http;
    this.process = process;
    this.apiKey = apiKey;
    this.node = node;

    this.client = new Client({
      nodes: [
        {
          host: "localhost",
          port: http,
          protocol: "http",
        },
      ],
      logLevel: "DEBUG",
      apiKey: this.apiKey,
      connectionTimeoutSeconds: 100,
      retryIntervalSeconds: 3,
      numRetries: 20,
    });

    this.process.on("close", (code, signal) => {
      this.exitCode = code;
      const logLevel = code === 0 ? "info" : "error";
      logger[logLevel](`[Node on port ${this.http}] Process exited with code=${code} signal=${signal}`);
      this.exitCode = code;
      this.emit("exit", { code, http: this.http });
    });

    this.process.on("exit", (code, signal) => {
      this.exitCode = code;
      const logLevel = code === 0 ? "info" : "error";
      logger[logLevel](`[Node on port ${this.http}] Process exited with code=${code} signal=${signal}`);
      this.exitCode = code;
      this.emit("exit", { code, http: this.http });
    });

    this.process.on("error", (error) => {
      this.error = error;
      logger.error(`[Node on port ${this.http}] Process error: ${error.message}`);
      this.emit("error", { error, http: this.http });
    });
  }

  public dispose(): ResultAsync<void, ErrorWithMessage> {
    if (!this.process || this.process.killed) {
      return okAsync(undefined);
    }

    return ResultAsync.fromPromise(
      new Promise<void>((resolve, reject) => {
        try {
          this.process!.kill("SIGTERM");
          this.process!.removeAllListeners();
          this.removeAllListeners();

          const killTimeout = setTimeout(() => {
            if (this.process && !this.process.killed) {
              this.process.kill("SIGKILL");
              this.process.removeAllListeners();
            }
          }, 20_000);

          this.process!.once("exit", () => {
            clearTimeout(killTimeout);
            this.process = null;
            this.error = null;
            this.exitCode = null;
            logger.info(`[Node on port ${this.http}] Process killed`);
            resolve();
          });

          this.process!.once("error", (error) => {
            clearTimeout(killTimeout);
            if (error instanceof Error && "signal" in error) {
              if (error.signal === "SIGTERM" || error.signal === "SIGKILL") {
                resolve();
                return;
              }
            }
            reject(new Error(error instanceof Error ? error.message : String(error)));
          });
        } catch (error) {
          if (error instanceof Error && "signal" in error) {
            if (error.signal === "SIGTERM" || error.signal === "SIGKILL") {
              resolve();
              return;
            }
          }
          reject(new Error(error instanceof Error ? error.message : String(error)));
        }
      }),
      toErrorWithMessage,
    );
  }
}

export class TypesenseProcessManager {
  public processes = new Map<number, TypesenseProcessController>();
  private readonly isInCi = process.env.CI === "true";
  private readonly ipAddress?: string;
  private readonly snapshotPath: string;
  private hasSetupExitHandler = false;

  constructor(
    private readonly spinner: Ora,
    private readonly binaryPath: string,
    private readonly apiKey: string,
    private readonly workingDirectory: string,
    snapshotPath?: string,
    ipAddress?: string,
  ) {
    this.ipAddress = ipAddress;
    this.snapshotPath = snapshotPath ?? path.join(this.workingDirectory, "snapshots");
    this.setupGlobalExitHandler();
  }

  public static readonly nodeToPortMap = [
    { grpc: 8107, http: 8108 },
    { grpc: 7107, http: 7108 },
    { grpc: 9107, http: 9108 },
  ] as const;

  restartProcess(port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"]) {
    const process = this.getProcessByHttpPort(port);

    if (process.isErr()) {
      return err(process.error);
    }

    this.spinner.start(`Restarting Process on ${process.value.http}`);

    return process.value.dispose().map(() => {
      this.processes.delete(process.value.http);
      return this.startProcess(process.value.node).map((newProcess) => {
        this.processes.set(newProcess.http, newProcess);
        this.spinner.succeed(`Restarted Process on ${newProcess.http}`);
      });
    });
  }

  stopProcess(
    port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"],
  ): ResultAsync<void, ErrorWithMessage> {
    const process = this.getProcessByHttpPort(port);

    if (process.isErr()) {
      return errAsync(process.error);
    }

    this.spinner.start("Stopping Typesense process");
    return process.value
      .dispose()
      .andThen(() => {
        this.processes.delete(process.value.http);
        this.spinner.succeed(`Stopped Typesense process on port ${process.value.http}`);
        return okAsync(undefined);
      })
      .mapErr((error) => {
        this.spinner.fail(`Failed to stop Typesense process: ${error.message}`);
        return error;
      });
  }

  getHealth(
    port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"],
  ): ResultAsync<HealthResponse, ErrorWithMessage> {
    const process = this.getProcessByHttpPort(port);

    if (process.isErr()) {
      return errAsync(process.error);
    }

    const spinner = ora().start(`Calling out to Typesense process on node ${process.value.http}\n`);

    return ResultAsync.fromPromise(process.value.client.health.retrieve(), toErrorWithMessage).andThen((res) => {
      if (!res.ok) {
        spinner.fail(`Typesense process on node ${process.value.http} is not healthy`);
        return errAsync({
          message: `Node ${process.value.http} health check failed`,
        });
      }
      spinner.succeed(`Typesense process on node ${process.value.http} is healthy`);
      return okAsync(res);
    });
  }

  snapshot(port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"]) {
    const process = this.getProcessByHttpPort(port);

    if (process.isErr()) {
      return err(process.error);
    }

    this.spinner.start(`Taking snapshot of Typesense process on node ${process.value.http}\n`);

    return ResultAsync.fromPromise(
      process.value.client.operations.perform("snapshot", {
        snapshot_path: this.snapshotPath,
      }),
      toErrorWithMessage,
    ).map(() => {
      this.spinner.succeed(`Took snapshot of Typesense process on node ${process.value.http}`);
      return okAsync(undefined);
    });
  }

  initNode(
    dataDir: string,
    port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"],
  ): ResultAsync<NodeConfig, ErrorWithMessage> {
    return exists(dataDir).andThen((exists) => {
      if (!exists) {
        return errAsync({ message: `${dataDir} does not exist` });
      }

      const portObj = TypesenseProcessManager.nodeToPortMap.find((ports) => ports.http === port);

      if (!portObj) {
        return errAsync({ message: `${port} is not a valid port` });
      }

      return okAsync({
        dataDir: dataDir,
        grpc: portObj.grpc,
        http: portObj.http,
      });
    });
  }

  startProcess(
    node: NodeConfig,
    options?: { multiNode?: false },
  ): ResultAsync<TypesenseProcessController, ErrorWithMessage> {
    const { grpc, http } = node;
    this.spinner.start(`Starting Typesense process on node ${http}\n`);

    return exists(this.workingDirectory)
      .andThen((exists) =>
        exists ?
          okAsync(undefined)
        : errAsync({
            message: `${this.workingDirectory} does not exist`,
          }),
      )
      .andThen(() =>
        exists(this.binaryPath, constants.X_OK | constants.F_OK).andThen((exists) => {
          if (!exists) {
            return errAsync({
              message: `${this.binaryPath} does not exist or is not executable`,
            });
          }

          const args = this.buildProcessArgs(node, options);
          if (args.isErr()) {
            return errAsync(args.error);
          }

          const execaOptions: ExecaOptions = {
            cwd: this.workingDirectory,
            stdio: "pipe",
            shell: false,
            windowsHide: true,
            cleanup: true,
            extendEnv: true,
            env: {
              HTTP_PROXY: "http://localhost:8443",
              HTTPS_PROXY: "http://localhost:8443",
            },
          };

          logger.info(`[Node on port ${http}] Starting process with ports HTTP=${http} gRPC=${grpc}\n`);
          logger.info(`[Node on port ${http}] Command: ${this.binaryPath} ${args.value.join(" ")}`);

          const typesenseProcess = execa(this.binaryPath, args.value, execaOptions);

          typesenseProcess.stdout?.on("data", (data) => {
            const message = isStringifiable(data) ? data.toString().trim() : "Not a stringifiable object";
            logger.info(`[Node on port ${http}] stdout: ${message}`);
          });

          typesenseProcess.stderr?.on("data", (data) => {
            const message = isStringifiable(data) ? data.toString().trim() : "Not a stringifiable object";
            logger.info(`[Node on port ${http}] stderr: ${message}`);
          });

          const typesenseInfo = new TypesenseProcessController(typesenseProcess, http, this.apiKey, node);

          this.processes.set(http, typesenseInfo);
          this.spinner.succeed(`Started Typesense process on node ${http}`);
          return okAsync(typesenseInfo);
        }),
      );
  }

  setupNodes(options?: SetupNodesOptions): ResultAsync<NodeConfig[], ErrorWithMessage> {
    return this.writeToNodesFile()
      .andThen(() => {
        if (options?.skipCleanup) {
          return okAsync(this.mapNodesToDirectories());
        }

        return this.createDataDirectories();
      })
      .andThen((directories) => {
        if (!this.verifyDataDirectories(directories)) {
          return errAsync({ message: "Number of directories does not match number of nodes" });
        }

        return okAsync(directories);
      })
      .andThen((directories) =>
        this.verifyDirectoriesExist(directories).andThen((exists) => {
          if (!exists) {
            return errAsync({ message: "Directories do not exist" });
          }

          this.spinner.succeed("All Directories exist");
          return okAsync(directories);
        }),
      )
      .andThen((directories) =>
        ResultAsync.combine(
          TypesenseProcessManager.nodeToPortMap.map(({ http }, index) => this.initNode(directories[index]!, http)),
        ),
      );
  }

  private buildProcessArgs(node: NodeConfig, options?: { multiNode?: false }): Result<string[], AddressError> {
    const address = this.findAdressOrThrow();

    if (address.isErr()) {
      return err(address.error);
    }

    const { grpc, http, dataDir } = node;
    const multiNodeArgs = [`--nodes`, path.join(this.workingDirectory, "nodes")];
    const ipArgs = ["--peering-address", address.value];
    const baseArgs = [
      `--data-dir=${dataDir}`,
      `--api-key=${this.apiKey}`,
      `--api-port`,
      `${http}`,
      `--api-address`,
      `0.0.0.0`,
      `--peering-port`,
      `${grpc}`,
    ];

    const args: string[] = [];
    if (options?.multiNode !== false) {
      args.push(...multiNodeArgs);
    }
    args.push(...ipArgs);
    args.push(...baseArgs);
    return ok(args);
  }

  private findAdressOrThrow(): Result<string, AddressError> {
    if (this.ipAddress) {
      return ok(this.ipAddress);
    }

    const defaultAddress = this.findDefaultNetworkAddress();
    if (!defaultAddress) {
      return err({
        message: "[TypesenseProcessManager]: No default network address found",
        type: "address",
      });
    }

    return ok(defaultAddress);
  }

  private findDefaultNetworkAddress(): string | null {
    const interfaces = networkInterfaces();

    if (!this.isInCi) {
      return DEFAULT_IP_ADDRESS;
    }

    // First try to find a 10.1.0.* address
    const preferredAddress = Object.values(interfaces)
      .flatMap((interfaceInfo) => interfaceInfo ?? [])
      .find((info) => info.family === "IPv4" && info.address.startsWith("10.1.0."))?.address;

    if (preferredAddress) {
      return preferredAddress;
    }

    // Fallback: find any non-internal IPv4 address
    return (
      Object.values(interfaces)
        .flatMap((interfaceInfo) => interfaceInfo ?? [])
        .find((info) => info.family === "IPv4" && !info.internal)?.address ?? null
    );
  }

  getProcessByHttpPort(httpPort: number): Result<TypesenseProcessController, ErrorWithMessage> {
    const process = this.processes.get(httpPort);
    if (!process) {
      return err({
        message: `[TypesenseProcessManager]: Process on port ${httpPort} not found`,
        type: "process",
      });
    }

    if (!process.process) {
      return err({
        message: `[TypesenseProcessManager]: Process on port ${httpPort} not found. Has it been started?`,
        type: "process",
      });
    }

    return ok(process);
  }

  private setupGlobalExitHandler() {
    if (this.hasSetupExitHandler) return;

    const handleExit = () => {
      for (const process of this.processes.values()) {
        process.dispose();
      }
    };

    const currentCount = global.process.listenerCount("exit");
    const needed = currentCount + 1;
    if (global.process.getMaxListeners() < needed) {
      global.process.setMaxListeners(needed);
    }

    global.process.on("exit", handleExit);
    this.hasSetupExitHandler = true;
  }

  private mapNodesToDirectories() {
    return Array.from({ length: 3 }, (_, i) => path.join(this.workingDirectory, `typesense-data-${i + 1}`));
  }

  private createDataDirectories(): ResultAsync<string[], ErrorWithMessage> {
    return ResultAsync.combine(
      this.mapNodesToDirectories().map((directory) =>
        safeMakeOrEmptyDir({
          directory,
          options: { recursive: true },
        }),
      ),
    ).map((values) =>
      values.map((value) => {
        ora().succeed(`Created data directory ${value}`);
        return value;
      }),
    );
  }

  private verifyDataDirectories(
    directories: readonly string[],
  ): directories is StringTupleOfLength<typeof TypesenseProcessManager.nodeToPortMap> {
    return directories.length == TypesenseProcessManager.nodeToPortMap.length;
  }

  private verifyDirectoriesExist(
    directories: StringTupleOfLength<typeof TypesenseProcessManager.nodeToPortMap>,
  ): ResultAsync<boolean, ErrorWithMessage> {
    return ResultAsync.combine(directories.map((dir) => exists(dir))).map(() => true);
  }

  private writeToNodesFile(): ResultAsync<string, ErrorWithMessage> {
    this.spinner.start("Writing nodes file");
    const nodesFile = path.join(this.workingDirectory, "nodes");

    const ipAddress = this.findAdressOrThrow();
    if (ipAddress.isErr()) {
      return errAsync(ipAddress.error);
    }

    const contents = TypesenseProcessManager.nodeToPortMap
      .map(({ grpc, http }) => `${ipAddress.value}:${grpc}:${http}`)
      .join(",");

    logger.info(`Writing nodes file to ${nodesFile} with contents:\n${contents}`);
    return ResultAsync.fromPromise(writeFile(nodesFile, contents, { encoding: "utf-8" }), toErrorWithMessage).map(
      () => {
        this.spinner.succeed("Nodes file written successfully");
        return nodesFile;
      },
    );
  }

  private emptyDataDirectories(): ResultAsync<void[], ErrorWithMessage> {
    return ResultAsync.combine(
      Array.from(this.processes.values()).map((value) =>
        ResultAsync.fromPromise(safeEmptyDir(value.node.dataDir), toErrorWithMessage).map(() => {
          ora().succeed(`Emptied data directory for node ${value.http}`);
        }),
      ),
    );
  }
}
