import EventEmitter from "events";
import { constants } from "fs/promises";
import path from "path";
import type { FilesystemService } from "@/services/fs";
import type { ErrorWithMessage } from "@/utils/error";
import type { ChildProcess } from "child_process";
import type { Options as ExecaOptions } from "execa";
import type { Ora } from "ora";
import type { CollectionCreateSchema } from "typesense/lib/Typesense/Collections";
import type { ConversationModelCreateSchema } from "typesense/lib/Typesense/ConversationModel";
import type { SearchParams } from "typesense/lib/Typesense/Documents";

import { execa } from "execa";
import { errAsync, ok, okAsync, Result, ResultAsync } from "neverthrow";
import { Client } from "typesense";

import { toErrorWithMessage } from "@/utils/error";
import { delay } from "@/utils/execa";
import { logger, LogLevel } from "@/utils/logger";
import { isStringifiable } from "@/utils/stringifiable";

export const DEFAULT_IP_ADDRESS = "192.168.2.25";

export interface NodeConfig {
  grpc: number;
  http: number;
  dataDir: string;
}

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
      logLevel: logger.getLevel() === LogLevel.DEBUG ? "debug" : "info",
      apiKey: this.apiKey,
      connectionTimeoutSeconds: 100,
      retryIntervalSeconds: 10,
      numRetries: 6,
    });

    this.process.on("exit", (code, signal) => {
      this.exitCode = code;
      const logLevel = code === 0 ? "debug" : "error";
      logger[logLevel](`[Node ${http}] Process exited with code=${code} signal=${signal}`);
      this.exitCode = code;
      this.emit("exit", { code, http });
    });

    this.process.on("error", (error) => {
      this.error = error;
      logger.error(`[Node ${http}] Process error: ${error.message}`);
      this.emit("error", { error, http });
    });
  }

  public dispose(): Result<void, ErrorWithMessage> {
    if (!this.process || this.process.killed) {
      return ok(undefined);
    }

    return Result.fromThrowable(() => {
      try {
        this.process!.kill("SIGTERM");
        this.process!.removeAllListeners();
        this.removeAllListeners();

        // Set up SIGKILL fallback
        setTimeout(() => {
          if (this.process && !this.process.killed) {
            this.process.kill("SIGKILL");
            this.process.removeAllListeners();
          }
        }, 20_000);

        // Clean up resources
        this.process = null;
        this.error = null;
        this.exitCode = null;
      } catch (error) {
        if (error instanceof Error && "signal" in error) {
          if (error.signal === "SIGTERM" || error.signal === "SIGKILL") {
            return;
          }
        }
        throw error;
      }
    }, toErrorWithMessage)();
  }
}

export class TypesenseProcessManager {
  public processes = new Map<number, TypesenseProcessController>();
  private readonly ipAddress?: string;
  private readonly snapshotPath: string;
  private hasSetupExitHandler = false;

  constructor(
    private readonly spinner: Ora,
    private readonly binaryPath: string,
    private readonly apiKey: string,
    private readonly workingDirectory: string,
    private readonly fsService: FilesystemService,
    snapshotPath?: string,
    ipAddress?: string,
  ) {
    this.ipAddress = ipAddress;
    this.snapshotPath = snapshotPath ?? path.join(this.workingDirectory, "snapshots");
    this.setupGlobalExitHandler();
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

  get getSnapshotPath() {
    return this.snapshotPath;
  }

  public static readonly nodeToPortMap = {
    0: { grpc: 8107, http: 8108 },
    1: { grpc: 7107, http: 7108 },
    2: { grpc: 9107, http: 9108 },
  } as const;

  restartProcess(process: TypesenseProcessController) {
    this.spinner.start(`Restarting Process on ${process.http}`);

    return process.dispose().map(() => {
      this.processes.delete(process.http);
      return delay(10_000).andThen(() =>
        this.startProcess(process.node).map((newProcess) => {
          this.processes.set(newProcess.http, newProcess);
          this.spinner.succeed(`Restarted Process on ${newProcess.http}`);
        }),
      );
    });
  }

  stopProcess(process: TypesenseProcessController) {
    this.spinner.start("Stopping Typesense process");
    return process
      .dispose()
      .map(() => {
        this.processes.delete(process.http);
        this.spinner.succeed("Stopped Typesense process");
      })
      .mapErr((error) => {
        this.spinner.fail(`Failed to stop Typesense process: ${error.message}`);
        return error;
      });
  }

  callOutToProcess(process: TypesenseProcessController) {
    this.spinner.start(`Calling out to Typesense process on node ${process.http}\n`);

    return ResultAsync.fromPromise(process.client.health.retrieve(), toErrorWithMessage).andThen((res) => {
      if (!res.ok) {
        this.spinner.fail(`Typesense process on node ${process.http} is not healthy`);
        return errAsync({
          message: `Node ${process.http} health check failed`,
        });
      }
      this.spinner.succeed(`Typesense process on node ${process.http} is healthy`);
      return okAsync(res);
    });
  }

  snapshot(process: TypesenseProcessController) {
    this.spinner.start(`Taking snapshot of Typesense process on node ${process.http}\n`);

    return ResultAsync.fromPromise(
      process.client.operations.perform("snapshot", {
        snapshot_path: this.snapshotPath,
      }),
      toErrorWithMessage,
    ).map(() => {
      this.spinner.succeed(`Took snapshot of Typesense process on node ${process.http}`);
      return okAsync(undefined);
    });
  }

  getCollection(process: TypesenseProcessController, collectionName: string) {
    this.spinner.start(`Getting collection ${collectionName} on node ${process.http}\n`);

    return ResultAsync.fromPromise(process.client.collections(collectionName).retrieve(), toErrorWithMessage).map(
      (res) => {
        this.spinner.succeed(`Got collection ${collectionName} on node ${process.http}`);
        return res;
      },
    );
  }

  queryCollection(process: TypesenseProcessController, options: { collectionName: string; query: SearchParams }) {
    this.spinner.start(`Querying collection ${options.collectionName} on node ${process.http}\n`);
    return ResultAsync.fromPromise(
      process.client.collections(options.collectionName).documents().search(options.query),
      toErrorWithMessage,
    ).map((res) => {
      this.spinner.succeed(`Queried collection ${options.collectionName} on node ${process.http}`);
      logger.debug(`Query result: ${JSON.stringify(res)}`);
      return res;
    });
  }

  indexDocuments(process: TypesenseProcessController, collectionName: string, documents: Record<string, unknown>[]) {
    return ResultAsync.fromPromise(
      process.client.collections(collectionName).documents().import(documents),
      toErrorWithMessage,
    );
  }

  createCollection(process: TypesenseProcessController, schema: CollectionCreateSchema) {
    return ResultAsync.fromPromise(process.client.collections().create(schema), toErrorWithMessage);
  }

  getConversationModel(process: TypesenseProcessController, model_id: string) {
    return ResultAsync.fromPromise(process.client.conversations().models(model_id).retrieve(), toErrorWithMessage);
  }

  createConversationModel(process: TypesenseProcessController, model: ConversationModelCreateSchema) {
    return ResultAsync.fromPromise(process.client.conversations().models().create(model), toErrorWithMessage);
  }

  initNode(dataDir: string, port: number): ResultAsync<NodeConfig, ErrorWithMessage> {
    return this.fsService.exists(dataDir).andThen((exists) => {
      if (!exists) {
        return errAsync({ message: `${dataDir} does not exist` });
      }

      const portObj = Object.values(TypesenseProcessManager.nodeToPortMap).find((ports) => ports.http === port);

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

    return this.fsService
      .exists(this.workingDirectory)
      .andThen((exists) =>
        exists ?
          okAsync(undefined)
        : errAsync({
            message: `${this.workingDirectory} does not exist`,
          }),
      )
      .andThen(() =>
        this.fsService.exists(this.binaryPath, constants.X_OK | constants.F_OK).andThen((exists) => {
          if (!exists) {
            return errAsync({
              message: `${this.binaryPath} does not exist or is not executable`,
            });
          }

          const args = this.buildProcessArgs(node, options);
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

          logger.debug(`[Node ${http}] Starting process with ports HTTP=${http} gRPC=${grpc}\n`);
          logger.debug(`[Node ${http}] Command: ${this.binaryPath} ${args.join(" ")}`);

          const typesenseProcess = execa(this.binaryPath, args, execaOptions);

          typesenseProcess.stdout?.on("data", (data) => {
            const message = isStringifiable(data) ? data.toString().trim() : "Not a stringifiable object";
            logger.debug(`[Node ${http}] stdout: ${message}`);
          });

          typesenseProcess.stderr?.on("data", (data) => {
            const message = isStringifiable(data) ? data.toString().trim() : "Not a stringifiable object";
            logger.debug(`[Node ${http}] stderr: ${message}`);
          });

          const typesenseInfo = new TypesenseProcessController(typesenseProcess, http, this.apiKey, node);

          this.processes.set(http, typesenseInfo);
          this.spinner.succeed(`Started Typesense process on node ${http}`);
          return okAsync(typesenseInfo);
        }),
      );
  }

  private buildProcessArgs(node: NodeConfig, options?: { multiNode?: false }): string[] {
    const { grpc, http, dataDir } = node;
    const multiNodeArgs = [`--nodes`, path.join(this.workingDirectory, "nodes")];
    const ipArgs = ["--peering-address", this.ipAddress];
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
    if (this.ipAddress) {
      args.push(...(ipArgs as string[]));
    }
    args.push(...baseArgs);
    return args;
  }
}
