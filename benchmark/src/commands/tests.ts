import { writeFile } from "fs/promises";
import { networkInterfaces } from "os";
import path from "path";
import type { TypesenseProcessController } from "@/services/typesense-process";
import type { ErrorWithMessage } from "@/utils/error";
import type { Result } from "neverthrow";
import type { Ora } from "ora";
import type { CollectionCreateSchema } from "typesense/lib/Typesense/Collections";
import type { ConversationModelSchema } from "typesense/lib/Typesense/ConversationModel";
import type { SearchParams } from "typesense/lib/Typesense/Documents";

import { Command } from "commander";
import { err, errAsync, ok, okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { z } from "zod";

import { ServiceContainer } from "@/services/container";
import { DEFAULT_TYPESENSE_GIT_URL } from "@/services/git";
import { OpenAIProxyService } from "@/services/openai-mock";
import { DEFAULT_IP_ADDRESS, TypesenseProcessManager } from "@/services/typesense-process";
import { toErrorWithMessage } from "@/utils/error";
import { delay } from "@/utils/execa";
import { logger, LogLevel } from "@/utils/logger";
import { dirName, findRoot } from "@/utils/package-info";
import { parseOptions } from "@/utils/parse";

const cwd = process.cwd();

const integrationTestOptionsSchema = z.object({
  containerName: z.string(),
  imageName: z.string(),
  typesenseGitUrl: z.string(),
  workingDirectory: z.string(),
  commitHash: z.string().optional(),
  verbose: z.boolean(),
  yes: z.boolean(),
  binary: z.string().optional(),
  apiKey: z.string(),
  openAIKey: z.string({
    message: "OpenAI API key must be provided either through environment variable or directly",
  }),
  numDim: z
    .string()
    .transform((value) => parseInt(value))
    .pipe(z.number().min(0)),
  ip: z.string().optional(),
  snapshotPath: z.string(),
});

type IntegrationTestOptions = z.infer<typeof integrationTestOptionsSchema>;

interface NodeConfig {
  grpc: number;
  http: number;
  dataDir: string;
}

class IntegrationTests {
  private readonly ipAddress: string | null;
  private readonly isInCi = process.env.CI === "true";
  private readonly openAIProxy: OpenAIProxyService;

  public static baseCollectionName = "base_collection";
  public static openAICollectionName = "openai_collection";
  public static conversationStoreName = "conversation_store";
  public static conversationModelName = "gpt-4-turbo";
  private nodes: NodeConfig[] = [];

  constructor(
    protected readonly services: ServiceContainer,
    private readonly spinner: Ora,
    public readonly typesenseProcessManager: TypesenseProcessManager,
    private readonly workingDirectory: string,
    private readonly openAIKey: string,
    private readonly numDim: number,
    ipAddress?: string,
  ) {
    this.ipAddress = ipAddress ?? this.findDefaultNetworkAddress();
    this.numDim = numDim;
    this.openAIProxy = new OpenAIProxyService(spinner, workingDirectory, this.numDim);
  }

  private createDataDirectories(workingDirectory: string = this.workingDirectory) {
    this.spinner.start("Creating data directories");
    const promises = Array.from({ length: 3 }, (_, i) =>
      this.services.get("fs").createDirectory(path.join(workingDirectory, `typesense-data-${i + 1}`)),
    );

    return ResultAsync.combine(promises).andThen((res) => {
      this.spinner.succeed("Created data directories");
      return okAsync(res as [string, string, string]);
    });
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

  private mapNodesToDirectories(dataDirs: [string, string, string]): Result<NodeConfig[], ErrorWithMessage> {
    const nodes: NodeConfig[] = [];

    for (const [nodeId, ports] of Object.entries(TypesenseProcessManager.nodeToPortMap)) {
      const dataDir = dataDirs[parseInt(nodeId)];
      if (dataDir === undefined) {
        return err(new Error(`Missing data directory for node ${nodeId}`));
      }
      nodes.push({ ...ports, dataDir });
    }

    this.nodes = nodes;
    return ok(nodes);
  }

  private writeToNodesFile() {
    this.spinner.start("Writing nodes file");
    const nodesFile = path.join(this.workingDirectory, "nodes");

    if (this.ipAddress === null) {
      return errAsync({ message: "IP address not found" });
    }

    const contents = `${this.ipAddress}:8107:8108,${this.ipAddress}:7107:7108,${this.ipAddress}:9107:9108`;

    logger.debug(`Writing nodes file to ${nodesFile} with contents:\n${contents}`);
    return ResultAsync.fromPromise(writeFile(nodesFile, contents, { encoding: "utf-8" }), toErrorWithMessage).map(
      () => {
        this.spinner.succeed("Nodes file written successfully");
        return nodesFile;
      },
    );
  }

  emptyDataDirectories(): ResultAsync<string[], ErrorWithMessage> {
    this.spinner.start("Emptying data directories");

    return ResultAsync.combine(this.nodes.map((node) => this.services.get("fs").emptyDirectory(node.dataDir))).map(
      () => {
        this.spinner.succeed("Data directories emptied");
        return this.nodes.map((node) => node.dataDir);
      },
    );
  }

  setup(): ResultAsync<void, ErrorWithMessage> {
    return this.setupProcesses().andThen(() => {
      this.spinner.start("Setting up OpenAI proxy");
      this.openAIProxy.start();
      this.spinner.succeed("OpenAI proxy set up");
      return okAsync(undefined);
    });
  }

  tearDown(): ResultAsync<void, ErrorWithMessage> {
    this.spinner.start("Tearing down OpenAI proxy");
    this.openAIProxy.stop();
    this.spinner.succeed("OpenAI proxy torn down");
    return okAsync(undefined);
  }

  private setupProcesses(): ResultAsync<TypesenseProcessController[], ErrorWithMessage> {
    this.spinner.start("Setting up Typesense processes");

    return this.writeToNodesFile()
      .andThen(() => this.createDataDirectories())
      .andThen((dataDirs) => this.mapNodesToDirectories(dataDirs))
      .andThen((nodes) => this.startAndVerifyProcesses(nodes));
  }

  private startAndVerifyProcesses(nodes: NodeConfig[]): ResultAsync<TypesenseProcessController[], ErrorWithMessage> {
    this.spinner.start("Starting node processes");

    return this.startNodeProcesses(nodes)
      .andThen((processes) => {
        this.spinner.succeed("All node processes started");
        this.spinner.start("Waiting for nodes to initialize");

        return ResultAsync.fromPromise(new Promise((resolve) => setTimeout(resolve, 8000)), toErrorWithMessage).map(
          () => processes,
        );
      })
      .andThen((processes) => {
        this.spinner.succeed();
        return this.verifyNodesHealth(processes);
      });
  }

  private indexConversationDocuments(process: TypesenseProcessController) {
    const documents = [
      {
        title: "The Age of the Essay",
        text: "Paul Graham",
      },
      {
        title: "Maker's Schedule, Manager's Schedule",
        text: "Paul Graham",
      },
    ];

    this.spinner.start("Indexing conversation documents");

    return this.typesenseProcessManager
      .indexDocuments(process, IntegrationTests.baseCollectionName, documents)
      .map(() => {
        this.spinner.succeed("Conversation documents indexed");
      });
  }

  conversationTest(): ResultAsync<void, ErrorWithMessage> {
    logger.warn("Running conversations on a cluster test");

    const process = this.typesenseProcessManager.processes.get(8108);
    if (!process) {
      return errAsync({ message: `Process not found for port 8108` });
    }

    return this.createBaseCollection(process)
      .andThen(() => this.createConversationStoreCollection(process))
      .andThen(() => this.createConversationModel(process))
      .andThen(() => this.indexConversationDocuments(process))
      .andThen(() => this.queryEachNode())
      .andThen(() => {
        logger.success("Conversation test passed successfully\n");
        return okAsync(undefined);
      });
  }

  conversationRotationTest(): ResultAsync<void, ErrorWithMessage> {
    logger.warn("Running conversation models on rotation test");

    const process = this.typesenseProcessManager.processes.get(8108);
    if (!process) {
      return errAsync({ message: `Process not found for port 8108` });
    }
    const processesArray = Array.from(this.typesenseProcessManager.processes.values());

    return this.createBaseCollection(process)
      .andThen(() => this.createConversationStoreCollection(process))
      .andThen(() => this.createConversationModel(process))
      .andThen(() => {
        const modelRetrievalResults = processesArray.map((process) =>
          this.typesenseProcessManager.getConversationModel(process, IntegrationTests.conversationModelName),
        );

        return ResultAsync.combine(modelRetrievalResults);
      })
      .map(() => {
        this.spinner.succeed("All nodes have the model");
      })
      .andThen(() => {
        this.spinner.start("Starting rotation");

        return processesArray.reduce(
          (promise: ResultAsync<void, ErrorWithMessage>, process: TypesenseProcessController) =>
            promise.andThen(() =>
              this.restartProcessWithVerification(process, processesArray).map(() => {
                this.spinner.succeed(`Rotation complete for port ${process.http}`);
              }),
            ),
          okAsync<void, ErrorWithMessage>(undefined),
        );
      })
      .map(() => {
        logger.success("Conversation rotation test passed successfully\n");
      });
  }

  private restartProcessWithVerification(
    process: TypesenseProcessController,
    allProcesses: TypesenseProcessController[],
  ): ResultAsync<void, ErrorWithMessage> {
    const spinner = ora();
    return this.typesenseProcessManager
      .restartProcess(process)
      .asyncAndThen(() => {
        spinner.start(`Waiting for process ${process.http} to restart`);
        return delay(25_000);
      })
      .andThen(() => {
        spinner.text = "Retrieving results";
        const modelRetrievalResults = allProcesses.map((p) =>
          this.typesenseProcessManager.getConversationModel(p, IntegrationTests.conversationModelName),
        );
        return ResultAsync.combine(modelRetrievalResults);
      })
      .andThen(() => {
        spinner.succeed("All three nodes have the model");
        return okAsync(undefined);
      });
  }

  private queryEachNode() {
    this.spinner.start("Querying each node");

    const params: SearchParams = {
      q: "Explain Maker's Schedule",
      conversation: true,
      query_by: "embedding",
      exclude_fields: "embedding",
      conversation_model_id: IntegrationTests.conversationModelName,
    };

    this.spinner.text = "Waiting for 15s before querying";

    return delay(15_000).andThen(() =>
      ResultAsync.combine(
        Array.from(this.typesenseProcessManager.processes.values()).map((process) => {
          return this.typesenseProcessManager.queryCollection(process, {
            collectionName: IntegrationTests.baseCollectionName,
            query: params,
          });
        }),
      ).map(() => {
        this.spinner.succeed("Every node queried successfully");
      }),
    );
  }

  private createConversationModel(process: TypesenseProcessController) {
    this.spinner.start("Creating conversation model");

    const model: ConversationModelSchema = {
      id: IntegrationTests.conversationModelName,
      system_prompt:
        "You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.",
      history_collection: "conversation_store",
      model_name: "openai/gpt-4-turbo",
      max_bytes: 16384,
      api_key: this.openAIKey,
    };

    return this.typesenseProcessManager.createConversationModel(process, model).map(() => {
      this.spinner.succeed("Conversation model created");
    });
  }

  private createBaseCollection(process: TypesenseProcessController) {
    const spinner = ora();
    spinner.start("Creating base collection");

    const collection: CollectionCreateSchema = {
      name: IntegrationTests.baseCollectionName,
      fields: [
        {
          name: "title",
          type: "string",
          facet: false,
        },
        {
          name: "text",
          type: "string",
          facet: false,
        },
        {
          name: "embedding",
          type: "float[]",
          embed: {
            from: ["title", "text"],
            model_config: {
              model_name: "ts/e5-small",
            },
          },
        },
      ],
    };

    return this.typesenseProcessManager.createCollection(process, collection).map((res) => {
      spinner.succeed("Base collection created");
      return res;
    });
  }

  private createConversationStoreCollection(process: TypesenseProcessController) {
    this.spinner.start("Creating conversation store collection");

    const collection: CollectionCreateSchema = {
      name: IntegrationTests.conversationStoreName,
      fields: [
        {
          name: "conversation_id",
          type: "string",
        },
        {
          name: "model_id",
          type: "string",
        },
        {
          name: "role",
          type: "string",
          index: false,
        },
        {
          name: "message",
          type: "string",
          index: false,
        },
        {
          name: "timestamp",
          type: "int32",
        },
      ],
    };

    return this.typesenseProcessManager.createCollection(process, collection).map((res) => {
      this.spinner.succeed("Conversation store collection created");
      return res;
    });
  }

  private verifyNodesHealth(
    processes: TypesenseProcessController[],
  ): ResultAsync<TypesenseProcessController[], ErrorWithMessage> {
    this.spinner.start("Verifying node health");

    return ResultAsync.combine(
      processes.map((process) => this.typesenseProcessManager.callOutToProcess(process).map(() => process)),
    );
  }

  private startNodeProcesses(nodes: NodeConfig[]): ResultAsync<TypesenseProcessController[], ErrorWithMessage> {
    const configs = nodes.map((node) => this.typesenseProcessManager.startProcess(node));
    return ResultAsync.combine(configs);
  }

  snapshotTest(): ResultAsync<void, ErrorWithMessage> {
    logger.warn("Running snapshot test");

    return this.saveSnapshot()
      .andThen(() => this.verifySnapshot())
      .map(() => {
        logger.success("Snapshot test passed successfully\n");
      });
  }

  private verifySnapshot(): ResultAsync<void, ErrorWithMessage> {
    this.spinner.start("Verifying snapshot");

    return this.services
      .get("fs")
      .exists(path.join(this.typesenseProcessManager.getSnapshotPath, "state"))
      .andThen((exists) => {
        if (!exists) {
          return errAsync({ message: "Snapshot file does not exist" });
        }

        this.spinner.succeed("Snapshot verified");
        return okAsync(undefined);
      });
  }

  private saveSnapshot() {
    this.spinner.start("Saving snapshot");

    const process = this.typesenseProcessManager.processes.get(8108);

    if (!process) {
      return errAsync({ message: "Process not found for port 8108" });
    }

    return this.typesenseProcessManager.snapshot(process);
  }

  restartAllNodes() {
    this.spinner.start("Restarting all nodes");
    // Cleanup any existing processes to restart them
    const cleanupResults = Array.from(this.typesenseProcessManager.processes.values()).map((process) =>
      process.dispose().asyncAndThen(() => {
        this.typesenseProcessManager.processes.delete(process.http);
        return okAsync<void, ErrorWithMessage>(undefined);
      }),
    );

    return ResultAsync.combine(cleanupResults)
      .andThen(() => {
        this.spinner.text = "Cleaning up before restarting processes";
        return delay(10_000).map(() => {
          this.spinner.succeed("Cleanup complete");
        });
      })
      .andThen(() => this.startAndVerifyProcesses(Array.from(this.nodes.values())));
  }

  openAIEmbeddingTest(): ResultAsync<void, ErrorWithMessage> {
    logger.warn("Running OpenAI Embedding test");

    return this.createOpenAIEmbeddingCollection()
      .andThen(() => ResultAsync.fromPromise(new Promise((resolve) => setTimeout(resolve, 2000)), toErrorWithMessage))
      .andThen(() => this.validateOpenAIEmbeddingCollection())
      .andThen(() => this.restartAllNodes())
      .andThen(() => {
        this.spinner.start("Waiting for 5s before verifying number of dimensions");
        return delay(5_000);
      })
      .andThen(() => this.validateOpenAIEmbeddingCollection())
      .map(() => {
        logger.success("OpenAI Embedding test passed successfully\n");
      });
  }

  private createOpenAIEmbeddingCollection() {
    this.spinner.start("Creating OpenAI Embedding collection with custom number of  dimensions");

    const collection: CollectionCreateSchema = {
      name: IntegrationTests.openAICollectionName,
      fields: [
        {
          name: "product_name",
          type: "string",
          facet: false,
        },
        {
          name: "embedding",
          type: "float[]",
          num_dim: this.numDim,
          embed: {
            from: ["product_name"],
            model_config: {
              model_name: "openai/text-embedding-3-large",
              api_key: this.openAIKey,
            },
          },
        },
      ],
    };

    const process = this.typesenseProcessManager.processes.get(8108);

    if (!process) {
      return errAsync({ message: "Process not found for port 8108" });
    }

    return this.typesenseProcessManager.createCollection(process, collection).map(() => {
      this.spinner.succeed("OpenAI Embedding collection created");
    });
  }

  private validateOpenAIEmbeddingCollection() {
    this.spinner.start("Validating OpenAI Embedding collection");

    const process = this.typesenseProcessManager.processes.get(8108);

    if (!process) {
      return errAsync({ message: "Process not found for port 8108" });
    }

    return this.typesenseProcessManager
      .getCollection(process, IntegrationTests.openAICollectionName)
      .map((collection) => {
        if (collection.fields?.find((field) => field.name === "embedding")?.num_dim !== this.numDim) {
          return err({ message: `Number of dimensions is not ${this.numDim}` });
        }

        this.spinner.succeed("OpenAI Embedding collection validated");
      });
  }
}

const test = new Command()
  .name("test")
  .description("Run the integration tests")
  .option("-n, --container-name <name>", "Name for the Docker container. Defaults to bazel-build", "bazel-build")
  .option("-i, --image-name <image>", "Name for the Docker image. Defaults to ubuntu-build", "ubuntu-build")
  .option(
    "-g, --typesense-git-url <url>",
    "Git URL for the Typesense repo. Defaults to the main Typesense github repo",
    DEFAULT_TYPESENSE_GIT_URL,
  )
  .option(
    "-d, --working-directory <dir>",
    "Directory where the Typesense repo is saved. Defaults to the current directory",
    cwd,
  )
  .option("-c, --commitHash <commit-hash>", "Hash of the commit to install. Defaults to the latest commit")
  .option("-v, --verbose", "Verbose output", false)
  .option("-y, --yes", "Verbose output", false)
  .option("-b, --binary <path>", "Path of prebuilt-binary. Use this to skip the building process.")
  .option("--api-key <key>", "API key to use for the Typesense Process.", "xyz")
  .option("--num-dim <number>", "Number of dimensions to test against OpenAI embeddings", "256")
  .option("--openAI-key <key>", "OpenAI API key. Defaults to OPENAI_API_KEY in PATH", "oh_no_i_leaked_the_key")
  .option("--ip <ip>", "IP address to use for the Typesense Process.")
  .option("-s, --snapshot-path <path>", "Path to the snapshot file.", cwd)
  .action((options) => {
    logger.info("Running Typesense Integration tests");
    const services = new ServiceContainer(findRoot(dirName));
    const spinner = services.getSpinner();
    parseOptions(options as IntegrationTestOptions, integrationTestOptionsSchema, spinner)
      .andThen((options) => {
        if (options.verbose) {
          logger.setLevel(LogLevel.DEBUG);
        }
        logger.debug("Parsed options");
        return ok(options);
      })
      .andThen((options) => {
        services.initialize({
          directory: options.workingDirectory,
          containerName: options.containerName,
          gitUrl: options.typesenseGitUrl,
          yesToAll: options.yes,
        });
        return services
          .get("fs")
          .validateWorkingDirectory(options.workingDirectory)
          .map(() => options);
      })
      .andThen((options) => {
        if (options.binary) {
          return ok({ ...options, binary: options.binary });
        }
        return services
          .get("typesense")
          .validateDirectory()
          .andThen(() => services.get("docker").validateImage(options.imageName))
          .andThen(() =>
            services.get("docker").validateContainer({
              containerName: options.containerName,
              imageName: options.imageName,
            }),
          )
          .andThen(() => services.get("docker").startContainer(options.containerName))
          .andThen(() => services.get("containerGit").markDirectoryAsSafe(options.workingDirectory))
          .andThen(() =>
            services
              .get("containerGit")
              .checkoutToCommit(options.commitHash)
              .map((commitHash) => ({ ...options, commitHash })),
          )
          .andThen((options) =>
            services.get("typesense").buildAndSave({
              commitHash: options.commitHash,
              containerName: options.containerName,
              destination: options.workingDirectory,
            }),
          )
          .andThen((binaryPath) =>
            services
              .get("docker")
              .stopContainer(options.containerName)
              .map(() => binaryPath),
          )
          .map((binaryPath) => ({ ...options, binary: binaryPath }));
      })
      .andThen((options) => {
        logger.debug("Starting Typesense process");
        const typesenseProcessManager = new TypesenseProcessManager(
          spinner,
          options.binary,
          options.apiKey,
          options.workingDirectory,
          services.get("fs"),
          options.snapshotPath,
          options.ip,
        );
        const integrationTests = new IntegrationTests(
          services,
          spinner,
          typesenseProcessManager,
          options.workingDirectory,
          options.openAIKey,
          options.numDim,
          options.ip,
        );

        return ok(integrationTests);
      })
      .andThen((integrationTests) =>
        integrationTests
          .setup()
          .andThen(() => integrationTests.openAIEmbeddingTest())
          .andThen(() => integrationTests.emptyDataDirectories())
          .andThen(() => integrationTests.restartAllNodes())
          .andThen(() => integrationTests.conversationRotationTest())
          .andThen(() => integrationTests.emptyDataDirectories())
          .andThen(() => integrationTests.restartAllNodes())
          .andThen(() => integrationTests.conversationTest())
          .andThen(() => integrationTests.emptyDataDirectories())
          .andThen(() => integrationTests.restartAllNodes())
          .andThen(() => integrationTests.snapshotTest())
          .andThen(() => integrationTests.emptyDataDirectories())
          .andThen(() => integrationTests.tearDown()),
      )
      .then((result) => {
        if (result.isErr()) {
          spinner.fail();
          logger.error(result.error.message);
          process.exit(1);
        }
        logger.success("Integration tests passed successfully\n");
        process.exit(0);
      });
  });

export { test };
