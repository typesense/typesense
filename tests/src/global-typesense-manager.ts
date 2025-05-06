import type { ErrorWithMessage } from "@/error";
import type { NodeConfig } from "@/typesense-process";

import { toErrorWithMessage } from "@/error";

import "dotenv/config";

import { createEnv } from "@t3-oss/env-core";
import { okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { z } from "zod";

import { TypesenseProcessManager } from "@/typesense-process";
import { delay } from "@/utils";

const env = createEnv({
  clientPrefix: "TYPESENSE_",
  client: {
    TYPESENSE_BINARY: z.string(),
    TYPESENSE_WORKING_DIRECTORY: z.string(),
    TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
  },
  runtimeEnv: process.env,
});

const globalTypesenseManager = new TypesenseProcessManager(
  ora(),
  env.TYPESENSE_BINARY,
  "xyz",
  env.TYPESENSE_WORKING_DIRECTORY,
  env.TYPESENSE_SNAPSHOT_PATH,
);

function startTypesenseServer(): ResultAsync<NodeConfig[], ErrorWithMessage> {
  return globalTypesenseManager
    .setupNodes()
    .andThen((nodes) => {
      return ResultAsync.combine(nodes.map((node) => globalTypesenseManager.startProcess(node))).map(() => nodes);
    })
    .andThen((nodes) => {
      ora().start("Waiting for 8 seconds");
      return delay(8_000).map(() => nodes);
    })
    .andThen((nodes) => {
      return ResultAsync.combine(nodes.map((node) => globalTypesenseManager.getHealth(node.http))).map(() => nodes);
    });
}

function restartTypesenseServerFresh(): ResultAsync<NodeConfig[], ErrorWithMessage> {
  const cleanupResults = Array.from(globalTypesenseManager.processes.values()).map((process) =>
    process.dispose().asyncAndThen(() => {
      globalTypesenseManager.processes.delete(process.http);
      return okAsync<void, ErrorWithMessage>(undefined);
    }),
  );

  return ResultAsync.combine(cleanupResults)
    .andThen(() => {
      ora().start("Waiting for 10 seconds for cleanup");

      return delay(10_000).map(() => {
        ora().succeed("Cleanup complete");
      });
    })
    .andThen(() => startTypesenseServer());
}

function fetchNode<T, const B>({
  port,
  endpoint,
  method,
  body,
}: {
  port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"];
  endpoint: string;
  method: "GET" | "POST" | "PUT" | "DELETE";
  body?: B;
}): ResultAsync<T, ErrorWithMessage> {
  return ResultAsync.fromPromise(
    fetch(`http://localhost:${port}/${endpoint}`, {
      method,
      headers: {
        "Content-Type": "application/json",
        "X-Typesense-Api-Key": "xyz",
      },
      body: body ? JSON.stringify(body) : undefined,
    }).then((res) => res.json() as T),
    toErrorWithMessage,
  );
}

export { globalTypesenseManager, startTypesenseServer, restartTypesenseServerFresh, env, fetchNode };
