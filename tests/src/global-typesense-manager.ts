import type { ErrorWithMessage } from "@/error";
import type { NodeConfig, SetupNodesOptions } from "@/typesense-process";

import { toErrorWithMessage } from "@/error";

import "dotenv/config";

import { createEnv } from "@t3-oss/env-core";
import { errAsync, okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { z } from "zod";

import { TypesenseProcessManager } from "@/typesense-process";
import { constructUrl, delay } from "@/utils";

const env = createEnv({
  clientPrefix: "TYPESENSE_",
  client: {
    TYPESENSE_BINARY: z.string(),
    TYPESENSE_WORKING_DIRECTORY: z.string(),
    TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
    TYPESENSE_IP_ADDRESS: z.string().optional(),
  },
  runtimeEnv: process.env,
});

const globalTypesenseManager = new TypesenseProcessManager(
  ora(),
  env.TYPESENSE_BINARY,
  "xyz",
  env.TYPESENSE_WORKING_DIRECTORY,
  env.TYPESENSE_SNAPSHOT_PATH,
  env.TYPESENSE_IP_ADDRESS,
);

/**
 * Start the Typesense server and wait for it to be ready.
 * @param {Object} options
 * @param {boolean} options.skipCleanup - Whether to skip the cleanup of the data directories.
 * @example
 */
function startTypesenseServer(options?: SetupNodesOptions): ResultAsync<NodeConfig[], ErrorWithMessage> {
  return globalTypesenseManager
    .setupNodes(options)
    .andThen((nodes) => {
      return ResultAsync.combine(nodes.map((node) => globalTypesenseManager.startProcess(node))).map(() => nodes);
    })
    .andThen((nodes) => {
      return ResultAsync.combine(nodes.map((node) => globalTypesenseManager.getHealth(node.http))).map(() => nodes);
    });
}

/**
 * Restart the Typesense server and wait for it to be ready.
 * @param {Object} options
 * @param {number} options.waitForSeconds - The number of seconds to wait for the server to be ready.
 * @example
 */
function restartTypesenseServer({ waitForSeconds = 10 }: { waitForSeconds?: number } = {}): ResultAsync<
  NodeConfig[],
  ErrorWithMessage
> {
  const cleanupResults = Array.from(globalTypesenseManager.processes.values()).map((process) =>
    process.dispose().asyncAndThen(() => {
      globalTypesenseManager.processes.delete(process.http);
      return okAsync<void, ErrorWithMessage>(undefined);
    }),
  );

  return ResultAsync.combine(cleanupResults)
    .andThen(() => {
      ora().start(`Waiting for ${waitForSeconds} seconds for cleanup`);

      return delay(waitForSeconds * 1_000).map(() => {
        ora().succeed("Cleanup complete");
      });
    })
    .andThen(() => startTypesenseServer({ skipCleanup: true }));
}

/**
 * Restart the Typesense server, delete all data and wait for it to be ready.
 * @param {Object} options
 * @param {number} options.waitForSeconds - The number of seconds to wait for the server to be ready.
 */
function restartTypesenseServerFresh({ waitForSeconds = 10 }: { waitForSeconds?: number } = {}): ResultAsync<
  NodeConfig[],
  ErrorWithMessage
> {
  const cleanupResults = Array.from(globalTypesenseManager.processes.values()).map((process) =>
    process.dispose().asyncAndThen(() => {
      globalTypesenseManager.processes.delete(process.http);
      return okAsync<void, ErrorWithMessage>(undefined);
    }),
  );

  return ResultAsync.combine(cleanupResults)
    .andThen(() => {
      ora().start(`Waiting for ${waitForSeconds} seconds for cleanup`);

      return delay(waitForSeconds * 1_000).map(() => {
        ora().succeed("Cleanup complete");
      });
    })
    .andThen(() => startTypesenseServer());
}

/**
 * Perform a request to a node.
 */
function fetchNode<T, const Body, const QueryParams = Record<string, string>>({
  port,
  endpoint,
  method,
  body,
  queryParams,
}: {
  port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"];
  endpoint: `${string}`;
  method: "GET" | "POST" | "PUT" | "DELETE";
  body?: Body;
  queryParams?: QueryParams;
}): ResultAsync<T, ErrorWithMessage> {
  const urlParams = queryParams ? new URLSearchParams(queryParams) : undefined;

  const url = constructUrl({
    baseUrl: `http://localhost:${port}`,
    endpoint: `/${endpoint}`,
    params: urlParams,
  });

  return ResultAsync.fromPromise(
    fetch(url, {
      method,
      headers: {
        "Content-Type": "application/json",
        "X-Typesense-Api-Key": "xyz",
      },
      body: body ? JSON.stringify(body) : undefined,
    }),
    toErrorWithMessage,
  ).andThen((res) => {
    if (!res.ok) {
      return ResultAsync.fromPromise(res.text(), toErrorWithMessage).andThen((text) => {
        return errAsync({
          message: `HTTP error! status: ${res.status}, message: ${text}`,
        });
      });
    }

    return ResultAsync.fromPromise(res.json() as Promise<T>, toErrorWithMessage);
  });
}

export {
  globalTypesenseManager,
  startTypesenseServer,
  restartTypesenseServerFresh,
  restartTypesenseServer,
  env,
  fetchNode,
};
