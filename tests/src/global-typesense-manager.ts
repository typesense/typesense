import type { ErrorWithMessage } from "@/error";
import type { NodeConfig, SetupNodesOptions } from "@/typesense-process";

import { toErrorWithMessage } from "@/error";

import "dotenv/config";

import { createEnv } from "@t3-oss/env-core";
import { errAsync, okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { z } from "zod";

import { OpenAIProxy } from "@/openai";
import { TypesenseProcessManager } from "@/typesense-process";
import { constructUrl, delay } from "@/utils";

const env = createEnv({
  clientPrefix: "TYPESENSE_",
  client: {
    TYPESENSE_BINARY: z.string(),
    TYPESENSE_WORKING_DIRECTORY: z.string(),
    TYPESENSE_SNAPSHOT_PATH: z.string().optional(),
    TYPESENSE_IP_ADDRESS: z.string().optional(),
    TYPESENSE_NUM_DIM: z.number().default(256),
  },
  runtimeEnv: process.env,
  skipValidation: process.env.NODE_ENV === "test",
  emptyStringAsUndefined: true,
});

const snapshotPath = env.TYPESENSE_SNAPSHOT_PATH ?? `${env.TYPESENSE_WORKING_DIRECTORY}/snapshot`;

const spinner = ora();

const globalTypesenseManager = new TypesenseProcessManager(
  spinner,
  env.TYPESENSE_BINARY,
  "xyz",
  env.TYPESENSE_WORKING_DIRECTORY,
  snapshotPath,
  env.TYPESENSE_IP_ADDRESS,
);

const openAIProxy = new OpenAIProxy(spinner, env.TYPESENSE_WORKING_DIRECTORY, env.TYPESENSE_NUM_DIM);

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
function restartTypesenseServer(): ResultAsync<NodeConfig[], ErrorWithMessage> {
  const cleanupResults = Array.from(globalTypesenseManager.processes.values()).map((process) =>
    process.dispose().andThen(() => {
      globalTypesenseManager.processes.delete(process.http);
      return okAsync<void, ErrorWithMessage>(undefined);
    }),
  );

  return ResultAsync.combine(cleanupResults).andThen(() => startTypesenseServer({ skipCleanup: true }));
}

function closeDownTypesenseServer(): ResultAsync<void[], ErrorWithMessage> {
  return ResultAsync.combine(
    Array.from(globalTypesenseManager.processes.values()).map((process) =>
      process.dispose().andThen(() => {
        globalTypesenseManager.processes.delete(process.http);
        return okAsync<void, ErrorWithMessage>(undefined);
      }),
    ),
  );
}

/**
 * Restart the Typesense server, delete all data and wait for it to be ready.
 * @param {Object} options
 * @param {number} options.waitForSeconds - The number of seconds to wait for the server to be ready.
 */
function restartTypesenseServerFresh(): ResultAsync<NodeConfig[], ErrorWithMessage> {
  const cleanupResults = Array.from(globalTypesenseManager.processes.values()).map((process) =>
    process.dispose().andThen(() => {
      globalTypesenseManager.processes.delete(process.http);
      return okAsync<void, ErrorWithMessage>(undefined);
    }),
  );

  return ResultAsync.combine(cleanupResults).andThen(() => startTypesenseServer());
}

interface FetchNodeParams<Body, QueryParams> {
  port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"];
  endpoint: `${string}`;
  method: "GET" | "POST" | "PUT" | "DELETE";
  body?: Body;
  queryParams?: QueryParams;
  numRetries?: number;
  retryDelayMs?: number;
}

function fetchNode<T, const Body = string, const QueryParams = Record<string, string>>(
  params: FetchNodeParams<Body, QueryParams>,
  _retriesLeft?: number,
): ResultAsync<T, ErrorWithMessage> {
  const { port, endpoint, method, body, queryParams, numRetries = 3, retryDelayMs = 5_000 } = params;

  const currentRetriesLeft = _retriesLeft ?? numRetries;

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
  )
    .andThen((res) => {
      if (!res.ok) {
        return ResultAsync.fromPromise(res.text(), toErrorWithMessage)
          .andThen((errorText) => {
            if (currentRetriesLeft > 0) {
              return ResultAsync.fromPromise(delay(retryDelayMs), toErrorWithMessage).andThen(() =>
                fetchNode<T, Body, QueryParams>(params, currentRetriesLeft - 1),
              );
            }
            return errAsync({
              message: `HTTP error! status: ${res.status}, message: ${errorText} (no retries left)`,
            });
          })
          .orElse((getTextError) => {
            if (currentRetriesLeft > 0) {
              return ResultAsync.fromPromise(delay(retryDelayMs), toErrorWithMessage).andThen(() =>
                fetchNode<T, Body, QueryParams>(params, currentRetriesLeft - 1),
              );
            }
            return errAsync({
              message: `HTTP error! status: ${res.status}, failed to get error body: ${getTextError.message} (no retries left)`,
            });
          });
      }
      return ResultAsync.fromPromise(res.json() as Promise<T>, toErrorWithMessage);
    })
    .orElse((networkOrFetchError) => {
      if (currentRetriesLeft > 0) {
        return ResultAsync.fromPromise(delay(retryDelayMs), toErrorWithMessage).andThen(() =>
          fetchNode<T, Body, QueryParams>(params, currentRetriesLeft - 1),
        );
      }
      return errAsync({
        message: `Request failed: ${networkOrFetchError.message} (no retries left)`,
      });
    });
}

export {
  globalTypesenseManager,
  startTypesenseServer,
  restartTypesenseServerFresh,
  restartTypesenseServer,
  env,
  fetchNode,
  openAIProxy,
  closeDownTypesenseServer,
};
