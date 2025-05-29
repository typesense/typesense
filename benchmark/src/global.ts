import type { NodeConfig, SetupNodesOptions } from "@/services/typesense-process";
import type { ErrorWithMessage } from "@/utils/error";

import { toErrorWithMessage } from "@/utils/error";

import "dotenv/config";

import type {
  CollectionDeleteOptions,
  CollectionSchema,
  CollectionUpdateSchema,
} from "typesense/lib/Typesense/Collection";
import type {
  CollectionCreateOptions,
  CollectionCreateSchema,
  CollectionsRetrieveOptions,
} from "typesense/lib/Typesense/Collections";
import type { ConversationModelCreateSchema, ConversationModelSchema } from "typesense/lib/Typesense/ConversationModel";
import type { DebugResponseSchema } from "typesense/lib/Typesense/Debug";
import type {
  DeleteQuery,
  DeleteResponse,
  DocumentSchema,
  DocumentsExportParameters,
  DocumentWriteParameters,
  SearchParams,
  SearchResponse,
} from "typesense/lib/Typesense/Documents";
import type { MetricsResponse } from "typesense/lib/Typesense/Metrics";
import type { OverrideSchema } from "typesense/lib/Typesense/Override";
import type { OverrideCreateSchema, OverridesRetrieveSchema } from "typesense/lib/Typesense/Overrides";
import type { StatsResponse } from "typesense/lib/Typesense/Stats";

import { createEnv } from "@t3-oss/env-core";
import { errAsync, okAsync, ResultAsync } from "neverthrow";
import ora from "ora";
import { z } from "zod";

import { OpenAIProxy } from "@/services/openai-mock";
import { TypesenseProcessManager } from "@/services/typesense-process";
import { constructUrl, delay } from "@/utils/base";

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

interface Endpoints<
  TDoc extends DocumentSchema,
  ColCreateOptions extends CollectionCreateOptions = CollectionCreateOptions,
> {
  collections: {
    GET: {
      return: CollectionSchema[];
      body: never;
      queryParams: CollectionsRetrieveOptions;
    };
    POST: {
      return: CollectionSchema;
      body: CollectionCreateSchema<ColCreateOptions>;
      queryParams: ColCreateOptions;
    };
    PATCH: {
      return: CollectionSchema;
      body: CollectionUpdateSchema;
      queryParams: never;
    };
  };
  "collections/:id/documents": {
    POST: {
      return: TDoc;
      body: TDoc;
      queryParams: DocumentWriteParameters;
    };
    PATCH: {
      return: TDoc;
      body: TDoc;
      queryParams: DocumentWriteParameters;
    };
    DELETE: {
      return: DeleteResponse;
      body: never;
      queryParams: DeleteQuery;
    };
  };
  "collections/:id/documents/search": {
    GET: {
      return: SearchResponse<TDoc>;
      body: never;
      queryParams: SearchParams<TDoc, string>;
    };
  };
  "collections/:id/documents/export": {
    GET: {
      return: string;
      body: never;
      queryParams: DocumentsExportParameters;
    };
  };
  "collections/:id/overrides": {
    GET: {
      return: OverridesRetrieveSchema;
      body: never;
      queryParams: never;
    };
  };
  "collections/:id/overrides/:id": {
    PUT: {
      return: OverrideSchema;
      body: OverrideCreateSchema;
      queryParams: never;
    };
    DELETE: {
      return: OverrideSchema;
      body: never;
      queryParams: never;
    };
    GET: {
      return: OverrideSchema;
      body: never;
      queryParams: never;
    };
  };
  "collections/:id": {
    GET: {
      return: CollectionSchema;
      body: never;
      queryParams: never;
    };
    DELETE: {
      return: CollectionSchema;
      body: never;
      queryParams: CollectionDeleteOptions;
    };
  };
  "conversations/models": {
    GET: {
      return: ConversationModelSchema[];
      body: never;
      queryParams: never;
    };
    POST: {
      return: ConversationModelSchema;
      body: ConversationModelCreateSchema;
      queryParams: never;
    };
  };
  "conversations/models/:id": {
    GET: {
      return: ConversationModelSchema;
      body: never;
      queryParams: never;
    };
    PUT: {
      return: ConversationModelSchema;
      body: ConversationModelCreateSchema;
      queryParams: never;
    };
    DELETE: {
      return: ConversationModelSchema;
      body: never;
      queryParams: never;
    };
  };
  "operations/cache_clear": {
    POST: {
      return: unknown;
      body: never;
      queryParams: never;
    };
  };
  "operations/vote": {
    POST: {
      return: unknown;
      body: never;
      queryParams: never;
    };
  };
  "operations/snapshot": {
    POST: {
      return: unknown;
      body: never;
      queryParams: never;
    };
  };
  "operations/schema_changes": {
    POST: {
      return: unknown;
      body: never;
      queryParams: never;
    };
  };
  debug: {
    GET: {
      return: DebugResponseSchema;
      body: never;
      queryParams: never;
    };
  };
  "metrics.json": {
    GET: {
      return: MetricsResponse;
      body: never;
      queryParams: never;
    };
  };
  "stats.json": {
    GET: {
      return: StatsResponse;
      body: never;
      queryParams: never;
    };
  };
}

type PathSegment = Exclude<string, `${string}/${string}`>;

type TransformPath<T extends string> =
  T extends `${infer Prefix}/:id${infer Rest}` ?
    Rest extends `/${infer Next}` ?
      `${Prefix}/${PathSegment}${TransformPath<Next>}`
    : `${Prefix}/${PathSegment}`
  : T;

type TransformIdToTemplate<T> = {
  [K in keyof T as K extends string ? TransformPath<K> : K]: T[K];
};

type TransformedEndpoints<T extends DocumentSchema> = TransformIdToTemplate<Endpoints<T>>;

type GetParentPaths<T extends string> =
  T extends `${infer Start}/${infer Middle}/${infer _Rest}` ? `${Start}/${Middle}`
  : T extends `${infer Start}/${infer _Rest}` ? Start
  : never;

type GetRawEndpointResponse<
  TPath extends keyof TransformedEndpoints<TDoc>,
  TMethod extends keyof TransformedEndpoints<TDoc>[TPath],
  TDoc extends DocumentSchema,
> = TransformedEndpoints<TDoc>[TPath][TMethod];

type GetParentPathResponses<TPath extends string, TMethod extends string, TDoc extends DocumentSchema> =
  TPath extends keyof TransformedEndpoints<TDoc> ?
    GetParentPaths<TPath> extends infer Parents ?
      Parents extends keyof TransformedEndpoints<TDoc> ?
        TMethod extends keyof TransformedEndpoints<TDoc>[Parents] ?
          TransformedEndpoints<TDoc>[Parents][TMethod]
        : never
      : never
    : never
  : never;

type RemoveFromIntersection<T, U> =
  T extends U & infer Rest ? Rest
  : T extends U ? never
  : T;

type GetEndpointResponse<
  TPath extends keyof TransformedEndpoints<TDoc>,
  TMethod extends string & keyof TransformedEndpoints<TDoc>[TPath],
  TDoc extends DocumentSchema,
> = RemoveFromIntersection<GetRawEndpointResponse<TPath, TMethod, TDoc>, GetParentPathResponses<TPath, TMethod, TDoc>>;

interface FetchNodeParams<
  TPath extends keyof TransformedEndpoints<TDoc>,
  TMethod extends string & keyof TransformedEndpoints<TDoc>[TPath],
  TDoc extends DocumentSchema,
> {
  port: (typeof TypesenseProcessManager.nodeToPortMap)[number]["http"];
  endpoint: TPath;
  method: TMethod;
  body?: GetEndpointResponse<TPath, TMethod, TDoc> extends { body: unknown } ?
    GetEndpointResponse<TPath, TMethod, TDoc>["body"]
  : never;
  queryParams?: GetEndpointResponse<TPath, TMethod, TDoc> extends { queryParams: unknown } ?
    GetEndpointResponse<TPath, TMethod, TDoc>["queryParams"]
  : never;
  numRetries?: number;
  retryDelayMs?: number;
  "~type"?: TDoc;
}

/**
 * Fetch data from a Typesense node, with retries.
 */
function fetchNode<
  TDoc extends DocumentSchema,
  const TPath extends keyof TransformedEndpoints<TDoc>,
  const TMethod extends string & keyof TransformedEndpoints<TDoc>[TPath],
>(
  params: FetchNodeParams<TPath, TMethod, TDoc>,
  _retriesLeft?: number,
): ResultAsync<GetEndpointResponse<TPath, TMethod, TDoc> extends { return: infer R } ? R : never, ErrorWithMessage> {
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
                fetchNode<TDoc, TPath, TMethod>(params, currentRetriesLeft - 1),
              );
            }
            return errAsync({
              message: `HTTP error! status: ${res.status}, message: ${errorText} (no retries left)`,
            });
          })
          .orElse((getTextError) => {
            if (currentRetriesLeft > 0) {
              return ResultAsync.fromPromise(delay(retryDelayMs), toErrorWithMessage).andThen(() =>
                fetchNode<TDoc, TPath, TMethod>(params, currentRetriesLeft - 1),
              );
            }
            return errAsync({
              message: `HTTP error! status: ${res.status}, failed to get error body: ${getTextError.message} (no retries left)`,
            });
          });
      }
      return ResultAsync.fromPromise(
        res.json() as Promise<GetEndpointResponse<TPath, TMethod, TDoc> extends { return: infer R } ? R : never>,
        toErrorWithMessage,
      );
    })
    .orElse((networkOrFetchError) => {
      if (currentRetriesLeft > 0) {
        return ResultAsync.fromPromise(delay(retryDelayMs), toErrorWithMessage).andThen(() =>
          fetchNode<TDoc, TPath, TMethod>(params, currentRetriesLeft - 1),
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
