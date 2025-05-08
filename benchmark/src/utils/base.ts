import type { ErrorWithMessage } from "@/utils/error";

import { ResultAsync } from "neverthrow";

import { toErrorWithMessage } from "@/utils/error";

function delay(ms: number): ResultAsync<void, ErrorWithMessage> {
  return ResultAsync.fromPromise(new Promise((resolve) => setTimeout(resolve, ms)), toErrorWithMessage);
}

type Protocol = "http" | "https";
type Domain = `${string}.${string}` | `localhost`;
type Port = `:${number}`;
type Path = `/${string}${string}`;

type UrlString = `${Protocol}://${Domain}${Port | ""}${Path | ""}`;

function constructUrl({
  baseUrl,
  params,
  endpoint,
}: {
  baseUrl: UrlString;
  params?: URLSearchParams;
  endpoint?: `/${string}`;
}) {
  const urlWithEndpoint = endpoint ? `${baseUrl}${endpoint}` : baseUrl;
  return params && Array.from(params.entries()).length > 0 ? `${urlWithEndpoint}?${params}` : urlWithEndpoint;
}

export { constructUrl, delay };
