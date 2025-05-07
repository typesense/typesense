import type { ErrorWithMessage } from "@/error";
import type { Result as ExecaResult, Options } from "execa";

import { execa } from "execa";
import { ResultAsync } from "neverthrow";

import { toErrorWithMessage } from "@/error";

function isStringifiable(data: unknown): data is { toString(): string } {
  return (
    data !== null &&
    (typeof data === "string" ||
      typeof data === "number" ||
      typeof data === "boolean" ||
      (typeof data === "object" && typeof data.toString === "function"))
  );
}

function delay(ms: number): ResultAsync<void, ErrorWithMessage> {
  return ResultAsync.fromPromise(new Promise((resolve) => setTimeout(resolve, ms)), toErrorWithMessage);
}

function safeExeca(
  command: string,
  args: string[],
  opts?: Options,
): ResultAsync<ExecaResult<Options>, ErrorWithMessage> {
  return ResultAsync.fromPromise(execa(command, args, opts), toErrorWithMessage);
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

export { constructUrl, delay, isStringifiable, safeExeca };
