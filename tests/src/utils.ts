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

export { delay, isStringifiable, safeExeca };
