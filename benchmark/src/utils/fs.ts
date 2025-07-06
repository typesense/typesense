import { access, mkdir, rm } from "fs/promises";
import type { ErrorWithMessage } from "@/utils/error";
import type { MakeDirectoryOptions, RmOptions } from "fs";

import { emptyDir } from "fs-extra";
import { err, ok, ResultAsync } from "neverthrow";

import { toErrorWithMessage } from "@/utils/error";

function exists(directory: string, mode?: number): ResultAsync<boolean, ErrorWithMessage> {
  return ResultAsync.fromPromise(
    access(directory, mode)
      .then(() => true)
      .catch(() => false),
    toErrorWithMessage,
  );
}

type PlatformFlag = "linux/amd64" | "linux/arm64" | "darwin/amd64" | "windows/amd64";

export function getPlatform(): PlatformFlag {
  const plat = process.arch;

  switch (plat) {
    case "x64":
      return "linux/amd64";
    case "arm":
    case "arm64":
      return "linux/arm64";
    default:
      return "linux/amd64";
  }
}

function safeRm({
  directory,
  options,
}: {
  directory: string;
  options: RmOptions;
}): ResultAsync<void, ErrorWithMessage> {
  return ResultAsync.fromPromise(rm(directory, options), toErrorWithMessage);
}

function safeMakeOrEmptyDir({
  directory,
  options,
}: {
  directory: string;
  options: MakeDirectoryOptions;
}): ResultAsync<string, ErrorWithMessage> {
  return exists(directory).andThen((exists) => {
    if (exists) {
      return safeEmptyDir(directory);
    }

    return ResultAsync.fromPromise(mkdir(directory, options), toErrorWithMessage).andThen((value) => {
      if (value === undefined) {
        return err({
          message: `Failed to create directory ${directory}`,
        });
      }
      return ok(value);
    });
  });
}

function safeEmptyDir(directory: string): ResultAsync<string, ErrorWithMessage> {
  return ResultAsync.fromPromise(emptyDir(directory), toErrorWithMessage).map(() => directory);
}

export { exists, safeEmptyDir, safeMakeOrEmptyDir, safeRm };
