import { access, mkdir, rm } from "fs/promises";
import path from "path";
import type { ErrorWithMessage } from "@/utils/error";
import type { Ora } from "ora";

import inquirer from "inquirer";
import { errAsync, okAsync, ResultAsync } from "neverthrow";

import { toErrorWithMessage } from "@/utils/error";

type PlatformFlag =
  | "linux/amd64"
  | "linux/arm64"
  | "darwin/amd64"
  | "windows/amd64";

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

export class FilesystemService {
  constructor(private spinner: Ora) {}

  exists(
    directory: string,
    mode?: number,
  ): ResultAsync<boolean, ErrorWithMessage> {
    this.spinner.start("Checking directory existence");

    return ResultAsync.fromPromise(
      access(directory, mode)
        .then(() => true)
        .catch(() => false),
      toErrorWithMessage,
    ).map((exists) => {
      return exists;
    });
  }

  removeDirectory(directory: string): ResultAsync<void, ErrorWithMessage> {
    this.spinner.start(`Removing directory ${directory}`);
    return ResultAsync.fromPromise(
      rm(directory, { recursive: true }),
      toErrorWithMessage,
    ).map(() => {
      this.spinner.succeed(`Removed directory ${directory}`);
    });
  }

  getAbsolutePath(directory: string): string {
    return path.resolve(directory);
  }

  validateWorkingDirectory(
    directory: string,
  ): ResultAsync<string, ErrorWithMessage> {
    const resolved = path.resolve(directory);

    return this.exists(resolved).andThen((exists) => {
      if (exists) {
        this.spinner.succeed(`Save directory verified: ${resolved}`);
        return okAsync(resolved);
      }

      this.spinner.stop();
      return ResultAsync.fromPromise(
        inquirer.prompt([
          {
            type: "confirm",
            name: "createDir",
            message: `Directory ${directory} does not exist. Create directory?`,
            default: true,
          },
        ]),
        toErrorWithMessage,
      ).andThen((answer) => {
        if (!answer.createDir) {
          return errAsync({
            message: `Directory ${directory} does not exist.`,
          });
        }

        this.spinner.text = `Save directory ${directory} does not exist. Creating directory`;
        return ResultAsync.fromPromise(
          mkdir(resolved, { recursive: true }),
          toErrorWithMessage,
        ).map(() => resolved);
      });
    });
  }
}
