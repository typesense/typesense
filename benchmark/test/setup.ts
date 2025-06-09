import { toErrorWithMessage } from "@/utils/error";
import { exists, safeMakeOrEmptyDir, safeRm } from "@/utils/fs";

import "dotenv/config";

import inquirer from "inquirer";
import { okAsync, ResultAsync } from "neverthrow";
import ora from "ora";

export async function setup() {
  const workingDir = process.env.TYPESENSE_WORKING_DIRECTORY;

  if (!workingDir) {
    throw new Error("WorkingDirectory environment variable is not set");
  }

  let existsRes = await exists(workingDir);

  if (!existsRes.isOk() || !existsRes.value === true) {
    ora().start(`Creating working directory: ${workingDir}`);
    const res = await safeMakeOrEmptyDir({ directory: workingDir, options: { recursive: true } });

    ora().stop();
    if (!res.isOk()) {
      const failString = `Failed to create working directory: ${workingDir}`;
      ora().fail(failString);
      throw new Error(failString);
    }

    ora().succeed(`Working directory created: ${workingDir}`);
  }

  existsRes = await exists(workingDir);
  if (!existsRes.isOk() || !existsRes.value === true) {
    const failString = `Failed to create working directory: ${workingDir}`;
    ora().fail(failString);
    throw new Error(failString);
  }

  ora().succeed(`Working directory is ready: ${workingDir}`);
}

export async function teardown() {
  const workingDir = process.env.TYPESENSE_WORKING_DIRECTORY;

  if (!workingDir) {
    throw new Error("WorkingDirectory environment variable is not set");
  }

  const isInCi = process.env.CI === "true";

  if (isInCi) {
    return safeRm({ directory: workingDir, options: { recursive: true } }).andThen(() => {
      ora().succeed(`Working directory ${workingDir} deleted`);
      return okAsync(undefined);
    });
  }

  return ResultAsync.fromPromise(
    inquirer.prompt([
      {
        type: "confirm",
        name: "rmDir",
        message: `Delete the working directory ${workingDir}?`,
        default: true,
      },
    ]),
    toErrorWithMessage,
  ).andThen(({ rmDir }) =>
    rmDir ?
      safeRm({ directory: workingDir, options: { recursive: true } }).andThen(() => {
        ora().succeed(`Working directory ${workingDir} deleted`);
        return okAsync(undefined);
      })
    : okAsync(undefined).andThen(() => {
        ora().succeed(`Working directory ${workingDir} NOT deleted`);
        return okAsync(undefined);
      }),
  );
}
