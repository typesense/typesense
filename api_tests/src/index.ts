import { TypesenseProcessManager } from "./manager";
import { Phases } from "./constants";

class TypesenseTestRunner {
  private manager: TypesenseProcessManager;
  private static instance: TypesenseTestRunner;

  constructor() {
    this.manager = new TypesenseProcessManager();
  }

  static getInstance() {
    if (!TypesenseTestRunner.instance) {
      TypesenseTestRunner.instance = new TypesenseTestRunner();
    }
    return TypesenseTestRunner.instance;
  }

  async run() {
    try {
      this.manager.cleanDataDirs();
      await Promise.all([
        this.singleServerTests(),
        this.multiServerTests(),
        this.noPhase(),
      ]);
      await this.manager.shutdown();
    } catch (err) {
      await this.manager.shutdown();
      throw err;
    }
  }

  async singleServerTests() {
    try {
      await this.singleFresh();
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleRestarted();
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleSnapshot();
    } catch (err) {
      console.error(err);
    }
  }

  async multiServerTests() {
    try {
      await this.multiFresh();
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiRestarted();
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiSnapshot();
    } catch (err) {
      console.error(err);
    }
  }

  async noPhase() {
    console.log(`\n=== ⭐ Running phase: ${Phases.NO_PHASE} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.NO_PHASE],
      stderr: "inherit",
      stdout: "inherit",
    });
  }

  async singleFresh() {
    await this.manager.startSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_FRESH} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.SINGLE_FRESH],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_FRESH} failed ===\n`);
    }
  }

  async singleRestarted() {
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_RESTARTED} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.SINGLE_RESTARTED],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_RESTARTED} failed ===\n`);
    }
  }

  async singleSnapshot() {
    await this.manager.createSnapshot(8108);
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_SNAPSHOT} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.SINGLE_SNAPSHOT],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_SNAPSHOT} failed ===\n`);
    }
  }

  async multiFresh() {
    await this.manager.startMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_FRESH} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.MULTI_FRESH],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_FRESH} failed ===\n`);
    }
  }

  async multiRestarted() {
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_RESTARTED} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.MULTI_RESTARTED],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_RESTARTED} failed ===\n`);
    }
  }

  async multiSnapshot() {
    await this.manager.createSnapshot(5108);
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_SNAPSHOT} ===\n`);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", Phases.MULTI_SNAPSHOT],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_SNAPSHOT} failed ===\n`);
    }
  }
}

async function main() {
  const runner = TypesenseTestRunner.getInstance();
  await runner.run();
}

main();