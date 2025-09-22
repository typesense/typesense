import { TypesenseProcessManager } from "./manager";
import { Phases, Filters } from "./constants";

export class TypesenseTestRunner {
  private manager: TypesenseProcessManager;
  private static instance: TypesenseTestRunner;
  private exit_code: number = 0;

  constructor() {
    this.manager = new TypesenseProcessManager();
  }

  static getInstance() {
    if (!TypesenseTestRunner.instance) {
      TypesenseTestRunner.instance = new TypesenseTestRunner();
    }
    return TypesenseTestRunner.instance;
  }

  getTestNamePattern(phase: Phases, filters: Filters[]) {
    if (filters.includes(Filters.SECRETS)) {
      return `^(?=.*${phase})(?!.*${Filters.SECRETS}).*$`;
    }
    return phase;
  }

  async run(filters: Filters[]) {
    try {
      this.manager.cleanDataDirs();
      await this.singleServerTests(filters);
      await this.multiServerTests(filters);
      await this.noPhase(filters);
      await this.manager.shutdown();
      process.exit(this.exit_code);
    } catch (err) {
      await this.manager.shutdown();
      throw err;
    }
  }

  async singleServerTests(filters: Filters[]) {
    try {
      await this.singleFresh(filters);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleRestarted(filters);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleSnapshot(filters);
    } catch (err) {
      console.error(err);
    }
  }

  async multiServerTests(filters: Filters[]) {
    try {
      await this.multiFresh(filters);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiRestarted(filters);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiSnapshot(filters);
    } catch (err) {
      console.error(err);
    }
  }

  async noPhase(filters: Filters[]) {
    console.log(`\n=== ⭐ Running phase: ${Phases.NO_PHASE} ===\n`);
    const pattern = this.getTestNamePattern(Phases.NO_PHASE, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.NO_PHASE} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleFresh(filters: Filters[]) {
    await this.manager.startSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_FRESH, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleRestarted(filters: Filters[]) {
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_RESTARTED, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleSnapshot(filters: Filters[]) {
    await this.manager.createSnapshot(8108);
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_SNAPSHOT, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiFresh(filters: Filters[]) {
    await this.manager.startMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_FRESH, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiRestarted(filters: Filters[]) {
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_RESTARTED, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiSnapshot(filters: Filters[]) {
    await this.manager.createSnapshot(5108);
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_SNAPSHOT, filters);
    const proc = Bun.spawnSync({
      cmd: ["bun", "test", "curation_sets.test.ts", "--test-name-pattern", pattern, "--timeout", "100000"],
      stderr: "inherit",
      stdout: "inherit",
    });
    if (proc.exitCode !== 0) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }
}