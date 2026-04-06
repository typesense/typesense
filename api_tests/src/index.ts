import { TypesenseProcessManager } from "./manager";
import { Phases, Filters } from "./constants";

export class TypesenseTestRunner {
  private manager: TypesenseProcessManager;
  private static instance: TypesenseTestRunner;
  private exit_code: number = 0;
  private readonly decoder = new TextDecoder();

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

  async run(filters: Filters[], testFile: string | null = null) {
    try {
      this.manager.cleanDataDirs();
      await this.singleServerTests(filters, testFile);
      await this.multiServerTests(filters, testFile);
      await this.noPhase(filters, testFile);
      await this.manager.shutdown();
      process.exit(this.exit_code);
    } catch (err) {
      await this.manager.shutdown();
      throw err;
    }
  }

  async singleServerTests(filters: Filters[], testFile: string | null = null) {
    try {
      await this.singleFresh(filters, testFile);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleRestarted(filters, testFile);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.singleSnapshot(filters, testFile);
    } catch (err) {
      console.error(err);
    }
  }

  async multiServerTests(filters: Filters[], testFile: string | null = null) {
    try {
      await this.multiFresh(filters, testFile);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiRestarted(filters, testFile);
    } catch (err) {
      console.error(err);
    }

    try {
      await this.multiSnapshot(filters, testFile);
    } catch (err) {
      console.error(err);
    }
  }

  private buildTestCommand(pattern: string, testFile: string | null): string[] {
    const cmd = ["bun", "test", "--test-name-pattern", pattern, "--timeout", "100000"];
    if (testFile) {
      cmd.push(testFile);
    }
    return cmd;
  }

  private runPhaseTests(phase: Phases, pattern: string, testFile: string | null) {
    const proc = Bun.spawnSync({
      cmd: this.buildTestCommand(pattern, testFile),
      stderr: "pipe",
      stdout: "pipe",
    });

    const stdout = proc.stdout ? this.decoder.decode(proc.stdout) : "";
    const stderr = proc.stderr ? this.decoder.decode(proc.stderr) : "";

    if (stdout) {
      process.stdout.write(stdout);
    }

    if (stderr) {
      process.stderr.write(stderr);
    }

    const noTestsMatched = testFile !== null && proc.exitCode !== 0 && `${stdout}\n${stderr}`.includes(`regex "${pattern}" matched 0 tests`);
    if (noTestsMatched) {
      console.log(`\n=== ⏭️ Skipping phase: ${phase} (no matching tests in ${testFile}) ===\n`);
      return true;
    }

    return proc.exitCode === 0;
  }

  async noPhase(filters: Filters[], testFile: string | null = null) {
    console.log(`\n=== ⭐ Running phase: ${Phases.NO_PHASE} ===\n`);
    const pattern = this.getTestNamePattern(Phases.NO_PHASE, filters);
    if (!this.runPhaseTests(Phases.NO_PHASE, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.NO_PHASE} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleFresh(filters: Filters[], testFile: string | null = null) {
    await this.manager.startSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_FRESH, filters);
    if (!this.runPhaseTests(Phases.SINGLE_FRESH, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleRestarted(filters: Filters[], testFile: string | null = null) {
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_RESTARTED, filters);
    if (!this.runPhaseTests(Phases.SINGLE_RESTARTED, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleSnapshot(filters: Filters[], testFile: string | null = null) {
    await this.manager.createSnapshot(8108);
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_SNAPSHOT, filters);
    if (!this.runPhaseTests(Phases.SINGLE_SNAPSHOT, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiFresh(filters: Filters[], testFile: string | null = null) {
    await this.manager.startMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_FRESH, filters);
    if (!this.runPhaseTests(Phases.MULTI_FRESH, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiRestarted(filters: Filters[], testFile: string | null = null) {
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_RESTARTED, filters);
    if (!this.runPhaseTests(Phases.MULTI_RESTARTED, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiSnapshot(filters: Filters[], testFile: string | null = null) {
    await this.manager.createSnapshot(5108);
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_SNAPSHOT, filters);
    if (!this.runPhaseTests(Phases.MULTI_SNAPSHOT, pattern, testFile)) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }
}
