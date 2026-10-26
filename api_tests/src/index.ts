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

  async run(filters: Filters[], testFile: string | null = null) {
    try {
      this.manager.cleanDataDirs();
      // the three groups share no port, directory or server, so they run side by side
      await Promise.all([
        this.singleServerTests(filters, testFile),
        this.multiServerTests(filters, testFile),
        this.noPhase(filters, testFile),
      ]);
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
      this.failStep(err);
    }

    try {
      await this.singleRestarted(filters, testFile);
    } catch (err) {
      this.failStep(err);
    }

    try {
      await this.singleSnapshot(filters, testFile);
    } catch (err) {
      this.failStep(err);
    }
  }

  async multiServerTests(filters: Filters[], testFile: string | null = null) {
    try {
      await this.multiFresh(filters, testFile);
    } catch (err) {
      this.failStep(err);
    }

    try {
      await this.multiRestarted(filters, testFile);
    } catch (err) {
      this.failStep(err);
    }

    try {
      await this.multiSnapshot(filters, testFile);
    } catch (err) {
      this.failStep(err);
    }
  }

  // a step that throws (a server that never got healthy, or one that didn't stop) leaves its phase unrun
  private failStep(err: unknown) {
    console.error(err);
    this.exit_code = 1;
  }

  private buildTestCommand(pattern: string, testFile: string | null): string[] {
    const cmd = ["bun", "test", "--test-name-pattern", pattern, "--timeout", "100000"];
    if (testFile) {
      cmd.push(testFile);
    }
    return cmd;
  }

  private async runPhaseTests(phase: Phases, pattern: string, testFile: string | null) {
    const proc = Bun.spawn({
      cmd: this.buildTestCommand(pattern, testFile),
      stderr: "pipe",
      stdout: "pipe",
    });

    // read both pipes while the phase runs, and print its output as one block so concurrent phases don't interleave
    const [stdout, stderr] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    await proc.exited;

    console.log(`\n=== Output of phase: ${phase}${testFile ? ` (${testFile})` : ""} ===\n`);
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

  // Each file with no-phase tests manages its own servers on its own ports, so the files run side by side.
  async noPhase(filters: Filters[], testFile: string | null = null) {
    console.log(`\n=== ⭐ Running phase: ${Phases.NO_PHASE} ===\n`);
    const pattern = this.getTestNamePattern(Phases.NO_PHASE, filters);
    const files = testFile ? [testFile] : await this.findFilesWith("Phases.NO_PHASE");
    const results = await Promise.all(files.map((file) => this.runPhaseTests(Phases.NO_PHASE, pattern, file)));
    if (results.includes(false)) {
      console.error(`\n=== ❌ Phase ${Phases.NO_PHASE} failed ===\n`);
      this.exit_code = 1;
    }
  }

  private async findFilesWith(text: string): Promise<string[]> {
    const files: string[] = [];
    // relative to the harness, not the caller's working directory
    for await (const file of new Bun.Glob("tests/*.test.ts").scan({ cwd: `${import.meta.dir}/..`, absolute: true })) {
      if ((await Bun.file(file).text()).includes(text)) files.push(file);
    }
    if (files.length === 0) throw new Error(`no test file under ${import.meta.dir}/../tests contains ${text}`);
    return files.sort();
  }

  async singleFresh(filters: Filters[], testFile: string | null = null) {
    await this.manager.startSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_FRESH, filters);
    if (!(await this.runPhaseTests(Phases.SINGLE_FRESH, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleRestarted(filters: Filters[], testFile: string | null = null) {
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_RESTARTED, filters);
    if (!(await this.runPhaseTests(Phases.SINGLE_RESTARTED, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async singleSnapshot(filters: Filters[], testFile: string | null = null) {
    await this.manager.createSnapshot(8108);
    await this.manager.restartSingleNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.SINGLE_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.SINGLE_SNAPSHOT, filters);
    if (!(await this.runPhaseTests(Phases.SINGLE_SNAPSHOT, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.SINGLE_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiFresh(filters: Filters[], testFile: string | null = null) {
    await this.manager.startMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_FRESH} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_FRESH, filters);
    if (!(await this.runPhaseTests(Phases.MULTI_FRESH, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_FRESH} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiRestarted(filters: Filters[], testFile: string | null = null) {
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_RESTARTED} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_RESTARTED, filters);
    if (!(await this.runPhaseTests(Phases.MULTI_RESTARTED, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_RESTARTED} failed ===\n`);
      this.exit_code = 1;
    }
  }

  async multiSnapshot(filters: Filters[], testFile: string | null = null) {
    await this.manager.createSnapshot(5108);
    await this.manager.restartMultiNode();
    console.log(`\n=== ⭐ Running phase: ${Phases.MULTI_SNAPSHOT} ===\n`);
    const pattern = this.getTestNamePattern(Phases.MULTI_SNAPSHOT, filters);
    if (!(await this.runPhaseTests(Phases.MULTI_SNAPSHOT, pattern, testFile))) {
      console.error(`\n=== ❌ Phase ${Phases.MULTI_SNAPSHOT} failed ===\n`);
      this.exit_code = 1;
    }
  }
}
