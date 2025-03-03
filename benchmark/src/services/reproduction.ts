import { writeFile } from "fs/promises";
import path from "path";
import type { FormattedSearchResult } from "@/commands/benchmark";
import type { SearchParams } from "typesense/lib/Typesense/Documents";

import { ResultAsync } from "neverthrow";

import { searchScenarios } from "@/benchmarks/k6-utils";
import { K6Benchmarks } from "@/services/k6";
import { toErrorWithMessage } from "@/utils/error";

export class ReproductionService {
  generateReproductionFile(params: {
    passingBenchmarks: FormattedSearchResult[];
    failingBenchmarks: FormattedSearchResult[];
    commitHash?: string;
    apiKey: string;
  }) {
    const markdownGuide = this.generateMarkdownGuide(params);
    const filePath = path.resolve("reproduction-guide.md");
    return ResultAsync.fromPromise(writeFile(filePath, markdownGuide), toErrorWithMessage).map(() => filePath);
  }

  private formatSearchParams(params: SearchParams) {
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        (params as Record<string, unknown>)[key] = value.join(",");
      }
    });
    return new URLSearchParams(params as Record<string, string>).toString();
  }

  private generateScenariosMarkdown(params: { benchmarks: FormattedSearchResult[]; apiKey: string }) {
    const scenarioGroups = new Map<
      string,
      {
        scenario: (typeof searchScenarios)[number] | undefined;
        results: (typeof params.benchmarks)[number][];
      }
    >();

    params.benchmarks.forEach((result) => {
      if (!scenarioGroups.has(result.scenario)) {
        const scenario = searchScenarios.find((s) => s.name === result.scenario);
        scenarioGroups.set(result.scenario, {
          scenario: scenario,
          results: [],
        });
      }
      const scenarioGroup = scenarioGroups.get(result.scenario);
      if (scenarioGroup) {
        scenarioGroup.results.push(result);
      }
    });

    return Array.from(scenarioGroups.entries())
      .map(([scenarioName, { scenario, results }]) => {
        const vuInfo = results
          .map(
            (r) =>
              `${r.vus} VUs: ${r.newValue} vs ${r.oldValue} (${
                Number.isInteger(r.percentageChange) ? r.percentageChange : r.percentageChange.toFixed(3)
              }%)`,
          )
          .join("\n# ");
        return `
### ${scenarioName}
### Search Parameters
\`\`\`json
${JSON.stringify(scenario?.params, null, 2)}
\`\`\`
### Curl Request
\`\`\`bash
# ${vuInfo}
curl "http://localhost:8108/collections/${K6Benchmarks.COLLECTION_NAME}/documents/search?${this.formatSearchParams(scenario!.params)}" \\
    -X GET \\
    -H "Content-Type: application/json" \\
    -H "X-TYPESENSE-API-KEY: ${params.apiKey}"
\`\`\``;
      })
      .join("");
  }

  private generateMarkdownGuide(params: {
    passingBenchmarks: FormattedSearchResult[];
    failingBenchmarks: FormattedSearchResult[];
    commitHash?: string;
    apiKey: string;
  }) {
    return `
# Reproduction Guide
## Failing commit
\`${params.commitHash ?? "All tests passed"}\`
## Steps to reproduce
1. Create the collection
\`\`\`bash
curl "http://localhost:8108/collections" \\
    -X POST \\
    -H "Content-Type: application/json" \\
    -H "X-TYPESENSE-API-KEY: ${params.apiKey}" \\
    -d '${JSON.stringify(K6Benchmarks.COLLECTION_SCHEMA)}'
\`\`\`
2. Download the dataset from [here](${K6Benchmarks.DATASET_URL})
3. Index the dataset
\`\`\`bash
curl "http://localhost:8108/collections/${K6Benchmarks.COLLECTION_NAME}/documents/import" \\
    -X POST \\
    -H "Content-Type: application/json" \\
    -H "X-TYPESENSE-API-KEY: ${params.apiKey}" \\
    --data-binary @musicbrainz-1M-songs.jsonl
\`\`\`
  
4. Run the passing test benchmark scenarios
## Passing Scenarios
${this.generateScenariosMarkdown({ benchmarks: params.passingBenchmarks, apiKey: params.apiKey })}
5. Run the failing test benchmark scenarios
## Failing Scenarios
${this.generateScenariosMarkdown({ benchmarks: params.failingBenchmarks, apiKey: params.apiKey })}`;
  }
}
