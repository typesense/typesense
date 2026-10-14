import type { SearchParams } from "typesense/lib/Typesense/Documents";
import type { K6Env } from "../services/k6";
import type { EnsureExhaustive, EnvVariableKey } from "../utils/types";

interface ValidationError {
  key: keyof K6Env;
  value: string | number | undefined;
  reason: string;
}

type ValidationResult = { isValid: true; env: K6Env } | { isValid: false; errors: ValidationError[] };

export function validateK6Environment(env: EnvVariableKey<K6Env>): ValidationResult {
  const requiredKeys = [
    "API_KEY",
    "HOST",
    "PORT",
    "BATCH_SIZE",
    "COLLECTION_NAME",
    "COMMIT_HASH",
    "DURATION",
  ] as const satisfies (keyof K6Env)[];

  type Validated = EnsureExhaustive<K6Env, typeof requiredKeys>;

  const _validated: Validated = true;
  const errors: ValidationError[] = [];

  requiredKeys.forEach((key) => {
    const value = env[key];
    if (value === undefined || value === null) {
      errors.push({
        key,
        value: undefined,
        reason: "Value is required but was not provided",
      });
      return;
    }

    switch (key) {
      case "PORT":
      case "BATCH_SIZE": {
        if (typeof value !== "string") {
          errors.push({
            key,
            value,
            reason: "Value must be a string",
          });
        } else if (!parseInt(value)) {
          errors.push({
            key,
            value,
            reason: "Value must be a valid integer",
          });
        }
        break;
      }
      case "API_KEY":
      case "HOST":
      case "COMMIT_HASH":
      case "COLLECTION_NAME": {
        if (typeof value !== "string") {
          errors.push({
            key,
            value,
            reason: "Value must be a string",
          });
        } else if (value.length === 0) {
          errors.push({
            key,
            value,
            reason: "Value cannot be empty",
          });
        }
        break;
      }
      case "DURATION": {
        if (typeof value !== "string") {
          errors.push({
            key,
            value,
            reason: "Value must be a string",
          });
        } else if (!/^\d+[smhd]$/.test(value)) {
          errors.push({
            key,
            value,
            reason: "Duration must be in format: number followed by s/m/h/d",
          });
        }
        break;
      }
    }
  });

  if (errors.length === 0) {
    return { isValid: true, env: env as K6Env };
  }

  return { isValid: false, errors };
}

export const searchScenarios = [
  {
    name: "just_q",
    params: {
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
    },
    wildCardQuery: false,
  },
  {
    name: "q_star",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_simple",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      filter_by: "genres:Rock",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_complex",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      filter_by: "genres:Rock && primary_artist_name:Queen || primary_artist_name:Led Zeppelin",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_selective_and",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      // A conjunction whose sides differ in selectivity by three orders of magnitude: the artist matches
      // 431 songs, the release types 960,372 of the million. The result is the size of the narrow side, so
      // the only thing the wide side can cost is the cost of computing it.
      filter_by: "primary_artist_name:Nirvana && release_group_types:[Album,Single,Compilation]",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_and_ratio_17x",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      // 54,683 releases against 913,296. Lopsided, but not by enough that asking the wide side about the
      // narrow side's ids one at a time beats intersecting the two. Exact matches rather than token
      // matches, so both counts -- and the ratio between them -- stay predictable.
      filter_by: "release_decade:=1980s && release_group_types:=Album",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_and_ratio_32x",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      // 28,666 against 913,296, within a whisker of the ratio that decides between the two plans, so this
      // is the scenario that notices when that threshold moves. The sides are estimated rather than counted
      // at the moment the choice is made, so which way this one falls is not guaranteed.
      filter_by: "country:=AU && release_group_types:=Album",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_and_numeric_wide",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      // 431 songs against 899,558 releases since 1986. Lopsided enough to be worth probing on size alone,
      // except that a numeric range has already built its id list by the time the conjunction is planned,
      // leaving nothing for a probing plan to save.
      filter_by: "primary_artist_name:=Nirvana && release_date:>504921600",
    },
    wildCardQuery: true,
  },
  {
    name: "filter_and_numeric_wide_lazy",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      // The same shape under lazy filter evaluation, where the numeric side keeps one iterator per value in
      // the range instead of a single id list. Stepping through that is far dearer than materializing it, so
      // this scenario is what catches a plan that starts probing such a side.
      filter_by: "primary_artist_name:=Nirvana && release_date:>504921600",
      enable_lazy_filter: "true",
    },
    wildCardQuery: true,
  },
  {
    name: "sort_simple",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      sort_by: "release_date:desc",
    },
    wildCardQuery: true,
  },
  {
    name: "sort_eval_condition",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      sort_by: "_eval(primary_artist_name:Queen):desc, release_date:desc",
    },
    wildCardQuery: true,
  },
  {
    name: "sort_eval_score",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      sort_by: "_eval([(primary_artist_name:Queen):3, (primary_artist_name:Nirvana):5]):desc, release_date:desc",
    },
    wildCardQuery: true,
  },
  {
    name: "facet",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      facet_by: "genres,country,release_decade",
    },
    wildCardQuery: true,
  },
  {
    name: "group",
    params: {
      q: "*",
      query_by: "primary_artist_name,title,album_name",
      highlight_full_fields: "primary_artist_name,title,album_name",
      group_by: "genres",
    },
    wildCardQuery: true,
  },
] as const satisfies Scenario[];

type Scenario =
  | {
      name: string;
      params: Omit<SearchParams, "q">;
      wildCardQuery: false;
    }
  | {
      name: string;
      params: Omit<SearchParams, "q"> & { q: "*" };
      wildCardQuery: true;
    };
