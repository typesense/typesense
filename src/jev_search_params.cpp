#include "jev_search_params.h"
#include "jev_client.h"
#include "collection_manager.h"
#include "filter.h"
#include "tokenizer.h"
#include "stemmer_manager.h"
#include "string_utils.h"
#include "logger.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <memory>
#include <regex>
#include <sstream>
#include <unordered_set>

static long elapsed_ms(const std::chrono::steady_clock::time_point& since) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - since).count();
}

static bool needs_backticks(const std::string& value) {
    if(value.empty()) {
        return false;
    }

    if(value[0] == '=' || value[0] == '!') {
        return true;
    }

    // bare `_missing` is the missing values operator, backticked it is a literal
    if(value == "_missing") {
        return true;
    }

    return value.find_first_of("()&|,[]{}:") != std::string::npos;
}

std::string JevSearchParams::sanitize_filter_value(const std::string& raw_value) {
    std::string value;
    value.reserve(raw_value.size());

    // backticks, backslashes and quotes cannot round trip through split_to_values, all three are token separators
    for(char c : raw_value) {
        if(c == '`' || c == '\\' || c == '"') {
            continue;
        }
        value += c;
    }

    StringUtils::trim(value);

    const size_t begin = value.find_first_not_of('*');
    if(begin == std::string::npos) {
        return "";
    }
    const size_t end = value.find_last_not_of('*');
    value = value.substr(begin, end - begin + 1);

    StringUtils::trim(value);
    return value;
}

std::string JevSearchParams::escape_filter_value(const std::string& raw_value) {
    const std::string value = sanitize_filter_value(raw_value);
    if(value.empty()) {
        return "";
    }

    if(needs_backticks(value)) {
        return "`" + value + "`";
    }

    return value;
}

// a value that tokenizes to nothing hard 400s at filter time and parse_filter_query does not catch it
static bool has_indexable_token(const std::string& value, const field& f) {
    Tokenizer tokenizer(value, true, false, f.locale, f.symbols_to_index, f.token_separators);
    std::string token;
    size_t token_index = 0;
    return tokenizer.next(token, token_index);
}

std::string JevSearchParams::facet_present_id(size_t i) { return "f" + std::to_string(i) + "__present"; }
std::string JevSearchParams::facet_value_id(size_t i) { return "f" + std::to_string(i) + "__value"; }
std::string JevSearchParams::facet_value2_id(size_t i) { return "f" + std::to_string(i) + "__value2"; }
std::string JevSearchParams::facet_both_id(size_t i) { return "f" + std::to_string(i) + "__both"; }
std::string JevSearchParams::facet_negate_id(size_t i) { return "f" + std::to_string(i) + "__negate"; }
std::string JevSearchParams::facet_alt_id(size_t i) { return "f" + std::to_string(i) + "__alt"; }
std::string JevSearchParams::bool_alt_id(size_t i) { return "b" + std::to_string(i) + "__alt"; }
std::string JevSearchParams::number_alt_id(size_t k) { return "num" + std::to_string(k) + "__alt"; }
std::string JevSearchParams::number_field_id(size_t k) { return "num" + std::to_string(k) + "__field"; }
std::string JevSearchParams::number_op_id(size_t k) { return "num" + std::to_string(k) + "__op"; }
std::string JevSearchParams::number_also_id(size_t k, size_t i) {
    return "num" + std::to_string(k) + "__also" + std::to_string(i);
}
std::string JevSearchParams::sort_dir_id(size_t i) { return "sort" + std::to_string(i) + "__dir"; }
std::string JevSearchParams::bool_id(size_t i) { return "b" + std::to_string(i); }
std::string JevSearchParams::token_id(size_t i) { return "q" + std::to_string(i); }
std::string JevSearchParams::token_number_id(size_t i) { return "q" + std::to_string(i) + "__num"; }

// libstdc++ regex recursion overflows the stack on long digit runs, the scan length is bounded
static constexpr size_t MAX_NUMBER_SCAN_CHARS = 2048;

static constexpr size_t MAX_NUMBER_LITERALS = 8;

static std::string context_around(const std::string& text, size_t begin, size_t end) {
    size_t ctx_begin = begin;
    for(int i = 0; i < 2; i++) {
        if(ctx_begin < 2) {
            ctx_begin = 0;
            break;
        }
        size_t prev = text.rfind(' ', ctx_begin - 2);
        if(prev == std::string::npos) {
            ctx_begin = 0;
            break;
        }
        ctx_begin = prev + 1;
    }

    size_t ctx_end = end;
    for(int i = 0; i < 2; i++) {
        size_t next = text.find(' ', ctx_end + 1);
        if(next == std::string::npos) {
            ctx_end = text.size();
            break;
        }
        ctx_end = next;
    }

    return text.substr(ctx_begin, ctx_end - ctx_begin);
}

static const std::regex& number_regex() {
    static const std::regex re(R"((-?)\$?((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)([kKmMbB](?![A-Za-z0-9]))?)");
    return re;
}

static std::string format_number(double value) {
    if(std::fabs(value) < 1e15 && value == std::floor(value)) {
        return std::to_string(static_cast<long long>(value));
    }

    std::ostringstream os;
    os.precision(15);
    os << value;
    return os.str();
}

static std::string today_utc() {
    std::time_t now = std::time(nullptr);
    std::tm tm_utc{};
    gmtime_r(&now, &tm_utc);

    char buf[16] = {};
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_utc);
    return std::string(buf);
}

jev_query_facts_t JevSearchParams::pre_parse(const std::string& query) {
    jev_query_facts_t facts;
    facts.query = query;
    facts.today = today_utc();

    StringUtils::split(query, facts.tokens, " ");

    facts.has_query_operators = query.find('"') != std::string::npos;
    for(const auto& token : facts.tokens) {
        if(token.size() > 1 && token[0] == '-' && !std::isdigit(static_cast<unsigned char>(token[1]))) {
            facts.has_query_operators = true;
        }

        std::string lowered = token;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(), ::tolower);
        if(lowered == "or" || lowered == "either") {
            facts.has_or = true;
        }
    }

    const std::string scan = query.size() > MAX_NUMBER_SCAN_CHARS ?
                             query.substr(0, MAX_NUMBER_SCAN_CHARS) : query;

    auto begin = std::sregex_iterator(scan.begin(), scan.end(), number_regex());
    auto end = std::sregex_iterator();

    size_t prev_match_end = std::string::npos;

    for(auto it = begin; it != end; ++it) {
        if(facts.numbers.size() >= MAX_NUMBER_LITERALS) {
            break;
        }

        const std::smatch& match = *it;

        const size_t match_begin = match.position(0);
        const bool separator_minus = !match[1].str().empty() && match_begin == prev_match_end;
        const bool negative = !match[1].str().empty() && !separator_minus;
        prev_match_end = match_begin + match.length(0);

        std::string digits = match[2].str();
        digits.erase(std::remove(digits.begin(), digits.end(), ','), digits.end());
        if(digits.empty()) {
            continue;
        }

        std::string normalized = digits;
        const std::string multiplier = match[3].str();
        if(!multiplier.empty()) {
            double scale = 1;
            const char m = std::tolower(multiplier[0]);
            if(m == 'k') {
                scale = 1e3;
            } else if(m == 'm') {
                scale = 1e6;
            } else if(m == 'b') {
                scale = 1e9;
            }

            try {
                normalized = format_number(std::stod(digits) * scale);
            } catch(...) {
                continue;
            }
        }

        if(negative) {
            normalized = "-" + normalized;
        }

        facts.numbers.push_back(normalized);
        facts.number_spans.push_back(separator_minus ? match[0].str().substr(1) : match[0].str());
        facts.number_contexts.push_back(context_around(scan, match.position(0),
                                                       match.position(0) + match.length(0)));
    }

    return facts;
}

// reference helpers leak as sortable int64, unindexed fields 400 late, geo and vector are unfilterable from text
static bool is_filterable_candidate(const field& f) {
    return f.index && !f.is_reference_helper && f.num_dim == 0 && f.name != "id" &&
           !f.is_geopoint() && !f.is_geopolygon() && !f.is_object() && !f.is_image();
}

Option<jev_catalog_t> JevSearchParams::build_catalog(const std::string& collection_name,
                                                     const schema_prompt_params_t& prompt_params,
                                                     const std::string& scope_filter) {
    auto collection = CollectionManager::get_instance().get_collection(collection_name);
    if(collection == nullptr) {
        return Option<jev_catalog_t>(404, "Collection not found");
    }

    Collection* coll = collection.get();
    const auto search_schema = coll->get_schema();

    jev_catalog_t catalog;
    catalog.collection_name = collection_name;

    auto facet_fields_op = NaturalLanguageSearchModelManager::resolve_facet_fields(coll, prompt_params);
    if(!facet_fields_op.ok()) {
        return Option<jev_catalog_t>(facet_fields_op.code(), facet_fields_op.error());
    }

    const auto& string_facet_fields = facet_fields_op.get();
    auto field_facet_values = NaturalLanguageSearchModelManager::fetch_facet_values(coll, string_facet_fields,
                                                                                    prompt_params, scope_filter);

    for(const auto& field_name : string_facet_fields) {
        auto values_it = field_facet_values.find(field_name);
        if(values_it == field_facet_values.end() || values_it->second.empty()) {
            continue;
        }

        auto schema_it = search_schema.find(field_name);
        if(schema_it == search_schema.end() || !is_filterable_candidate(schema_it.value())) {
            continue;
        }

        jev_facet_field_t facet_field;
        facet_field.name = field_name;

        std::unordered_set<std::string> seen;
        for(const auto& raw_value : values_it->second) {
            if(facet_field.values.size() >= prompt_params.schema_sample_values) {
                break;
            }

            const std::string value = sanitize_filter_value(raw_value);
            if(value.empty() || value == JevClient::NONE_OPTION || seen.count(value) != 0) {
                continue;
            }
            if(!has_indexable_token(value, schema_it.value())) {
                continue;
            }

            seen.insert(value);
            facet_field.values.push_back(value);
        }

        if(!facet_field.values.empty()) {
            catalog.facet_fields.push_back(facet_field);
        }
    }

    for(auto it = search_schema.begin(); it != search_schema.end(); ++it) {
        const auto& f = it.value();
        if(!is_filterable_candidate(f)) {
            continue;
        }

        if(f.is_integer() || f.is_float()) {
            catalog.numeric_fields.push_back({f.name, f.type});
        } else if(f.is_bool()) {
            catalog.bool_fields.push_back(f.name);
        }
    }

    for(const auto& f : coll->get_sort_fields()) {
        if(is_filterable_candidate(f)) {
            catalog.sort_fields.push_back(f.name);
        }
    }

    for(auto it = search_schema.begin(); it != search_schema.end(); ++it) {
        if(!it.value().description.empty()) {
            catalog.field_descriptions[it.value().name] = it.value().description;
        }
    }

    return Option<jev_catalog_t>(catalog);
}

nlohmann::json JevSearchParams::build_state(const jev_query_facts_t& facts, const jev_catalog_t& catalog,
                                            const std::string& context) {
    nlohmann::json state;
    state["query"] = facts.query;
    state["today"] = facts.today;
    state["numbers_found"] = facts.numbers;

    nlohmann::json values = nlohmann::json::object();
    for(const auto& facet_field : catalog.facet_fields) {
        values[facet_field.name] = facet_field.values;
    }
    if(!values.empty()) {
        state["values"] = values;
    }

    nlohmann::json excluded_terms = nlohmann::json::array();
    for(const auto& token : facts.tokens) {
        if(token.size() > 1 && token[0] == '-' && !std::isdigit(static_cast<unsigned char>(token[1]))) {
            excluded_terms.push_back(token.substr(1));
        }
    }
    if(!excluded_terms.empty()) {
        state["excluded_terms"] = excluded_terms;
    }

    std::vector<std::string> attribute_lines;
    for(const auto& [field_name, description] : catalog.field_descriptions) {
        attribute_lines.push_back(field_name + ": " + description);
    }
    std::sort(attribute_lines.begin(), attribute_lines.end());
    if(!attribute_lines.empty()) {
        state["attributes"] = attribute_lines;
    }

    if(!context.empty()) {
        state["context"] = context;
    }

    return state;
}

static const std::vector<std::pair<std::string, std::string>>& numeric_op_options() {
    static const std::vector<std::pair<std::string, std::string>> options = {
        {"eq", "results have to equal the number exactly"},
        {"gt", "results have to be greater than the number"},
        {"gte", "results have to be at least the number"},
        {"lt", "results have to be less than the number"},
        {"lte", "results have to be at most the number"},
    };
    return options;
}

static std::string described(const jev_catalog_t& catalog, const std::string& name) {
    const auto it = catalog.field_descriptions.find(name);
    if(it == catalog.field_descriptions.end()) {
        return "`" + name + "`";
    }
    return "`" + name + "` (" + it->second + ")";
}

static nlohmann::json alt_question(const std::string& subject) {
    return JevClient::noul_question(
            "In the query, is the " + subject + " itself listed as one of the `or` alternatives?",
            "the value this condition requires is written in the `or` list itself, the way `red` and "
            "`blue` each are in `red or blue`",
            "the condition is stated on its own elsewhere in the query, or it is only implied by or "
            "related to one of the alternatives without being listed");
}

static void add_numeric_questions(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                  const jev_options_t& opts, nlohmann::json& questions) {
    const size_t num_numeric = std::min(catalog.numeric_fields.size(), opts.max_options_per_question);

    std::vector<std::pair<std::string, std::string>> field_options;
    for(size_t i = 0; i < num_numeric; i++) {
        field_options.emplace_back(catalog.numeric_fields[i].name,
                                   "the number is about the " +
                                   described(catalog, catalog.numeric_fields[i].name) + " field");
    }
    field_options.emplace_back(JevClient::NONE_OPTION, "the number is not about any of these fields");

    for(size_t k = 0; k < facts.numbers.size(); k++) {
        const std::string occurrence = "`" + facts.number_spans[k] + "` in `" +
                (k < facts.number_contexts.size() ? facts.number_contexts[k] : facts.number_spans[k]) + "`";

        questions[JevSearchParams::number_field_id(k)] = JevClient::choice_question(
                "The query contains the number " + occurrence +
                ". Which field is that number about?", field_options);

        questions[JevSearchParams::number_op_id(k)] = JevClient::choice_question(
                "How does the query use the number " + occurrence +
                " to bound the field it is about?", numeric_op_options());

        for(size_t i = 0; i < num_numeric; i++) {
            questions[JevSearchParams::number_also_id(k, i)] = JevClient::noul_question(
                    "In the query, does the number " + occurrence + " apply to the " +
                    described(catalog, catalog.numeric_fields[i].name) + " field too?",
                    "the query gives this number for `" + catalog.numeric_fields[i].name +
                    "` as well, as when several fields share one number",
                    "the number is about some other field, `" + catalog.numeric_fields[i].name +
                    "` does not take it");
        }

        if(facts.has_or) {
            questions[JevSearchParams::number_alt_id(k)] = alt_question(
                    "condition built around the number " + occurrence);
        }
    }
}

Option<nlohmann::json> JevSearchParams::build_questions(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                                        const jev_options_t& opts) {
    const bool ask_numeric = !facts.numbers.empty() && !catalog.numeric_fields.empty();
    const bool ask_sort = !catalog.sort_fields.empty();
    const bool ask_tokens = !facts.has_query_operators && !facts.tokens.empty() &&
                            facts.tokens.size() <= opts.max_token_questions;

    const bool ask_word_numbers = opts.word_numbers && ask_tokens && facts.numbers.empty() &&
                                  !catalog.numeric_fields.empty();

    const size_t per_facet = facts.has_or ? 6 : 5;
    const size_t per_number = 2 + std::min(catalog.numeric_fields.size(), opts.max_options_per_question) +
                              (facts.has_or ? 1 : 0);
    size_t num_questions = catalog.facet_fields.size() * per_facet +
                           catalog.bool_fields.size() * (facts.has_or ? 2 : 1) +
                           (ask_numeric ? facts.numbers.size() * per_number : 0) +
                           (ask_sort ? 1 + std::min(catalog.sort_fields.size(), opts.max_options_per_question) : 0) +
                           (ask_tokens ? facts.tokens.size() * (ask_word_numbers ? 2 : 1) : 0);

    if(num_questions > opts.max_questions) {
        return Option<nlohmann::json>(400, "Collection `" + catalog.collection_name + "` needs " +
                                           std::to_string(num_questions) + " judgments, which is over the limit of " +
                                           std::to_string(opts.max_questions) + ". Narrow the field set with "
                                           "`nl_facet_fields`.");
    }

    nlohmann::json questions = nlohmann::json::object();

    for(size_t i = 0; i < catalog.facet_fields.size(); i++) {
        const auto& facet_field = catalog.facet_fields[i];
        const std::string& name = facet_field.name;

        questions[facet_present_id(i)] = JevClient::noul_question(
                "Does the search query itself ask for or rule out one of the " +
                described(catalog, name) + " values listed under `" + name + "` in `values`?",
                "the query refers to one of those listed values directly or by obvious rephrasing, "
                "whether it wants it or excludes it",
                "the query does not touch any listed `" + name + "` value, one is at most deducible "
                "from some other thing the query names, the way a city implies its region, or a query "
                "word is about something else and merely resembles a listed value, the way `italian` "
                "in `italian food` names the cuisine and not the country `Italy`");

        // speculative for every field, read back only past the present gate
        std::vector<std::pair<std::string, std::string>> value_options;
        for(size_t v = 0; v < facet_field.values.size() && v < opts.max_options_per_question; v++) {
            value_options.emplace_back(facet_field.values[v], "");
        }
        value_options.emplace_back(JevClient::NONE_OPTION,
                                   "the query refers to none of these `" + name + "` values");

        questions[facet_value_id(i)] = JevClient::choice_question(
                "Which `" + name + "` value does the query refer to, whether it wants it or rules it out?",
                value_options);

        // a choice concentrates its mass on one winner, the second value needs its own premise stated question
        std::vector<std::pair<std::string, std::string>> value2_options;
        for(size_t v = 0; v < facet_field.values.size() && v < opts.max_options_per_question; v++) {
            value2_options.emplace_back(facet_field.values[v], "");
        }
        value2_options.emplace_back(JevClient::NONE_OPTION,
                                    "the query refers to at most one `" + name + "` value");

        questions[facet_value2_id(i)] = JevClient::choice_question(
                "If the query refers to more than one `" + name +
                "` value, which is the second one?", value2_options);

        questions[facet_both_id(i)] = JevClient::noul_question(
                "If the query refers to two `" + name + "` values, must matching results satisfy "
                "both at the same time?",
                "the query wants both values to hold at once, the way `with outdoor seating` adds to "
                "`wheelchair accessible`",
                "either value on its own is enough, as with `red or blue`, or the query refers to at "
                "most one `" + name + "` value");

        // negation is a known jev weakness, the false side covers not-mentioned to avoid vacuous truth
        questions[facet_negate_id(i)] = JevClient::noul_question(
                "Does the query rule out one of the `" + name + "` values listed under `" + name +
                "` in `values`, rather than ask for one?",
                "an exclusion word points at a listed value itself, as in `not red`, `-red` or "
                "`anything but red`, and any term in `excluded_terms` is ruled out this way",
                "the query asks for a listed `" + name + "` value positively or does not mention one, "
                "or its negation applies to some other thing, the way `not in spain` rules out a "
                "country and not a cuisine named elsewhere in the query");

        if(facts.has_or) {
            questions[facet_alt_id(i)] = JevClient::noul_question(
                    "Does the query's `or` list itself contain one of the `" + name +
                    "` values listed under `" + name + "` in `values`?",
                    "one of the alternatives written around `or` is a `" + name + "` value from that "
                    "list",
                    "the `or` alternatives are other things, any `" + name + "` requirement stands on "
                    "its own outside the list, or an alternative merely resembles a listed value while "
                    "being about something else, the way `greek food` names a cuisine and not the "
                    "country `Greece`");
        }
    }

    if(ask_numeric) {
        add_numeric_questions(catalog, facts, opts, questions);
    }

    for(size_t i = 0; i < catalog.bool_fields.size(); i++) {
        const std::string& name = catalog.bool_fields[i];
        questions[bool_id(i)] = JevClient::choice_question(
                "What does the query require of the yes/no field " + described(catalog, name) + "?",
                {{"true", "the query asks for results where `" + name + "` is true"},
                 {"false", "the query asks for results where `" + name + "` is false"},
                 {"unspecified", "the query says nothing about `" + name + "`"}});

        if(facts.has_or) {
            questions[bool_alt_id(i)] = alt_question("`" + name + "` condition");
        }
    }

    if(ask_sort) {
        std::vector<std::pair<std::string, std::string>> sort_options;
        for(size_t i = 0; i < catalog.sort_fields.size() && i < opts.max_options_per_question; i++) {
            sort_options.emplace_back(catalog.sort_fields[i],
                                      "order the results by " + described(catalog, catalog.sort_fields[i]));
        }
        sort_options.emplace_back(JevClient::NONE_OPTION, "the query asks for no particular ordering");

        questions[SORT_FIELD_ID] = JevClient::choice_question(
                "Which field does the query want the results ordered by, whether it says so directly or "
                "through a word like `best` or `cheapest`?", sort_options);

        for(size_t i = 0; i < catalog.sort_fields.size() && i < opts.max_options_per_question; i++) {
            const std::string& name = catalog.sort_fields[i];
            questions[sort_dir_id(i)] = JevClient::choice_question(
                    "If the results are ordered by " + described(catalog, name) +
                    ", which direction does the query want?",
                    {{"asc", "smallest, cheapest, oldest or lowest first"},
                     {"desc", "largest, most expensive, newest or highest first"}});
        }
    }

    if(ask_tokens) {
        for(size_t i = 0; i < facts.tokens.size(); i++) {
            questions[token_id(i)] = JevClient::choice_question(
                    "In the query, what role does the word `" + facts.tokens[i] + "` play?",
                    {{"content", "the word names a thing, a property or a name the matching result's "
                                 "own text should contain, as in `sushi`, `nike` or `waterproof`"},
                     {"excluded", "the word names the thing being ruled out, whose text matching "
                                  "results should not contain, the way `fast` and `food` are in "
                                  "`not fast food`, while the negation word `not` itself is filler"},
                     {"filler", "the word is an opinion, ranking or praise word, or the attribute being "
                                "praised or measured, as in `best`, `top`, `reviewed` in `best reviewed` "
                                "or `service` in `nice service`, a filler, negation or relational word "
                                "such as `with`, `not` or `than`, a number or its unit such as `500` or "
                                "`stars`, a word naming one of the collection's `attributes`, or a "
                                "generic word for whatever is being searched, such as `restaurants`, "
                                "`places` or `items`"}});
        }
    }

    if(ask_word_numbers) {
        static const std::vector<std::string> spelled = {
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
            "20", "30", "40", "50", "60", "70", "80", "90", "100", "1000"};

        std::vector<std::pair<std::string, std::string>> numeral_options;
        for(const auto& digits : spelled) {
            numeral_options.emplace_back(digits, "the word spells out exactly " + digits);
        }
        numeral_options.emplace_back(JevClient::NONE_OPTION,
                                     "the word does not spell out a number on its own, or it is only "
                                     "part of a larger spelled out amount like `five hundred`");

        for(size_t i = 0; i < facts.tokens.size(); i++) {
            questions[token_number_id(i)] = JevClient::choice_question(
                    "If the word `" + facts.tokens[i] +
                    "` spells out an exact number in the query, which is it?", numeral_options);
        }
    }

    return Option<nlohmann::json>(questions);
}

namespace {
    struct clause_t {
        std::string field;
        std::string expr;
        std::vector<std::string> terms;
        bool alternative = false;
        double confidence = 0;
        size_t order = 0;
    };
}

static std::string filter_from_clauses(const nlohmann::json& clauses) {
    std::string all;
    std::string any;
    size_t any_count = 0;

    for(const auto& clause : clauses) {
        const std::string expr = clause["expr"].get<std::string>();
        if(clause.value("alt", false)) {
            if(!any.empty()) {
                any += " || ";
            }
            any += expr;
            any_count++;
        } else {
            if(!all.empty()) {
                all += " && ";
            }
            all += expr;
        }
    }

    if(any_count > 0) {
        if(!all.empty()) {
            all += " && ";
        }
        all += any_count == 1 ? any : "(" + any + ")";
    }

    return all;
}

static void add_consumed_terms(const std::vector<std::string>& terms,
                               std::unordered_set<std::string>& consumed) {
    auto stemmer = StemmerManager::get_instance().get_stemmer("en");
    for(const auto& term : terms) {
        consumed.insert(term);
        if(stemmer != nullptr) {
            consumed.insert(stemmer->stem(term));
        }
    }
}

static bool term_is_consumed(const std::string& word, const std::unordered_set<std::string>& consumed) {
    if(consumed.count(word) != 0) {
        return true;
    }

    auto stemmer = StemmerManager::get_instance().get_stemmer("en");
    if(stemmer != nullptr && consumed.count(stemmer->stem(word)) != 0) {
        return true;
    }

    for(const auto& term : consumed) {
        // term as prefix covers abbreviated values like `Mon`, the reverse needs 4+ chars to keep `bar` out of `Barcelona`
        if(term.size() >= 3 && word.size() > term.size() && word.compare(0, term.size(), term) == 0) {
            return true;
        }
        if(word.size() >= 4 && term.size() > word.size() && term.compare(0, word.size(), word) == 0) {
            return true;
        }
    }

    return false;
}

static bool token_is_consumed(const std::string& token, const std::unordered_set<std::string>& consumed) {
    std::vector<std::string> words;
    collect_terms(token, words);
    if(words.empty()) {
        return false;
    }

    std::string joined;
    for(const auto& word : words) {
        if(term_is_consumed(word, consumed)) {
            return true;
        }
        joined += word;
    }

    return words.size() > 1 && consumed.count(joined) != 0;
}

static bool by_confidence_desc(const clause_t& a, const clause_t& b) {
    return a.confidence > b.confidence;
}

static bool by_order(const clause_t& a, const clause_t& b) {
    return a.order < b.order;
}

static double noul_or(const nlohmann::json& answers, const std::string& id, double fallback) {
    auto noul_op = JevClient::get_noul(answers, id);
    return noul_op.ok() ? noul_op.get() : fallback;
}

static double confidence_or(const nlohmann::json& answers, const std::string& id, double fallback) {
    auto conf_op = JevClient::get_confidence(answers, id);
    return conf_op.ok() ? conf_op.get() : fallback;
}

static double probability_of(const nlohmann::json& answers, const std::string& id, const std::string& option) {
    if(!answers.contains(id) || !answers[id].is_object() ||
       !answers[id].contains("probabilities") || !answers[id]["probabilities"].is_object()) {
        return -1;
    }
    const auto it = answers[id]["probabilities"].find(option);
    return it != answers[id]["probabilities"].end() && it->is_number() ? it->get<double>() : 0.0;
}

// argmax only, scraping the distribution smuggled second values in from confused splits
static bool selected_value(const nlohmann::json& answers, const std::string& id,
                           const std::vector<std::string>& allowed,
                           std::string& value, double& probability) {
    auto choice_op = JevClient::get_choice(answers, id);
    if(!choice_op.ok() || choice_op.get() == JevClient::NONE_OPTION) {
        return false;
    }

    if(std::find(allowed.begin(), allowed.end(), choice_op.get()) == allowed.end()) {
        return false;
    }

    value = choice_op.get();
    probability = probability_of(answers, id, value);
    if(probability < 0) {
        probability = confidence_or(answers, id, 1.0);
    }
    return true;
}

static std::string join_values(const std::vector<std::string>& values) {
    std::string joined;
    for(size_t i = 0; i < values.size(); i++) {
        if(i > 0) {
            joined += ",";
        }
        joined += values[i];
    }
    return joined;
}

static bool numeric_literal_fits(const std::string& literal, const jev_numeric_field_t& numeric_field) {
    field f(numeric_field.name, numeric_field.type, false);
    return filter::validate_numerical_filter_value(f, literal).ok();
}

static void add_facet_clauses(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                             const nlohmann::json& answers,
                             const jev_options_t& opts, std::vector<clause_t>& clauses,
                             nlohmann::json& trace) {
    for(size_t i = 0; i < catalog.facet_fields.size(); i++) {
        const auto& facet_field = catalog.facet_fields[i];

        nlohmann::json entry;
        entry["stage"] = "facet";
        entry["field"] = facet_field.name;

        const double present = noul_or(answers, JevSearchParams::facet_present_id(i), 0.0);
        entry["present"] = present;
        if(present < opts.present_threshold) {
            entry["outcome"] = "skipped, present below threshold";
            trace.push_back(entry);
            continue;
        }

        std::string primary;
        double mass = 0;
        if(!selected_value(answers, JevSearchParams::facet_value_id(i), facet_field.values, primary, mass)) {
            entry["outcome"] = "skipped, no value selected";
            trace.push_back(entry);
            continue;
        }

        entry["mass"] = mass;
        if(mass < opts.confidence_threshold) {
            entry["values"] = {primary};
            entry["outcome"] = "skipped, mass below confidence threshold";
            trace.push_back(entry);
            continue;
        }

        const bool negate = noul_or(answers, JevSearchParams::facet_negate_id(i), 0.0) >= opts.present_threshold;
        entry["negate"] = negate;

        // excluding on a weak signal hurts worse than missing a clause, negation demands conviction
        if(negate && mass < opts.also_bound_threshold) {
            entry["values"] = {primary};
            entry["outcome"] = "skipped, negated without conviction";
            trace.push_back(entry);
            continue;
        }

        // the second value must differ from the primary and clear a higher bar under negation
        std::string second;
        double second_probability = 0;
        if(selected_value(answers, JevSearchParams::facet_value2_id(i), facet_field.values,
                          second, second_probability)) {
            const double second_bar = negate ? opts.also_bound_threshold : opts.second_value_threshold;
            if(second == primary || second_probability < second_bar) {
                entry["second_rejected"] = second;
                entry["second_probability"] = second_probability;
                second.clear();
            } else {
                entry["second_value"] = second;
                entry["second_probability"] = second_probability;
            }
        }

        std::vector<std::string> selected = {primary};
        if(!second.empty()) {
            selected.push_back(second);
        }
        entry["values"] = selected;

        std::vector<std::string> escaped;
        for(const auto& value : selected) {
            const std::string token = JevSearchParams::escape_filter_value(value);
            if(!token.empty()) {
                escaped.push_back(token);
            }
        }

        if(escaped.empty()) {
            entry["outcome"] = "skipped, values escaped to nothing";
            trace.push_back(entry);
            continue;
        }

        // negation collapses the and-or distinction, excluding two values means neither may appear
        const bool both = escaped.size() == 2 && !negate &&
                          noul_or(answers, JevSearchParams::facet_both_id(i), 0.0) >= opts.present_threshold;
        entry["both"] = both;

        clause_t clause;
        clause.field = facet_field.name;
        // exact match, jev selected a catalog value and bare `:` would token match `York` against `New York`
        if(both) {
            // one parenthesized clause keeps the pair intact through the cap, pruning and the or group
            clause.expr = "(" + facet_field.name + ":=" + escaped[0] + " && " +
                          facet_field.name + ":=" + escaped[1] + ")";
        } else {
            clause.expr = facet_field.name + (negate ? ":!=" : ":=") +
                          (escaped.size() == 1 ? escaped[0] : "[" + join_values(escaped) + "]");
        }
        clause.alternative = facts.has_or &&
                             noul_or(answers, JevSearchParams::facet_alt_id(i), 0.0) >= opts.present_threshold;
        clause.confidence = std::min(present, escaped.size() == 2 ? std::min(mass, second_probability) : mass);
        clause.order = clauses.size();
        collect_terms(facet_field.name, clause.terms);
        for(const auto& value : selected) {
            collect_terms(value, clause.terms);
        }
        clauses.push_back(clause);

        entry["outcome"] = "clause";
        entry["expr"] = clause.expr;
        entry["alt"] = clause.alternative;
        trace.push_back(entry);
    }
}
static void add_numeric_clauses(const jev_catalog_t& catalog,
                                const jev_query_facts_t& facts, const nlohmann::json& answers,
                                const jev_options_t& opts, std::vector<clause_t>& clauses,
                                nlohmann::json& trace) {
    if(catalog.numeric_fields.empty()) {
        return;
    }

    for(size_t k = 0; k < facts.numbers.size(); k++) {
        nlohmann::json entry;
        entry["stage"] = "number";
        entry["literal"] = facts.numbers[k];
        entry["span"] = facts.number_spans[k];

        auto field_choice = JevClient::get_choice(answers, JevSearchParams::number_field_id(k));
        if(!field_choice.ok() || field_choice.get() == JevClient::NONE_OPTION) {
            entry["outcome"] = field_choice.ok() ? "skipped, bound to no field" : "skipped, no field answer";
            trace.push_back(entry);
            continue;
        }
        entry["owner"] = field_choice.get();

        const jev_numeric_field_t* numeric_field = nullptr;
        for(const auto& candidate : catalog.numeric_fields) {
            if(candidate.name == field_choice.get()) {
                numeric_field = &candidate;
                break;
            }
        }
        if(numeric_field == nullptr) {
            entry["outcome"] = "skipped, owner not in catalog";
            trace.push_back(entry);
            continue;
        }

        double none_probability = 0.0;
        double owner_probability = 1.0;
        const auto& field_answer = answers[JevSearchParams::number_field_id(k)];
        if(field_answer.contains("probabilities") && field_answer["probabilities"].is_object()) {
            const auto none_it = field_answer["probabilities"].find(JevClient::NONE_OPTION);
            if(none_it != field_answer["probabilities"].end() && none_it->is_number()) {
                none_probability = none_it->get<double>();
            }
            const auto owner_it = field_answer["probabilities"].find(numeric_field->name);
            if(owner_it != field_answer["probabilities"].end() && owner_it->is_number()) {
                owner_probability = owner_it->get<double>();
            }
        }
        entry["owner_probability"] = owner_probability;
        entry["none_probability"] = none_probability;
        if(none_probability >= opts.confidence_threshold) {
            entry["outcome"] = "skipped, none probability above confidence threshold";
            trace.push_back(entry);
            continue;
        }

        auto op_choice = JevClient::get_choice(answers, JevSearchParams::number_op_id(k));
        if(!op_choice.ok()) {
            entry["outcome"] = "skipped, no op answer";
            trace.push_back(entry);
            continue;
        }
        entry["op"] = op_choice.get();

        const double op_confidence = confidence_or(answers, JevSearchParams::number_op_id(k), 1.0);
        entry["op_confidence"] = op_confidence;
        if(op_confidence < opts.confidence_threshold) {
            entry["outcome"] = "skipped, op below confidence threshold";
            trace.push_back(entry);
            continue;
        }

        if(!numeric_literal_fits(facts.numbers[k], *numeric_field)) {
            entry["outcome"] = "skipped, literal does not fit the field type";
            trace.push_back(entry);
            continue;
        }

        const bool alternative = facts.has_or &&
                                 noul_or(answers, JevSearchParams::number_alt_id(k), 0.0) >= opts.present_threshold;

        const std::string& op = op_choice.get();
        std::string comparator;
        if(op == "eq") {
            comparator = "";
        } else if(op == "gt") {
            comparator = ">";
        } else if(op == "gte") {
            comparator = ">=";
        } else if(op == "lt") {
            comparator = "<";
        } else if(op == "lte") {
            comparator = "<=";
        } else {
            entry["outcome"] = "skipped, unknown op";
            trace.push_back(entry);
            continue;
        }

        clause_t clause;
        clause.field = numeric_field->name;
        clause.expr = numeric_field->name + ":" + comparator + facts.numbers[k];
        clause.alternative = alternative;
        clause.confidence = std::min(owner_probability, op_confidence);
        clause.order = clauses.size();
        collect_terms(numeric_field->name, clause.terms);
        clause.terms.push_back(facts.numbers[k]);
        if(facts.number_spans[k].find_first_of("0123456789") == std::string::npos) {
            collect_terms(facts.number_spans[k], clause.terms);
        }
        clauses.push_back(clause);

        entry["outcome"] = "clause";
        entry["expr"] = clause.expr;
        entry["alt"] = alternative;

        nlohmann::json also_json = nlohmann::json::array();
        for(size_t i = 0; i < catalog.numeric_fields.size(); i++) {
            const auto& also_field = catalog.numeric_fields[i];
            if(also_field.name == numeric_field->name) {
                continue;
            }

            const double also = noul_or(answers, JevSearchParams::number_also_id(k, i), 0.0);
            const bool taken = also >= opts.also_bound_threshold &&
                               numeric_literal_fits(facts.numbers[k], also_field);
            also_json.push_back({{"field", also_field.name}, {"noul", also}, {"taken", taken}});
            if(!taken) {
                continue;
            }

            clause_t also_clause;
            also_clause.field = also_field.name;
            also_clause.expr = also_field.name + ":" + comparator + facts.numbers[k];
            also_clause.alternative = alternative;
            also_clause.confidence = std::min(also, op_confidence);
            also_clause.order = clauses.size();
            collect_terms(also_field.name, also_clause.terms);
            also_clause.terms.push_back(facts.numbers[k]);
            if(facts.number_spans[k].find_first_of("0123456789") == std::string::npos) {
                collect_terms(facts.number_spans[k], also_clause.terms);
            }
            clauses.push_back(also_clause);
        }

        entry["also"] = also_json;
        trace.push_back(entry);
    }
}

static void add_bool_clauses(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                            const nlohmann::json& answers,
                            const jev_options_t& opts, std::vector<clause_t>& clauses,
                            nlohmann::json& trace) {
    for(size_t i = 0; i < catalog.bool_fields.size(); i++) {
        nlohmann::json entry;
        entry["stage"] = "bool";
        entry["field"] = catalog.bool_fields[i];

        auto choice_op = JevClient::get_choice(answers, JevSearchParams::bool_id(i));
        if(!choice_op.ok()) {
            entry["outcome"] = "skipped, no answer";
            trace.push_back(entry);
            continue;
        }

        const std::string& choice = choice_op.get();
        entry["choice"] = choice;
        if(choice != "true" && choice != "false") {
            entry["outcome"] = "skipped, unspecified";
            trace.push_back(entry);
            continue;
        }

        const double confidence = confidence_or(answers, JevSearchParams::bool_id(i), 1.0);
        entry["confidence"] = confidence;
        if(confidence < opts.confidence_threshold) {
            entry["outcome"] = "skipped, below confidence threshold";
            trace.push_back(entry);
            continue;
        }

        clause_t clause;
        clause.field = catalog.bool_fields[i];
        clause.expr = catalog.bool_fields[i] + ":" + choice;
        clause.alternative = facts.has_or &&
                             noul_or(answers, JevSearchParams::bool_alt_id(i), 0.0) >= opts.present_threshold;
        clause.confidence = confidence;
        clause.order = clauses.size();
        collect_terms(catalog.bool_fields[i], clause.terms);
        clauses.push_back(clause);

        entry["outcome"] = "clause";
        entry["expr"] = clause.expr;
        entry["alt"] = clause.alternative;
        trace.push_back(entry);
    }
}

static bool by_pick_desc(const std::pair<double, size_t>& a, const std::pair<double, size_t>& b) {
    return a.first > b.first;
}

static bool sort_dir_for(const nlohmann::json& answers, size_t idx, std::string& dir) {
    auto dir_op = JevClient::get_choice(answers, JevSearchParams::sort_dir_id(idx));
    if(!dir_op.ok() || (dir_op.get() != "asc" && dir_op.get() != "desc")) {
        return false;
    }
    dir = dir_op.get();
    return true;
}

static std::string build_sort_by(const jev_catalog_t& catalog, const nlohmann::json& answers,
                                 const jev_options_t& opts, double& confidence,
                                 nlohmann::json& trace) {
    if(catalog.sort_fields.empty()) {
        return "";
    }

    nlohmann::json entry;
    entry["stage"] = "sort";

    auto field_op = JevClient::get_choice(answers, JevSearchParams::SORT_FIELD_ID);
    if(!field_op.ok() || field_op.get() == JevClient::NONE_OPTION) {
        entry["outcome"] = field_op.ok() ? "skipped, no ordering wanted" : "skipped, no answer";
        if(field_op.ok() && answers[JevSearchParams::SORT_FIELD_ID].contains("probabilities")) {
            entry["probabilities"] = answers[JevSearchParams::SORT_FIELD_ID]["probabilities"];
        }
        trace.push_back(entry);
        return "";
    }
    entry["choice"] = field_op.get();

    std::vector<std::pair<double, size_t>> picks;
    const auto& answer = answers[JevSearchParams::SORT_FIELD_ID];
    if(answer.contains("probabilities") && answer["probabilities"].is_object()) {
        for(size_t i = 0; i < catalog.sort_fields.size(); i++) {
            const auto it = answer["probabilities"].find(catalog.sort_fields[i]);
            if(it != answer["probabilities"].end() && it->is_number() &&
               it->get<double>() >= opts.or_min_probability) {
                picks.emplace_back(it->get<double>(), i);
            }
        }
    }

    if(picks.empty()) {
        for(size_t i = 0; i < catalog.sort_fields.size(); i++) {
            if(catalog.sort_fields[i] == field_op.get()) {
                picks.emplace_back(1.0, i);
                break;
            }
        }
    }
    if(picks.empty()) {
        return "";
    }

    std::stable_sort(picks.begin(), picks.end(), by_pick_desc);
    if(picks.size() > opts.max_sort_fields) {
        picks.resize(opts.max_sort_fields);
    }

    double mass = 0;
    std::string sort_expr;
    nlohmann::json picks_json = nlohmann::json::array();
    for(const auto& [probability, idx] : picks) {
        std::string dir;
        if(!sort_dir_for(answers, idx, dir)) {
            picks_json.push_back({{"field", catalog.sort_fields[idx]}, {"probability", probability},
                                  {"taken", false}});
            continue;
        }
        picks_json.push_back({{"field", catalog.sort_fields[idx]}, {"probability", probability},
                              {"dir", dir}, {"taken", true}});
        if(!sort_expr.empty()) {
            sort_expr += ",";
        }
        sort_expr += catalog.sort_fields[idx] + ":" + dir;
        mass += probability;
    }
    entry["picks"] = picks_json;
    if(sort_expr.empty()) {
        entry["outcome"] = "skipped, no usable direction";
        trace.push_back(entry);
        return "";
    }

    std::vector<sort_by> parsed;
    if(!CollectionManager::parse_sort_by_str(sort_expr, parsed) || parsed.empty()) {
        entry["outcome"] = "skipped, sort expression failed to parse";
        trace.push_back(entry);
        return "";
    }

    confidence = std::min(mass, 1.0);
    entry["outcome"] = "sort";
    entry["expr"] = sort_expr;
    trace.push_back(entry);
    return sort_expr;
}

static std::string token_role(const nlohmann::json& answers, const std::string& id, double threshold) {
    auto choice_op = JevClient::get_choice(answers, id);
    if(!choice_op.ok()) {
        return "";
    }

    const double p_content = probability_of(answers, id, "content");
    if(p_content < 0) {
        const std::string& choice = choice_op.get();
        return choice == "content" || choice == "excluded" || choice == "filler" ? choice : "";
    }

    if(p_content >= threshold) {
        return "content";
    }
    if(probability_of(answers, id, "excluded") >= threshold) {
        return "excluded";
    }
    return "filler";
}

static std::string build_q(const jev_query_facts_t& facts, const nlohmann::json& answers,
                           const jev_options_t& opts, const std::unordered_set<std::string>& consumed,
                           nlohmann::json& trace) {
    nlohmann::json entry;
    entry["stage"] = "q";

    if(facts.has_query_operators) {
        entry["outcome"] = "raw query kept, phrase or exclusion operators present";
        trace.push_back(entry);
        return facts.query;
    }

    if(facts.tokens.size() > opts.max_token_questions && !consumed.empty()) {
        entry["outcome"] = "token questions over cap, filters stand, q dropped to *";
        trace.push_back(entry);
        return "*";
    }

    std::vector<std::string> roles(facts.tokens.size(), "content");

    if(!facts.tokens.empty() && facts.tokens.size() <= opts.max_token_questions) {
        bool have_all = true;
        for(size_t i = 0; i < facts.tokens.size() && have_all; i++) {
            roles[i] = token_role(answers, JevSearchParams::token_id(i), opts.token_threshold);
            have_all = !roles[i].empty();
        }

        if(!have_all) {
            roles.assign(facts.tokens.size(), "content");
        }
    }

    nlohmann::json tokens_json = nlohmann::json::array();
    std::string rewritten;
    std::string exclusions;
    for(size_t i = 0; i < facts.tokens.size(); i++) {
        const bool eaten = token_is_consumed(facts.tokens[i], consumed);
        const bool in_q = roles[i] == "content" && !eaten;

        nlohmann::json token_json = {{"token", facts.tokens[i]}, {"consumed", eaten},
                                     {"role", roles[i]}, {"in_q", in_q}};
        tokens_json.push_back(token_json);

        if(roles[i] == "excluded" && !eaten) {
            exclusions += " -" + facts.tokens[i];
            continue;
        }
        if(!in_q) {
            continue;
        }
        if(!rewritten.empty()) {
            rewritten += " ";
        }
        rewritten += facts.tokens[i];
    }
    entry["tokens"] = tokens_json;

    if(!rewritten.empty()) {
        entry["outcome"] = rewritten + exclusions;
        trace.push_back(entry);
        return rewritten + exclusions;
    }

    if(!exclusions.empty()) {
        entry["outcome"] = "*" + exclusions;
        trace.push_back(entry);
        return "*" + exclusions;
    }

    entry["outcome"] = consumed.empty() ? "raw query kept, nothing survived and nothing filters" : "*";
    trace.push_back(entry);
    return consumed.empty() ? facts.query : "*";
}

nlohmann::json JevSearchParams::assemble(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                         const nlohmann::json& answers, const jev_options_t& opts) {
    nlohmann::json trace = nlohmann::json::array();

    std::vector<clause_t> clauses;
    add_facet_clauses(catalog, facts, answers, opts, clauses, trace);
    add_numeric_clauses(catalog, facts, answers, opts, clauses, trace);
    add_bool_clauses(catalog, facts, answers, opts, clauses, trace);

    std::unordered_set<std::string> seen_exprs;
    std::vector<clause_t> unique_clauses;
    for(auto& clause : clauses) {
        if(seen_exprs.insert(clause.expr).second) {
            unique_clauses.push_back(std::move(clause));
        }
    }
    clauses = std::move(unique_clauses);

    if(clauses.size() > opts.max_clauses) {
        std::stable_sort(clauses.begin(), clauses.end(), by_confidence_desc);
        clauses.resize(opts.max_clauses);
        std::stable_sort(clauses.begin(), clauses.end(), by_order);
        trace.push_back({{"stage", "cap"}, {"outcome", "clauses truncated to the most confident"},
                         {"kept", opts.max_clauses}});
    }

    // confidence reports the least certain judgment that was used, not the product of all of them
    double confidence = 1.0;

    std::unordered_set<std::string> consumed;
    nlohmann::json clauses_json = nlohmann::json::array();
    for(const auto& clause : clauses) {
        confidence = std::min(confidence, clause.confidence);
        add_consumed_terms(clause.terms, consumed);
        clauses_json.push_back({{"field", clause.field}, {"expr", clause.expr},
                                {"alt", clause.alternative}, {"confidence", clause.confidence}});
    }

    const std::string filter_by = filter_from_clauses(clauses_json);

    double sort_confidence = 1.0;
    const std::string sort_by = build_sort_by(catalog, answers, opts, sort_confidence, trace);
    if(!sort_by.empty()) {
        confidence = std::min(confidence, sort_confidence);

        // field names only, a consumed `desc` would delete words like `description` through the prefix rule
        std::vector<std::string> sort_parts;
        StringUtils::split(sort_by, sort_parts, ",");

        std::vector<std::string> sort_terms;
        for(const auto& part : sort_parts) {
            collect_terms(part.substr(0, part.find(':')), sort_terms);
        }
        add_consumed_terms(sort_terms, consumed);
    }

    nlohmann::json params;
    params["q"] = build_q(facts, answers, opts, consumed, trace);
    if(!filter_by.empty()) {
        params["filter_by"] = filter_by;
    }
    if(!sort_by.empty()) {
        params["sort_by"] = sort_by;
    }

    std::vector<std::string> consumed_sorted(consumed.begin(), consumed.end());
    std::sort(consumed_sorted.begin(), consumed_sorted.end());

    nlohmann::json debug;
    debug["facts"] = {
        {"query", facts.query},
        {"tokens", facts.tokens},
        {"numbers", facts.numbers},
        {"number_spans", facts.number_spans},
        {"has_or", facts.has_or},
        {"has_query_operators", facts.has_query_operators}
    };
    debug["consumed_terms"] = consumed_sorted;
    debug["trace"] = trace;

    params["llm_response"] = {
        {"content", answers.dump()},
        {"model", ""},
        // read back by the augment step, jev's q is executed verbatim while llm q rewrites are not
        {"provider", "jev"},
        {"confidence", confidence},
        {"clauses", clauses_json},
        {"debug", debug}
    };

    return params;
}

static void prune_invalid_clauses(const std::string& collection_name, const jev_query_facts_t& facts,
                                  nlohmann::json& params) {
    if(!params.contains("filter_by")) {
        return;
    }

    auto collection = CollectionManager::get_instance().get_collection(collection_name);
    if(collection == nullptr) {
        return;
    }

    Collection* coll = collection.get();
    const auto search_schema = coll->get_schema();
    const std::string doc_id_prefix = std::to_string(coll->get_collection_id()) + "_" +
                                      Collection::DOC_ID_PREFIX + "_";
    Store* store = CollectionManager::get_instance().get_store();

    nlohmann::json& clauses = params["llm_response"]["clauses"];

    for(size_t i = 0; i < clauses.size();) {
        filter_node_t* root = nullptr;
        auto parse_op = filter::parse_filter_query(clauses[i]["expr"].get<std::string>(), search_schema,
                                                   store, doc_id_prefix, root);
        std::unique_ptr<filter_node_t> guard(root);

        if(parse_op.ok()) {
            i++;
            continue;
        }

        LOG(ERROR) << "Dropping Jev filter clause `" << clauses[i]["expr"].get<std::string>()
                   << "` on `" << collection_name << "`: " << parse_op.error();
        params["llm_response"]["debug"]["trace"].push_back(
                {{"stage", "prune"}, {"expr", clauses[i]["expr"]},
                 {"outcome", "dropped, " + parse_op.error()}});
        clauses.erase(i);
    }

    std::string filter_by = filter_from_clauses(clauses);

    if(!filter_by.empty()) {
        filter_node_t* root = nullptr;
        auto parse_op = filter::parse_filter_query(filter_by, search_schema, store, doc_id_prefix, root);
        std::unique_ptr<filter_node_t> guard(root);

        if(!parse_op.ok()) {
            LOG(ERROR) << "Dropping Jev filter `" << filter_by << "` on `" << collection_name
                       << "`: " << parse_op.error();
            params["llm_response"]["debug"]["trace"].push_back(
                    {{"stage", "prune"}, {"expr", filter_by},
                     {"outcome", "whole filter dropped, " + parse_op.error()}});
            clauses.clear();
            filter_by.clear();
        }
    }

    if(filter_by.empty()) {
        params.erase("filter_by");
        // the clauses already consumed their words out of q, the raw query is the only honest fallback
        params["q"] = facts.query;
        return;
    }

    params["filter_by"] = filter_by;
}

static Option<bool> read_threshold(const nlohmann::json& config, const char* key, double& out) {
    if(config.count(key) == 0) {
        return Option<bool>(true);
    }
    if(!config[key].is_number() || config[key].get<double>() < 0.0 || config[key].get<double>() > 1.0) {
        return Option<bool>(400, "Property `" + std::string(key) + "` must be a number between 0 and 1.");
    }
    out = config[key].get<double>();
    return Option<bool>(true);
}

static Option<bool> read_count(const nlohmann::json& config, const char* key,
                               size_t min_value, size_t max_value, size_t& out) {
    if(config.count(key) == 0) {
        return Option<bool>(true);
    }
    if(!config[key].is_number_unsigned() || config[key].get<size_t>() < min_value ||
       config[key].get<size_t>() > max_value) {
        return Option<bool>(400, "Property `" + std::string(key) + "` must be an integer between " +
                                 std::to_string(min_value) + " and " + std::to_string(max_value) + ".");
    }
    out = config[key].get<size_t>();
    return Option<bool>(true);
}

static Option<bool> read_flag(const nlohmann::json& config, const char* key, bool& out) {
    if(config.count(key) == 0) {
        return Option<bool>(true);
    }
    if(!config[key].is_boolean()) {
        return Option<bool>(400, "Property `" + std::string(key) + "` must be a boolean.");
    }
    out = config[key].get<bool>();
    return Option<bool>(true);
}

Option<bool> JevSearchParams::options_from_config(const nlohmann::json& model_config, jev_options_t& opts) {
    const std::vector<std::pair<const char*, double*>> thresholds = {
        {"present_threshold", &opts.present_threshold},
        {"confidence_threshold", &opts.confidence_threshold},
        {"or_min_probability", &opts.or_min_probability},
        {"second_value_threshold", &opts.second_value_threshold},
        {"token_threshold", &opts.token_threshold},
        {"also_bound_threshold", &opts.also_bound_threshold},
    };
    for(const auto& [key, out] : thresholds) {
        auto op = read_threshold(model_config, key, *out);
        if(!op.ok()) {
            return op;
        }
    }

    const std::vector<std::tuple<const char*, size_t, size_t, size_t*>> counts = {
        // one slot below the client's 255 option cap keeps the appended __none__ from truncating away
        {"max_options_per_question", 2, JevClient::MAX_CHOICE_OPTIONS - 1, &opts.max_options_per_question},
        {"max_questions", 1, 10000, &opts.max_questions},
        {"max_token_questions", 0, 255, &opts.max_token_questions},
        // n anded clauses cost 2n-1 postfix tokens against filter_by_max_ops
        {"max_clauses", 1, 50, &opts.max_clauses},
        {"max_sort_fields", 1, 3, &opts.max_sort_fields},
        {"max_word_number_literals", 0, 8, &opts.max_word_number_literals},
        {"total_timeout_ms", JevClient::MIN_TIMEOUT_MS, JevClient::MAX_TOTAL_TIMEOUT_MS, &opts.total_timeout_ms},
    };
    for(const auto& [key, min_value, max_value, out] : counts) {
        auto op = read_count(model_config, key, min_value, max_value, *out);
        if(!op.ok()) {
            return op;
        }
    }

    const std::vector<std::pair<const char*, bool*>> flags = {
        {"word_numbers", &opts.word_numbers},
        {"consumed_check", &opts.consumed_check},
    };
    for(const auto& [key, out] : flags) {
        auto op = read_flag(model_config, key, *out);
        if(!op.ok()) {
            return op;
        }
    }

    return Option<bool>(true);
}

static bool word_number_hits(const nlohmann::json& answers, const jev_options_t& opts,
                             jev_query_facts_t& facts, nlohmann::json& trace_entry) {
    nlohmann::json hits = nlohmann::json::array();

    for(size_t i = 0; i < facts.tokens.size(); i++) {
        if(facts.numbers.size() >= opts.max_word_number_literals) {
            break;
        }

        const std::string id = JevSearchParams::token_number_id(i);
        auto choice_op = JevClient::get_choice(answers, id);
        if(!choice_op.ok() || choice_op.get() == JevClient::NONE_OPTION) {
            continue;
        }

        double probability = probability_of(answers, id, choice_op.get());
        if(probability < 0) {
            probability = confidence_or(answers, id, 1.0);
        }
        if(probability < opts.confidence_threshold) {
            continue;
        }

        std::string context;
        for(size_t j = i >= 2 ? i - 2 : 0; j < facts.tokens.size() && j <= i + 2; j++) {
            if(!context.empty()) {
                context += " ";
            }
            context += facts.tokens[j];
        }

        facts.numbers.push_back(choice_op.get());
        facts.number_spans.push_back(facts.tokens[i]);
        facts.number_contexts.push_back(context);
        hits.push_back({{"token", facts.tokens[i]}, {"literal", choice_op.get()},
                        {"probability", probability}});
    }

    trace_entry = {{"stage", "word_numbers"}, {"hits", hits}};
    return !facts.numbers.empty();
}

static nlohmann::json consumed_question(const std::string& word) {
    return JevClient::noul_question(
            "Is the word `" + word + "` from the query already expressed by the search's `filters` "
            "and `ordering`, rather than extra text a matching result should contain?",
            "the word's meaning is already carried by the filters or ordering, as a filtered value, "
            "the same thing in another spelling or language, or the attribute being constrained",
            "the word adds meaning the filters do not cover, the result text itself should contain it");
}

static void consumed_check_round(const std::string& context, const nlohmann::json& model_config,
                                 const jev_query_facts_t& facts, const jev_options_t& opts,
                                 nlohmann::json& params, long remaining_budget_ms,
                                 std::vector<jev_round_stats_t>& round_stats) {
    if(!params.contains("filter_by") && !params.contains("sort_by")) {
        return;
    }

    const std::string q = params["q"].get<std::string>();
    if(q.empty() || q == "*") {
        return;
    }

    std::vector<std::string> q_tokens;
    StringUtils::split(q, q_tokens, " ");

    std::vector<size_t> positives;
    for(size_t i = 0; i < q_tokens.size(); i++) {
        if(q_tokens[i] != "*" && q_tokens[i][0] != '-') {
            positives.push_back(i);
        }
    }
    if(positives.empty()) {
        return;
    }

    if(remaining_budget_ms < (long)JevClient::MIN_TIMEOUT_MS) {
        params["llm_response"]["debug"]["trace"].push_back(
                {{"stage", "consumed_check"}, {"outcome", "skipped, time budget exhausted"}});
        return;
    }

    nlohmann::json questions = nlohmann::json::object();
    for(size_t i = 0; i < positives.size(); i++) {
        questions["c" + std::to_string(i)] = consumed_question(q_tokens[positives[i]]);
    }

    nlohmann::json state;
    state["query"] = facts.query;
    state["filters"] = params.contains("filter_by") ? params["filter_by"].get<std::string>() : "";
    state["ordering"] = params.contains("sort_by") ? params["sort_by"].get<std::string>() : "";
    if(!context.empty()) {
        state["context"] = context;
    }

    jev_round_stats_t stats;
    stats.purpose = "consumed_check";
    auto response_op = JevClient::ask(state, questions, model_config, &stats, remaining_budget_ms);
    round_stats.push_back(stats);

    if(!response_op.ok() || !response_op.get().contains("answers") ||
       !response_op.get()["answers"].is_object()) {
        LOG(WARNING) << "jev consumed check round failed, q stands: "
                     << (response_op.ok() ? "malformed answers" : response_op.error());
        return;
    }

    // get() returns by value, binding a reference into the temporary would dangle
    const nlohmann::json answers = response_op.get()["answers"];
    std::vector<bool> keep(q_tokens.size(), true);
    nlohmann::json tokens_json = nlohmann::json::array();
    for(size_t i = 0; i < positives.size(); i++) {
        const double noul = noul_or(answers, "c" + std::to_string(i), 0.0);
        const bool dropped = noul >= opts.token_threshold;
        keep[positives[i]] = !dropped;
        tokens_json.push_back({{"token", q_tokens[positives[i]]}, {"noul", noul},
                               {"dropped", dropped}});
    }

    std::string rewritten;
    bool has_positive = false;
    for(size_t i = 0; i < q_tokens.size(); i++) {
        if(!keep[i]) {
            continue;
        }
        if(q_tokens[i] != "*" && q_tokens[i][0] != '-') {
            has_positive = true;
        }
        if(!rewritten.empty()) {
            rewritten += " ";
        }
        rewritten += q_tokens[i];
    }
    if(!has_positive) {
        rewritten = rewritten.empty() ? "*" : "* " + rewritten;
    }

    params["q"] = rewritten;
    params["llm_response"]["debug"]["trace"].push_back(
            {{"stage", "consumed_check"}, {"tokens", tokens_json}, {"outcome", rewritten}});
    params["llm_response"]["debug"]["questions"].update(questions);
}

Option<nlohmann::json> JevSearchParams::generate(const std::string& query,
                                                 const std::string& collection_name,
                                                 const nlohmann::json& model_config,
                                                 const schema_prompt_params_t& prompt_params,
                                                 const std::string& scope_filter,
                                                 const jev_options_t& base_opts) {
    const auto t_start = std::chrono::steady_clock::now();

    jev_options_t opts = base_opts;
    auto opts_op = options_from_config(model_config, opts);
    if(!opts_op.ok()) {
        return Option<nlohmann::json>(opts_op.code(), opts_op.error());
    }

    auto catalog_op = build_catalog(collection_name, prompt_params, scope_filter);
    if(!catalog_op.ok()) {
        return Option<nlohmann::json>(catalog_op.code(), catalog_op.error());
    }

    const long ms_catalog = elapsed_ms(t_start);
    const auto t_questions = std::chrono::steady_clock::now();

    const jev_catalog_t catalog = catalog_op.get();
    const jev_query_facts_t facts = pre_parse(query);

    auto questions_op = build_questions(catalog, facts, opts);
    if(!questions_op.ok()) {
        return Option<nlohmann::json>(questions_op.code(), questions_op.error());
    }

    const long ms_questions = elapsed_ms(t_questions);

    if(questions_op.get().empty()) {
        nlohmann::json params;
        params["q"] = query;
        params["llm_response"] = {{"content", "{}"}, {"model", ""}, {"provider", "jev"},
                                  {"confidence", 1.0}, {"clauses", nlohmann::json::array()}};
        return Option<nlohmann::json>(params);
    }

    const std::string context = model_config.contains("system_prompt") && model_config["system_prompt"].is_string() ?
                                model_config["system_prompt"].get<std::string>() : "";
    const nlohmann::json state = build_state(facts, catalog, context);

    const long per_call_timeout_ms = model_config.contains("timeout_ms") &&
                                     model_config["timeout_ms"].is_number_unsigned() ?
                                     model_config["timeout_ms"].get<long>() :
                                     (long)JevClient::DEFAULT_TIMEOUT_MS;
    const long total_budget_ms = opts.total_timeout_ms > 0 ? (long)opts.total_timeout_ms
                                                           : 2 * per_call_timeout_ms;

    std::vector<jev_round_stats_t> round_stats;

    jev_round_stats_t main_stats;
    main_stats.purpose = "main";

    // the main round is never skipped, a spent budget still buys it the minimum call timeout
    auto response_op = JevClient::ask(state, questions_op.get(), model_config, &main_stats,
                                      std::max<long>(total_budget_ms - elapsed_ms(t_start),
                                                     (long)JevClient::MIN_TIMEOUT_MS));
    round_stats.push_back(main_stats);

    if(!response_op.ok()) {
        LOG(INFO) << "jev timing: main round failed, status=" << main_stats.status
                  << " api=" << main_stats.wall_ms << "ms total=" << elapsed_ms(t_start)
                  << "ms asked=" << main_stats.num_questions
                  << " req_bytes=" << main_stats.request_bytes;
        return Option<nlohmann::json>(response_op.code(), response_op.error());
    }

    const nlohmann::json response = response_op.get();
    nlohmann::json answers = response.contains("answers") && response["answers"].is_object() ?
                             response["answers"] : nlohmann::json::object();
    nlohmann::json questions = questions_op.get();

    jev_query_facts_t final_facts = facts;
    nlohmann::json word_number_entry = nullptr;
    if(opts.word_numbers && facts.numbers.empty() && !catalog.numeric_fields.empty() &&
       !facts.has_query_operators && !facts.tokens.empty() &&
       facts.tokens.size() <= opts.max_token_questions) {
        nlohmann::json wn_entry;
        if(word_number_hits(answers, opts, final_facts, wn_entry)) {
            if(total_budget_ms - elapsed_ms(t_start) < (long)JevClient::MIN_TIMEOUT_MS) {
                final_facts.numbers.clear();
                final_facts.number_spans.clear();
                wn_entry["outcome"] = "skipped, time budget exhausted";
            } else {
                nlohmann::json binding_questions = nlohmann::json::object();
                add_numeric_questions(catalog, final_facts, opts, binding_questions);
                const nlohmann::json binding_state = build_state(final_facts, catalog, context);

                jev_round_stats_t binding_stats;
                binding_stats.purpose = "word_number_binding";
                // refloored again, a zero or negative cap here would read as no cap at all
                auto binding_op = JevClient::ask(binding_state, binding_questions, model_config,
                                                 &binding_stats,
                                                 std::max<long>(total_budget_ms - elapsed_ms(t_start),
                                                                (long)JevClient::MIN_TIMEOUT_MS));
                round_stats.push_back(binding_stats);

                if(binding_op.ok() && binding_op.get().contains("answers") &&
                   binding_op.get()["answers"].is_object()) {
                    answers.update(binding_op.get()["answers"]);
                    questions.update(binding_questions);
                } else {
                    LOG(WARNING) << "jev word number binding round failed: "
                                 << (binding_op.ok() ? "malformed answers" : binding_op.error());
                    final_facts.numbers.clear();
                    final_facts.number_spans.clear();
                    wn_entry["outcome"] = "binding round failed";
                }
            }
        }
        word_number_entry = wn_entry;
    }

    const auto t_assemble = std::chrono::steady_clock::now();

    nlohmann::json params = assemble(catalog, final_facts, answers, opts);
    if(!word_number_entry.is_null()) {
        params["llm_response"]["debug"]["trace"].push_back(word_number_entry);
    }

    // aliases resolve server side, and a wrong typed field must not throw into a discarded future
    params["llm_response"]["model"] = response.contains("model") && response["model"].is_string() ?
                                      response["model"].get<std::string>() : "";

    nlohmann::json catalog_json;
    catalog_json["numeric_fields"] = nlohmann::json::array();
    for(const auto& numeric_field : catalog.numeric_fields) {
        catalog_json["numeric_fields"].push_back(numeric_field.name);
    }
    catalog_json["facet_fields"] = nlohmann::json::array();
    for(const auto& facet_field : catalog.facet_fields) {
        catalog_json["facet_fields"].push_back({{"name", facet_field.name},
                                                {"num_values", facet_field.values.size()}});
    }
    catalog_json["bool_fields"] = catalog.bool_fields;
    catalog_json["sort_fields"] = catalog.sort_fields;

    nlohmann::json described_json = nlohmann::json::array();
    for(const auto& [field_name, _] : catalog.field_descriptions) {
        described_json.push_back(field_name);
    }
    catalog_json["described_fields"] = described_json;

    params["llm_response"]["debug"]["catalog"] = catalog_json;
    params["llm_response"]["debug"]["context_present"] = !context.empty();
    params["llm_response"]["debug"]["questions"] = questions;

    prune_invalid_clauses(collection_name, final_facts, params);
    const long ms_assemble = elapsed_ms(t_assemble);

    if(opts.consumed_check && !final_facts.has_query_operators) {
        consumed_check_round(context, model_config, final_facts, opts, params,
                             total_budget_ms - elapsed_ms(t_start), round_stats);
    }

    long ms_api = 0;
    long known_input_tokens = 0;
    size_t unknown_usage_rounds = 0;
    size_t failed_rounds = 0;
    size_t num_questions = 0;
    size_t request_bytes = 0;
    nlohmann::json rounds_json = nlohmann::json::array();
    for(const auto& stats : round_stats) {
        ms_api += stats.wall_ms;
        num_questions += stats.num_questions;
        request_bytes += stats.request_bytes;
        if(stats.status != 200) {
            failed_rounds++;
        }
        if(stats.input_tokens >= 0) {
            known_input_tokens += stats.input_tokens;
        } else {
            unknown_usage_rounds++;
        }
        rounds_json.push_back(stats.to_json());
    }
    params["llm_response"]["debug"]["rounds"] = rounds_json;

    LOG(INFO) << "jev timing: catalog=" << ms_catalog << "ms questions=" << ms_questions
              << "ms api=" << ms_api << "ms assemble=" << ms_assemble
              << "ms total=" << elapsed_ms(t_start) << "ms budget=" << total_budget_ms
              << "ms asked=" << num_questions
              << " rounds=" << round_stats.size()
              << " failed_rounds=" << failed_rounds
              << " req_bytes=" << request_bytes
              << " input_tokens=" << known_input_tokens
              << " unknown_usage_rounds=" << unknown_usage_rounds;

    return Option<nlohmann::json>(params);
}
