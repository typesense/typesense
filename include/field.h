#pragma once

#include <string>
#include <s2/s2latlng.h>
#include "option.h"
#include "string_utils.h"
#include "logger.h"
#include "store.h"
#include <sparsepp.h>
#include <tsl/htrie_map.h>
#include <filter.h>
#include "json.hpp"
#include "embedder_manager.h"
#include "vector_query_ops.h"
#include <mutex>
#include "stemmer_manager.h"

namespace field_types {
    // first field value indexed will determine the type
    static const std::string AUTO = "auto";
    static const std::string OBJECT = "object";
    static const std::string OBJECT_ARRAY = "object[]";

    static const std::string STRING = "string";
    static const std::string INT32 = "int32";
    static const std::string INT64 = "int64";
    static const std::string FLOAT = "float";
    static const std::string BOOL = "bool";
    static const std::string NIL = "nil";
    static const std::string GEOPOINT = "geopoint";
    static const std::string STRING_ARRAY = "string[]";
    static const std::string INT32_ARRAY = "int32[]";
    static const std::string INT64_ARRAY = "int64[]";
    static const std::string FLOAT_ARRAY = "float[]";
    static const std::string BOOL_ARRAY = "bool[]";
    static const std::string GEOPOINT_ARRAY = "geopoint[]";

    static const std::string IMAGE = "image";

    static bool is_string_or_array(const std::string& type_def) {
        return type_def == "string*";
    }

    static bool is_array(const std::string& type_def) {
        return type_def.size() > 2 && type_def[type_def.size() - 2] == '[' &&  type_def[type_def.size() - 1] == ']';
    }
}

namespace fields {
    static const std::string name = "name";
    static const std::string type = "type";
    static const std::string facet = "facet";
    static const std::string optional = "optional";
    static const std::string index = "index";
    static const std::string sort = "sort";
    static const std::string infix = "infix";
    static const std::string locale = "locale";
    static const std::string nested = "nested";
    static const std::string nested_array = "nested_array";
    static const std::string num_dim = "num_dim";
    static const std::string vec_dist = "vec_dist";
    static const std::string reference = "reference";
    static const std::string async_reference = "async_reference";
    static const std::string embed = "embed";
    static const std::string from = "from";
    static const std::string model_name = "model_name";
    static const std::string range_index = "range_index";
    static const std::string stem = "stem";

    // Some models require additional parameters to be passed to the model during indexing/querying
    // For e.g. e5-small model requires prefix "passage:" for indexing and "query:" for querying
    static const std::string indexing_prefix = "indexing_prefix";
    static const std::string query_prefix = "query_prefix";
    static const std::string api_key = "api_key";
    static const std::string model_config = "model_config";

    static const std::string reference_helper_fields = ".ref";
    static const std::string REFERENCE_HELPER_FIELD_SUFFIX = "_sequence_id";

    static const std::string store = "store";
    
    static const std::string hnsw_params = "hnsw_params";
}

enum vector_distance_type_t {
    ip,
    cosine
};

struct reference_pair_t {
    std::string collection;
    std::string field;

    reference_pair_t(std::string collection, std::string field) : collection(std::move(collection)),
                                                                  field(std::move(field)) {}
};

struct field {
    std::string name;
    std::string type;
    bool facet;
    bool optional;
    bool index;
    std::string locale;
    bool sort;
    bool infix;

    bool nested;        // field inside an object

    bool store = true;        // store the field in disk

    // field inside an array of objects that is forced to be an array
    // integer to handle tri-state: true (1), false (0), not known yet (2)
    // third state is used to diff between array of object and array within object during write
    int nested_array;

    size_t num_dim;
    nlohmann::json embed;
    vector_distance_type_t vec_dist;

    static constexpr int VAL_UNKNOWN = 2;

    std::string reference;      // Foo.bar (reference to bar field in Foo collection).
    bool is_async_reference = false;

    bool range_index;

    bool is_reference_helper = false;

    bool stem = false;
    std::shared_ptr<Stemmer> stemmer;
  
    nlohmann::json hnsw_params;

    field() {}

    field(const std::string &name, const std::string &type, const bool facet, const bool optional = false,
          bool index = true, std::string locale = "", int sort = -1, int infix = -1, bool nested = false,
          int nested_array = 0, size_t num_dim = 0, vector_distance_type_t vec_dist = cosine,
          std::string reference = "", const nlohmann::json& embed = nlohmann::json(), const bool range_index = false,
          const bool store = true, const bool stem = false, const nlohmann::json hnsw_params = nlohmann::json(),
          const bool async_reference = false) :
            name(name), type(type), facet(facet), optional(optional), index(index), locale(locale),
            nested(nested), nested_array(nested_array), num_dim(num_dim), vec_dist(vec_dist), reference(reference),
            embed(embed), range_index(range_index), store(store), stem(stem), hnsw_params(hnsw_params),
            is_async_reference(async_reference) {

        set_computed_defaults(sort, infix);

        auto const suffix = std::string(fields::REFERENCE_HELPER_FIELD_SUFFIX);
        is_reference_helper = name.size() > suffix.size() && name.substr(name.size() - suffix.size()) == suffix;
        if (stem) {
            stemmer = StemmerManager::get_instance().get_stemmer(locale);
        }
    }

    void set_computed_defaults(int sort, int infix) {
        if(sort != -1) {
            this->sort = bool(sort);
        } else {
            this->sort = is_num_sort_field();
        }

        this->infix = (infix != -1) ? bool(infix) : false;
    }

    bool operator<(const field& f) const {
        return name < f.name;
    }

    bool operator==(const field& f) const {
        return name == f.name;
    }

    bool is_auto() const {
        return (type == field_types::AUTO);
    }

    bool is_single_integer() const {
        return (type == field_types::INT32 || type == field_types::INT64);
    }

    bool is_single_float() const {
        return (type == field_types::FLOAT);
    }

    bool is_single_bool() const {
        return (type == field_types::BOOL);
    }

    bool is_single_geopoint() const {
        return (type == field_types::GEOPOINT);
    }

    bool is_image() const {
        return (type == field_types::IMAGE);
    }

    bool is_integer() const {
        return (type == field_types::INT32 || type == field_types::INT32_ARRAY ||
                type == field_types::INT64 || type == field_types::INT64_ARRAY);
    }

    bool is_int32() const {
        return (type == field_types::INT32 || type == field_types::INT32_ARRAY);
    }

    bool is_int64() const {
        return (type == field_types::INT64 || type == field_types::INT64_ARRAY);
    }

    bool is_float() const {
        return (type == field_types::FLOAT || type == field_types::FLOAT_ARRAY);
    }

    bool is_bool() const {
        return (type == field_types::BOOL || type == field_types::BOOL_ARRAY);
    }

    bool is_geopoint() const {
        return (type == field_types::GEOPOINT || type == field_types::GEOPOINT_ARRAY);
    }

    bool is_object() const {
        return (type == field_types::OBJECT || type == field_types::OBJECT_ARRAY);
    }

    bool is_string() const {
        return (type == field_types::STRING || type == field_types::STRING_ARRAY);
    }

    bool is_string_star() const {
        return field_types::is_string_or_array(type);
    }

    bool is_facet() const {
        return facet;
    }

    bool is_array() const {
        return (type == field_types::STRING_ARRAY || type == field_types::INT32_ARRAY ||
                type == field_types::FLOAT_ARRAY ||
                type == field_types::INT64_ARRAY || type == field_types::BOOL_ARRAY ||
                type == field_types::GEOPOINT_ARRAY || type == field_types::OBJECT_ARRAY);
    }

    bool is_singular() const {
        return !is_array();
    }

    static bool is_dynamic(const std::string& name, const std::string& type) {
        return type == "string*" || (name != ".*" && type == field_types::AUTO) ||
               (name != ".*" && name.find(".*") != std::string::npos);
    }

    bool is_dynamic() const {
        return is_dynamic(name, type);
    }

    bool has_numerical_index() const {
        return (type == field_types::INT32 || type == field_types::INT64 ||
                type == field_types::FLOAT || type == field_types::BOOL);
    }

    bool is_num_sort_field() const {
        return (has_numerical_index() || is_geopoint());
    }

    bool is_sort_field() const {
        return is_num_sort_field() || (type == field_types::STRING);
    }

    bool is_num_sortable() const {
        return sort && is_num_sort_field();
    }

    bool is_str_sortable() const {
        return sort && type == field_types::STRING;
    }

    bool is_sortable() const {
        return is_num_sortable() || is_str_sortable();
    }

    bool is_stem() const {
        return stem;
    }

    bool has_valid_type() const {
        bool is_basic_type = is_string() || is_integer() || is_float() || is_bool() || is_geopoint() ||
                             is_object() || is_auto() || is_image();
        if(!is_basic_type) {
            return field_types::is_string_or_array(type);
        }
        return true;
    }

    std::string faceted_name() const {
        return (facet && !is_string()) ? "_fstr_" + name : name;
    }

    std::shared_ptr<Stemmer> get_stemmer() const {
        return stemmer;
    }
    
    static bool get_type(const nlohmann::json& obj, std::string& field_type) {
        if(obj.is_array()) {
            if(obj.empty()) {
                return false;
            }

            bool parseable = get_single_type(obj[0], field_type);
            if(!parseable) {
                return false;
            }

            field_type = field_type + "[]";
            return true;
        }

        return get_single_type(obj, field_type);
    }

    static bool get_single_type(const nlohmann::json& obj, std::string& field_type) {
        if(obj.is_string()) {
            field_type = field_types::STRING;
            return true;
        }

        if(obj.is_number_float()) {
            field_type = field_types::FLOAT;
            return true;
        }

        if(obj.is_number_integer()) {
            field_type = field_types::INT64;
            return true;
        }

        if(obj.is_boolean()) {
            field_type = field_types::BOOL;
            return true;
        }

        if(obj.is_object()) {
            field_type = field_types::OBJECT;
            return true;
        }

        return false;
    }

    static Option<bool> fields_to_json_fields(const std::vector<field> & fields,
                                              const std::string & default_sorting_field,
                                              nlohmann::json& fields_json);

    static Option<bool> json_field_to_field(bool enable_nested_fields, nlohmann::json& field_json,
                                            std::vector<field>& the_fields,
                                            string& fallback_field_type, size_t& num_auto_detect_fields);

    static Option<bool> json_fields_to_fields(bool enable_nested_fields,
                                              nlohmann::json& fields_json,
                                              std::string& fallback_field_type,
                                              std::vector<field>& the_fields);

    static Option<bool> validate_and_init_embed_field(const tsl::htrie_map<char, field>& search_schema,
                                                       nlohmann::json& field_json,
                                                       const nlohmann::json& fields_json,
                                                       field& the_field);


    static bool flatten_obj(nlohmann::json& doc, nlohmann::json& value, bool has_array, bool has_obj_array,
                            bool is_update, const field& the_field, const std::string& flat_name,
                            const std::unordered_map<std::string, field>& dyn_fields,
                            std::unordered_map<std::string, field>& flattened_fields);

    static Option<bool> flatten_field(nlohmann::json& doc, nlohmann::json& obj, const field& the_field,
                                      std::vector<std::string>& path_parts, size_t path_index, bool has_array,
                                      bool has_obj_array, bool is_update,
                                      const std::unordered_map<std::string, field>& dyn_fields,
                                      std::unordered_map<std::string, field>& flattened_fields);

    static Option<bool> flatten_doc(nlohmann::json& document, const tsl::htrie_map<char, field>& nested_fields,
                                    const std::unordered_map<std::string, field>& dyn_fields,
                                    bool is_update, std::vector<field>& flattened_fields);

    static void compact_nested_fields(tsl::htrie_map<char, field>& nested_fields);
};

enum index_operation_t {
    CREATE,
    UPSERT,
    UPDATE,
    EMPLACE,
    DELETE
};

enum class DIRTY_VALUES {
    REJECT = 1,
    DROP = 2,
    COERCE_OR_REJECT = 3,
    COERCE_OR_DROP = 4,
};

namespace sort_field_const {
    static const std::string name = "name";
    static const std::string order = "order";
    static const std::string asc = "ASC";
    static const std::string desc = "DESC";

    static const std::string text_match = "_text_match";
    static const std::string eval = "_eval";
    static const std::string seq_id = "_seq_id";
    static const std::string group_found = "_group_found";

    static const std::string exclude_radius = "exclude_radius";
    static const std::string precision = "precision";

    static const std::string missing_values = "missing_values";

    static const std::string vector_distance = "_vector_distance";
    static const std::string vector_query = "_vector_query";
}

namespace ref_include {
    static const std::string strategy_key = "strategy";
    static const std::string merge_string = "merge";
    static const std::string nest_string = "nest";
    static const std::string nest_array_string = "nest_array";

    enum strategy_enum {merge = 0, nest, nest_array};

    static Option<strategy_enum> string_to_enum(const std::string& strategy) {
        if (strategy == merge_string) {
            return Option<strategy_enum>(merge);
        } else if (strategy == nest_string) {
            return Option<strategy_enum>(nest);
        } else if (strategy == nest_array_string) {
            return Option<strategy_enum>(nest_array);
        }

        return Option<strategy_enum>(400, "Unknown include strategy `" + strategy + "`. "
                                           "Valid options are `merge`, `nest`, `nest_array`.");
    }
}

struct ref_include_exclude_fields {
    std::string collection_name;
    std::string include_fields;
    std::string exclude_fields;
    std::string alias;
    ref_include::strategy_enum strategy = ref_include::nest;

    // In case we have nested join.
    std::vector<ref_include_exclude_fields> nested_join_includes = {};
};

struct hnsw_index_t;

struct sort_vector_query_t {
        vector_query_t query;
        hnsw_index_t* vector_index;
}; 

struct sort_by {
    enum missing_values_t {
        first,
        last,
        normal,
    };

    struct eval_t {
        filter_node_t** filter_trees = nullptr; // Array of filter_node_t pointers.
        std::vector<uint32_t*> eval_ids_vec;
        std::vector<uint32_t> eval_ids_count_vec;
        std::vector<int64_t> scores;
    };

    std::string name;
    std::vector<std::string> eval_expressions;
    std::string order;

    // for text_match score bucketing
    uint32_t text_match_buckets;

    // geo related fields
    int64_t geopoint;
    uint32_t exclude_radius;
    uint32_t geo_precision;
    std::string unit;

    missing_values_t missing_values;
    eval_t eval;

    std::string reference_collection_name;
    std::vector<std::string> nested_join_collection_names;
    sort_vector_query_t vector_query;

    sort_by(const std::string & name, const std::string & order):
            name(name), order(order), text_match_buckets(0), geopoint(0), exclude_radius(0), geo_precision(0),
            missing_values(normal) {
    }

    sort_by(std::vector<std::string> eval_expressions, std::vector<int64_t> scores, std::string  order):
            eval_expressions(std::move(eval_expressions)), order(std::move(order)), text_match_buckets(0), geopoint(0), exclude_radius(0),
            geo_precision(0), missing_values(normal) {
        name = sort_field_const::eval;
        eval.scores = std::move(scores);
    }

    sort_by(const std::string &name, const std::string &order, uint32_t text_match_buckets, int64_t geopoint,
            uint32_t exclude_radius, uint32_t geo_precision) :
            name(name), order(order), text_match_buckets(text_match_buckets),
            geopoint(geopoint), exclude_radius(exclude_radius), geo_precision(geo_precision),
            missing_values(normal) {
    }

    sort_by(const sort_by& other) {
        if (&other == this)
            return;
        name = other.name;
        eval_expressions = other.eval_expressions;
        order = other.order;
        text_match_buckets = other.text_match_buckets;
        geopoint = other.geopoint;
        exclude_radius = other.exclude_radius;
        geo_precision = other.geo_precision;
        unit = other.unit;
        missing_values = other.missing_values;
        eval = other.eval;
        reference_collection_name = other.reference_collection_name;
        nested_join_collection_names = other.nested_join_collection_names;
        vector_query = other.vector_query;
    }

    sort_by& operator=(const sort_by& other) {
        if (&other == this) {
            return *this;
        }

        name = other.name;
        eval_expressions = other.eval_expressions;
        order = other.order;
        text_match_buckets = other.text_match_buckets;
        geopoint = other.geopoint;
        exclude_radius = other.exclude_radius;
        geo_precision = other.geo_precision;
        unit = other.unit;
        missing_values = other.missing_values;
        eval = other.eval;
        reference_collection_name = other.reference_collection_name;
        nested_join_collection_names = other.nested_join_collection_names;
        return *this;
    }

    [[nodiscard]] inline bool is_nested_join_sort_by() const {
        return !nested_join_collection_names.empty();
    }
};

class GeoPoint {
    constexpr static const double EARTH_RADIUS = 3958.75;
    constexpr static const double METER_CONVERT = 1609.00;
    constexpr static const uint64_t MASK_H32_BITS = 0xffffffffUL;
public:
    static uint64_t pack_lat_lng(double lat, double lng) {
        // https://stackoverflow.com/a/1220393/131050
        const int32_t ilat = lat * 1000000;
        const int32_t ilng = lng * 1000000;
        // during int32_t -> uint64_t, higher order bits will be 1, so we have to mask that
        const uint64_t lat_lng = (uint64_t(ilat) << 32) | (uint64_t)(ilng & MASK_H32_BITS);
        return lat_lng;
    }

    static void unpack_lat_lng(uint64_t packed_lat_lng, S2LatLng& latlng) {
        const double lat = double(int32_t((packed_lat_lng >> 32) & MASK_H32_BITS)) / 1000000;
        const double lng = double(int32_t(packed_lat_lng & MASK_H32_BITS)) / 1000000;
        latlng = S2LatLng::FromDegrees(lat, lng);
    }

    // distance in meters
    static int64_t distance(const S2LatLng& a, const S2LatLng& b) {
        double rdist = a.GetDistance(b).radians();
        double dist = EARTH_RADIUS * rdist;
        return dist * METER_CONVERT;
    }
};

struct facet_count_t {
    uint32_t count = 0;
    // for value based faceting, actual value is stored here
    std::string fvalue;
    // for hash based faceting, hash value is stored here
    int64_t fhash;
    // used to fetch the actual document and value for representation
    uint32_t doc_id = 0;
    uint32_t array_pos = 0;
    //for sorting based on other field
    int64_t sort_field_val;
};

struct facet_stats_t {
    double fvmin = std::numeric_limits<double>::max(),
            fvmax = -std::numeric_limits<double>::max(),
            fvcount = 0,
            fvsum = 0;
};

struct range_specs_t {
    std::string range_label;
    int64_t lower_range;

    bool is_in_range(int64_t key) {
        return key >= lower_range;
    }
};

struct facet {
    const std::string field_name;
    spp::sparse_hash_map<uint64_t, facet_count_t> result_map;
    spp::sparse_hash_map<std::string, facet_count_t> value_result_map;

    // used for facet value query
    spp::sparse_hash_map<std::string, std::vector<std::string>> fvalue_tokens;
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> hash_tokens;

    // used for faceting grouped results
    spp::sparse_hash_map<uint32_t, spp::sparse_hash_set<uint32_t>> hash_groups;

    facet_stats_t stats;

    //dictionary of key=>pair(range_id, range_val)
    std::map<int64_t, range_specs_t> facet_range_map;

    bool is_range_query;

    bool sampled = false;

    bool is_wildcard_match = false;
    
    bool is_intersected = false;

    bool is_sort_by_alpha = false;

    std::string sort_order="";

    std::string sort_field="";

    uint32_t orig_index;

    bool get_range(int64_t key, std::pair<int64_t, std::string>& range_pair) {
        if(facet_range_map.empty()) {
            LOG (ERROR) << "Facet range is not defined!!!";
        }

        auto it = facet_range_map.lower_bound(key);

        if(it != facet_range_map.end() && it->first == key) {
            it++;
        }

        if(it != facet_range_map.end() && it->second.is_in_range(key)) {
            range_pair.first = it->first;
            range_pair.second = it->second.range_label;
            return true;
        }

        return false;
    }

    explicit facet(const std::string& field_name, uint32_t orig_index, std::map<int64_t, range_specs_t> facet_range = {},
                   bool is_range_q = false, bool sort_by_alpha=false, const std::string& order="",
                   const std::string& sort_by_field="")
                   : field_name(field_name), facet_range_map(facet_range),
                   is_range_query(is_range_q), is_sort_by_alpha(sort_by_alpha), sort_order(order),
                   sort_field(sort_by_field), orig_index(orig_index) {
    }
};

struct facet_info_t {
    // facet hash => resolved tokens
    std::unordered_map<uint64_t, std::vector<std::string>> hashes;
    std::vector<std::vector<std::string>> fvalue_searched_tokens;
    bool use_facet_query = false;
    bool should_compute_stats = false;
    bool use_value_index = false;
    field facet_field{"", "", false};
};

struct facet_query_t {
    std::string field_name;
    std::string query;
};

struct facet_value_t {
    std::string value;
    std::string highlighted;
    uint32_t count;
    int64_t sort_field_val;
    nlohmann::json parent;
};

struct facet_hash_values_t {
    uint32_t length = 0;
    std::vector<uint32_t> hashes;

    facet_hash_values_t() {
        length = 0;
    }

    facet_hash_values_t(facet_hash_values_t&& hash_values) noexcept {
        length = hash_values.length;
        hashes = hash_values.hashes;

        hash_values.length = 0;
        hash_values.hashes.clear();
    }

    facet_hash_values_t& operator=(facet_hash_values_t&& other) noexcept {
        if (this != &other) {
            hashes.clear();

            hashes = other.hashes;
            length = other.length;

            other.hashes.clear();
            other.length = 0;
        }

        return *this;
    }

    ~facet_hash_values_t() {
        hashes.clear();
    }

    uint64_t size() const {
        return length;
    }

    uint64_t back() const {
        return hashes.back();
    }
};