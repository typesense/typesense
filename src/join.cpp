#include "join.h"

#include <collection_manager.h>
#include <timsort.hpp>

Option<bool> Join::single_value_filter_query(nlohmann::json& document, const std::string& field_name,
                                             const std::string& ref_field_type, std::string& filter_value,
                                             const bool& is_reference_value) {
    auto const& json_value = document[field_name];

    if (json_value.is_null()) {
        return Option<bool>(422, "Field `" + field_name + "` cannot have `null` value.");
    }

    if (json_value.is_string() && ref_field_type == field_types::STRING) {
        std::string value = json_value.get<std::string>();
        if (value.empty()) {
            return Option<bool>(400, "Error with field `" + field_name + "`: Value cannot be empty.");
        }

        // Special symbols are ignored when enclosed inside backticks.
        bool is_backtick_present = false;
        bool special_symbols_present = false;
        bool in_backtick = false;
        auto const size = value.size();
        for (size_t i = 0; i < size; i++) {
            auto c = value[i];
            if (c == '`') {
                in_backtick = !in_backtick;
                is_backtick_present = true;
            } else if (!in_backtick && (c == '(' || c == ')' ||
                                        (c == '&' && i + 1 < size && value[i + 1] == '&') ||
                                        (c == '|' && i + 1 < size && value[i + 1] == '|'))) {
                special_symbols_present = true;
                if (is_backtick_present) {
                    break;
                }
            }
        }

        if (is_backtick_present && special_symbols_present) {
            // Value containing special symbols cannot be parsed.
            return Option<bool>(400, "Filter value `" + value + "` cannot be parsed.");
        } else if (!is_backtick_present) {
            value = "`" + json_value.get<std::string>() + "`";
        }
        filter_value += value;
    } else if (json_value.is_number_integer() && (ref_field_type == field_types::INT64 ||
                                                  (ref_field_type == field_types::INT32 &&
                                                   StringUtils::is_int32_t(std::to_string(json_value.get<int64_t>()))))) {
        filter_value += std::to_string(json_value.get<int64_t>());
    } else if (is_reference_value) {
        return Option<bool>(400, "Field `" + field_name + "` must have `" + ref_field_type + "` value.");
    } else if (json_value.is_number_float() && ref_field_type == field_types::FLOAT) {
        filter_value += std::to_string(json_value.get<float>());
    } else {
        return Option<bool>(400, "Expected field `" + field_name + "` to have any of {`int32`, `int64`, `float`, `string`} "
                                   "types.");
    }

    return Option<bool>(true);
}

Option<bool> Join::populate_reference_helper_fields(nlohmann::json& document,
                                                    const tsl::htrie_map<char, field>& schema,
                                                    const spp::sparse_hash_map<std::string, reference_info_t>& reference_fields,
                                                    tsl::htrie_set<char>& object_reference_helper_fields,
                                                    const bool& is_update) {
    tsl::htrie_set<char> flat_fields;
    if (!reference_fields.empty() && document.contains(".flat")) {
        for (const auto &item: document[".flat"].get<std::vector<std::string>>()) {
            flat_fields.insert(item);
        }
    }

    // Add reference helper fields in the document.
    for (auto const& pair: reference_fields) {
        auto field_name = pair.first;
        auto const reference_helper_field = field_name + fields::REFERENCE_HELPER_FIELD_SUFFIX;

        auto const& field = schema.at(field_name);
        auto const& optional = field.optional;
        auto const& is_async_reference = field.is_async_reference;
        // Strict checking for presence of non-optional reference field during indexing operation.
        auto is_required = !is_update && !optional;
        if (is_required && document.count(field_name) != 1) {
            return Option<bool>(400, "Missing the required reference field `" + field_name
                                     + "` in the document.");
        } else if (document.count(field_name) != 1) {
            if (is_update) {
                document[fields::reference_helper_fields] += reference_helper_field;
            }
            continue;
        }

        const auto& ref_info = pair.second;
        const auto& reference_collection_name = ref_info.collection;
        const auto& reference_field_name = ref_info.field;

        if (is_update && document.contains(reference_helper_field) &&
            (!document[field_name].is_array() || document[field_name].size() == document[reference_helper_field].size())) {

            document[fields::reference_helper_fields] += reference_helper_field;
            // No need to look up the reference collection since reference helper field is already populated.
            // Saves needless computation in cases where references are known beforehand. For example, when cascade
            // deleting the related docs.
            continue;
        }

        if (CollectionManager::get_instance().get_collection(reference_collection_name) == nullptr) {
            if (is_async_reference) {
                document[fields::reference_helper_fields] += reference_helper_field;
                if (document[field_name].is_array()) {
                    document[reference_helper_field] = nlohmann::json::array();
                    // Having the same number of values makes it easier to update the references in the future.
                    document[reference_helper_field].insert(document[reference_helper_field].begin(),
                                                            document[field_name].size(),
                                                            Join::reference_helper_sentinel_value);
                } else {
                    document[reference_helper_field] = Join::reference_helper_sentinel_value;
                }
                continue;
            }

            return Option<bool>(400, "Referenced collection `" + reference_collection_name
                                     + "` not found.");
        }

        bool is_object_reference_field = flat_fields.count(field_name) != 0;
        std::string object_key;
        bool is_object_array = false;
        if (is_object_reference_field) {
            object_reference_helper_fields.insert(reference_helper_field);

            std::vector<std::string> tokens;
            StringUtils::split(field_name, tokens, ".");
            if (schema.count(tokens[0]) == 0) {
                return Option<bool>(400, "Could not find `" + tokens[0] + "` object/object[] field in the schema.");
            }
            object_key = tokens[0];
            is_object_array = schema.at(object_key).is_array();
        }

        if (reference_field_name == "id") {
            auto id_field_type_error_op =  Option<bool>(400, "Field `" + field_name + "` must have string value.");
            if (is_object_array) {
                if (!document[field_name].is_array()) {
                    return Option<bool>(400, "Expected `" + field_name + "` to be an array.");
                }

                document[reference_helper_field] = nlohmann::json::array();
                document[fields::reference_helper_fields] += reference_helper_field;

                std::vector<std::string> keys;
                StringUtils::split(field_name, keys, ".");
                auto const& object_array = document[keys[0]];

                for (uint32_t i = 0; i < object_array.size(); i++) {
                    if (optional && object_array[i].count(keys[1]) == 0) {
                        continue;
                    } else if (object_array[i].count(keys[1]) == 0) {
                        return Option<bool>(400, "Object at index `" + std::to_string(i) + "` is missing `" + field_name + "`.");
                    } else if (!object_array[i].at(keys[1]).is_string()) {
                        return id_field_type_error_op;
                    }

                    auto id = object_array[i].at(keys[1]).get<std::string>();
                    auto ref_doc_id_op = CollectionManager::doc_id_to_seq_id(reference_collection_name, id);
                    if (!ref_doc_id_op.ok() && is_async_reference) {
                        auto const& value = nlohmann::json::array({i, Join::reference_helper_sentinel_value});
                        document[reference_helper_field] += value;
                    } else if (!ref_doc_id_op.ok()) {
                        return Option<bool>(400, "Referenced document having `id: " + id +
                                                 "` not found in the collection `" +=
                                                 reference_collection_name + "`." );
                    } else {
                        // Adding the index of the object along with referenced doc id to account for the scenario where a
                        // reference field of an object array might be optional and missing.
                        document[reference_helper_field] += nlohmann::json::array({i, ref_doc_id_op.get()});
                    }
                }
            } else if (document[field_name].is_array()) {
                document[reference_helper_field] = nlohmann::json::array();
                document[fields::reference_helper_fields] += reference_helper_field;

                for (const auto &item: document[field_name].items()) {
                    if (optional && item.value().is_null()) {
                        continue;
                    } else if (!item.value().is_string()) {
                        return id_field_type_error_op;
                    }

                    auto id = item.value().get<std::string>();
                    auto ref_doc_id_op = CollectionManager::doc_id_to_seq_id(reference_collection_name, id);
                    if (!ref_doc_id_op.ok() && is_async_reference) {
                        document[reference_helper_field] += Join::reference_helper_sentinel_value;
                    } else if (!ref_doc_id_op.ok()) {
                        return Option<bool>(400, "Referenced document having `id: " + id +
                                                 "` not found in the collection `" +=
                                                 reference_collection_name + "`." );
                    } else {
                        document[reference_helper_field] += ref_doc_id_op.get();
                    }
                }
            } else if (document[field_name].is_string()) {
                document[fields::reference_helper_fields] += reference_helper_field;

                auto id = document[field_name].get<std::string>();
                auto ref_doc_id_op = CollectionManager::doc_id_to_seq_id(reference_collection_name, id);
                if (!ref_doc_id_op.ok() && is_async_reference) {
                    document[reference_helper_field] = Join::reference_helper_sentinel_value;
                } else if (!ref_doc_id_op.ok()) {
                    return Option<bool>(400, "Referenced document having `id: " + id +
                                             "` not found in the collection `" +=
                            reference_collection_name + "`." );
                } else {
                    document[reference_helper_field] = ref_doc_id_op.get();
                }
            } else if (optional && document[field_name].is_null()) {
                // Reference helper field should also be removed along with reference field.
                if (is_update) {
                    document[reference_helper_field] = nullptr;
                }
                continue;
            } else {
                return id_field_type_error_op;
            }

            continue;
        }

        if (ref_info.referenced_field.name.empty()) {
            return Option<bool>(400, "Referenced field `" + reference_field_name +
                                     "` not found in the collection `" += reference_collection_name + "`.");
        }

        const auto& ref_field = ref_info.referenced_field;
        if (!ref_field.index) {
            return Option<bool>(400, "Referenced field `" + reference_field_name +
                                     "` in the collection `" += reference_collection_name + "` must be indexed.");
        }

        std::string ref_field_type = ref_field.is_string() ? field_types::STRING :
                                     ref_field.is_int32() ? field_types::INT32 :
                                     ref_field.is_int64() ? field_types::INT64 : field_types::NIL;

        if (ref_field_type == field_types::NIL) {
            return Option<bool>(400, "Cannot add a reference to `" + reference_collection_name + "." += reference_field_name +
                                        "` of type `" += ref_field.type + "`.");
        }

        if (is_object_array) {
            if (!document[field_name].is_array()) {
                return Option<bool>(400, "Expected `" + field_name + "` to be an array.");
            }

            document[reference_helper_field] = nlohmann::json::array();
            document[fields::reference_helper_fields] += reference_helper_field;
            nlohmann::json temp_doc; // To store singular values of `field_name` field.

            std::vector<std::string> keys;
            StringUtils::split(field_name, keys, ".");
            auto const& object_array = document[keys[0]];

            for (uint32_t i = 0; i < object_array.size(); i++) {
                if (optional && object_array[i].count(keys[1]) == 0) {
                    continue;
                } else if (object_array[i].count(keys[1]) == 0) {
                    return Option<bool>(400, "Object at index `" + std::to_string(i) + "` is missing `" + field_name + "`.");
                }

                temp_doc[field_name] = object_array[i].at(keys[1]);
                std::string filter_query = reference_field_name + ":= ";

                auto single_value_filter_query_op = single_value_filter_query(temp_doc, field_name, ref_field_type,
                                                                              filter_query);
                if (!single_value_filter_query_op.ok()) {
                    if (optional && single_value_filter_query_op.code() == 422) {
                        continue;
                    }
                    return Option<bool>(400, single_value_filter_query_op.error());
                }

                filter_result_t filter_result;
                auto filter_ids_op = CollectionManager::get_filter_ids(reference_collection_name, filter_query,
                                                                       filter_result);
                if (!filter_ids_op.ok()) {
                    return filter_ids_op;
                }

                if (filter_result.count == 0 && is_async_reference) {
                    document[reference_helper_field] += nlohmann::json::array({i, Join::reference_helper_sentinel_value});
                } else if (filter_result.count != 1) {
                    // Constraints similar to foreign key apply here. The reference match must be unique and not null.
                    return  Option<bool>(400, filter_result.count < 1 ?
                                              "Reference document having `" + filter_query + "` not found in the collection `"
                                              += reference_collection_name + "`." :
                                              "Multiple documents having `" + filter_query + "` found in the collection `" +=
                                              reference_collection_name + "`.");
                } else {
                    // Adding the index of the object along with referenced doc id to account for the scenario where a
                    // reference field of an object array might be optional and missing.
                    document[reference_helper_field] += nlohmann::json::array({i, filter_result.docs[0]});
                }
            }
            continue;
        }

        auto const is_reference_array_field = field.is_array();
        std::vector<std::string> filter_values;
        if (is_reference_array_field) {
            if (document[field_name].is_null()) {
                document[reference_helper_field] = nlohmann::json::array();
                document[fields::reference_helper_fields] += reference_helper_field;

                continue;
            } else if (!document[field_name].is_array()) {
                return Option<bool>(400, "Expected `" + field_name + "` to be an array.");
            }

            nlohmann::json temp_doc;
            for (size_t i = 0; i < document[field_name].size(); i++) {
                temp_doc[field_name] = document[field_name].at(i);
                std::string value;
                auto single_value_filter_query_op = single_value_filter_query(temp_doc, field_name, ref_field_type,
                                                                              value);
                if (!single_value_filter_query_op.ok()) {
                    // We don't accept null value in an array of values. No need to handle 422 code.
                    return single_value_filter_query_op;
                }
                filter_values.emplace_back(value);
            }

            document[reference_helper_field] = nlohmann::json::array();
            document[fields::reference_helper_fields] += reference_helper_field;

            if (filter_values.empty()) {
                continue;
            }
        } else {
            std::string value;
            auto single_value_filter_query_op = single_value_filter_query(document, field_name, ref_field_type, value);
            if (!single_value_filter_query_op.ok()) {
                if (optional && single_value_filter_query_op.code() == 422) {
                    // Reference helper field should also be removed along with reference field.
                    if (is_update) {
                        document[reference_helper_field] = nullptr;
                    }
                    continue;
                }
                return Option<bool>(400, single_value_filter_query_op.error());
            }

            filter_values.emplace_back(value);
            document[fields::reference_helper_fields] += reference_helper_field;
        }

        for (const auto& filter_value: filter_values) {
            std::string filter_query = reference_field_name + (field.is_string() ? ":= " : ": ") += filter_value;
            filter_result_t filter_result;
            auto filter_ids_op = CollectionManager::get_filter_ids(reference_collection_name, filter_query,
                                                                   filter_result);
            if (!filter_ids_op.ok()) {
                return filter_ids_op;
            }

            if (filter_result.count == 0 && is_async_reference) {
                if (is_reference_array_field) {
                    document[reference_helper_field] += Join::reference_helper_sentinel_value;
                } else {
                    document[reference_helper_field] = Join::reference_helper_sentinel_value;
                }
            } else if (filter_result.count != 1) {
                // Constraints similar to foreign key apply here. The reference match must be unique and not null.
                return  Option<bool>(400, filter_result.count < 1 ?
                                          "Reference document having `" + filter_query + "` not found in the collection `"
                                          += reference_collection_name + "`." :
                                          "Multiple documents having `" + filter_query + "` found in the collection `" +=
                                          reference_collection_name + "`.");
            } else {
                if (is_reference_array_field) {
                    document[reference_helper_field] += filter_result.docs[0];
                } else {
                    document[reference_helper_field] = filter_result.docs[0];
                }
            }
        }
    }

    return Option<bool>(true);
}

Option<bool> Join::prune_ref_doc(nlohmann::json& doc,
                                 const reference_filter_result_t& references,
                                 const tsl::htrie_set<char>& ref_include_fields_full,
                                 const tsl::htrie_set<char>& ref_exclude_fields_full,
                                 const bool& is_reference_array,
                                 const ref_include_exclude_fields& ref_include_exclude) {
    nlohmann::json original_doc;
    if (!ref_include_exclude.nested_join_includes.empty()) {
        original_doc = doc;
    }

    auto const& ref_collection_name = ref_include_exclude.collection_name;
    auto const& alias = ref_include_exclude.alias;
    auto const& strategy = ref_include_exclude.strategy;
    auto error_prefix = "Referenced collection `" + ref_collection_name + "`: ";

    // One-to-one relation.
    if (strategy != ref_include::nest_array && !is_reference_array && references.count == 1) {
        auto ref_doc_seq_id = references.docs[0];

        nlohmann::json ref_doc;
        auto get_doc_op = CollectionManager::get_document_from_store(ref_collection_name, ref_doc_seq_id, ref_doc);
        if (!get_doc_op.ok()) {
            if (ref_doc_seq_id == Join::reference_helper_sentinel_value) {
                return Option<bool>(true);
            }
            return Option<bool>(get_doc_op.code(), error_prefix + get_doc_op.error());
        }

        Collection::remove_flat_fields(ref_doc);
        Collection::remove_reference_helper_fields(ref_doc);

        auto prune_op = Collection::prune_doc(ref_doc, ref_include_fields_full, ref_exclude_fields_full,
                                              "", 0, std::map<std::string, reference_filter_result_t>{},
                                              ref_collection_name);
        if (!prune_op.ok()) {
            return Option<bool>(prune_op.code(), error_prefix + prune_op.error());
        }

        auto const key = alias.empty() ? ref_collection_name : alias;
        auto const& nest_ref_doc = (strategy == ref_include::nest);
        if (!ref_doc.empty()) {
            if (nest_ref_doc) {
                doc[key] = ref_doc;
            } else {
                if (!alias.empty()) {
                    auto temp_doc = ref_doc;
                    ref_doc.clear();
                    for (const auto &item: temp_doc.items()) {
                        ref_doc[alias + item.key()] = item.value();
                    }
                }
                doc.update(ref_doc);
            }
        }

        // Include nested join references.
        if (!ref_include_exclude.nested_join_includes.empty()) {
            // Passing empty references in case the nested include collection is not joined, but it still can be included
            // if we have a reference to it.
            std::map<std::string, reference_filter_result_t> refs;
            auto nested_include_exclude_op = include_references(nest_ref_doc ? doc[key] : doc, ref_doc_seq_id,
                                                                ref_collection_name,
                                                                references.coll_to_references == nullptr ? refs :
                                                                references.coll_to_references[0],
                                                                ref_include_exclude.nested_join_includes, original_doc);
            if (!nested_include_exclude_op.ok()) {
                return nested_include_exclude_op;
            }
        }

        return Option<bool>(true);
    }

    // One-to-many relation.
    if(!ref_include_exclude.related_docs_field.empty()) {
        doc[ref_include_exclude.related_docs_field] = references.count;
    }

    std::vector<uint32_t> doc_ids(references.docs, references.docs + references.count);
    size_t updated_count = references.count;

    if(!ref_include_exclude.sort_by_str.empty() && references.count > 1) {
        if(ref_include_exclude.limit > 0 && ref_include_exclude.limit < updated_count) {
            updated_count = ref_include_exclude.limit;
        }

        auto op = CollectionManager::process_ref_include_fields_sort(ref_collection_name,
                                                                     ref_include_exclude.sort_by_str, updated_count,
                                                                     doc_ids);
        if(!op.ok()) {
            return Option<bool>(op.code(), error_prefix + op.error());
        }
    }

    for (uint32_t i = 0; i < updated_count; i++) {
        auto ref_doc_seq_id = doc_ids[i];

        nlohmann::json ref_doc;
        std::string key;
        auto const& nest_ref_doc = (strategy == ref_include::nest || strategy == ref_include::nest_array);

        auto get_doc_op = CollectionManager::get_document_from_store(ref_collection_name, ref_doc_seq_id,
                                                                     ref_doc);
        if (!get_doc_op.ok()) {
            // Referenced document is not yet indexed.
            if (ref_doc_seq_id == Join::reference_helper_sentinel_value) {
                continue;
            }
            return Option<bool>(get_doc_op.code(), error_prefix + get_doc_op.error());
        }

        Collection::remove_flat_fields(ref_doc);
        Collection::remove_reference_helper_fields(ref_doc);

        auto prune_op = Collection::prune_doc(ref_doc, ref_include_fields_full, ref_exclude_fields_full);
        if (!prune_op.ok()) {
            return Option<bool>(prune_op.code(), error_prefix + prune_op.error());
        }

        if (!ref_doc.empty()) {
            if (nest_ref_doc) {
                key = alias.empty() ? ref_collection_name : alias;
                if (doc.contains(key) && !doc[key].is_array()) {
                    return Option<bool>(400, "Could not include the reference document of `" + ref_collection_name +
                                             "` collection. Expected `" += key + "` to be an array. Try " +
                                             (alias.empty() ? "adding an" : "renaming the") + " alias.");
                }

                doc[key] += ref_doc;
            } else {
                for (auto ref_doc_it = ref_doc.begin(); ref_doc_it != ref_doc.end(); ref_doc_it++) {
                    auto const& ref_doc_key = ref_doc_it.key();
                    key = alias + ref_doc_key;
                    if (doc.contains(key) && !doc[key].is_array()) {
                        return Option<bool>(400, "Could not include the value of `" + ref_doc_key +
                                                 "` key of the reference document of `" += ref_collection_name +
                                                 "` collection. Expected `" += key + "` to be an array. Try " +
                                                 (alias.empty() ? "adding an" : "renaming the") + " alias.");
                    }

                    // Add the values of ref_doc as JSON array into doc.
                    doc[key] += ref_doc_it.value();
                }
            }
        }

        // Include nested join references.
        if (!ref_include_exclude.nested_join_includes.empty()) {
            // Passing empty references in case the nested include collection is not joined, but it still can be included
            // if we have a reference to it.
            std::map<std::string, reference_filter_result_t> refs;
            auto nested_include_exclude_op = include_references(nest_ref_doc ? doc[key].at(i) : doc, ref_doc_seq_id,
                                                                ref_collection_name,
                                                                references.coll_to_references == nullptr ? refs :
                                                                references.coll_to_references[i],
                                                                ref_include_exclude.nested_join_includes, original_doc);
            if (!nested_include_exclude_op.ok()) {
                return nested_include_exclude_op;
            }
        }
    }

    return Option<bool>(true);
}

Option<bool> Join::include_references(nlohmann::json& doc, const uint32_t& seq_id, const std::string& collection_name,
                                      const std::map<std::string, reference_filter_result_t>& reference_filter_results,
                                      const std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec,
                                      const nlohmann::json& original_doc) {
    for (auto const& ref_include_exclude: ref_include_exclude_fields_vec) {
        auto ref_collection_name = ref_include_exclude.collection_name;

        {
            auto& cm = CollectionManager::get_instance();
            auto ref_collection = cm.get_collection(ref_collection_name);
            if (ref_collection == nullptr) {
                return Option<bool>(400, "Referenced collection `" + ref_collection_name +
                                         "` in `include_fields` not found.");
            }
            // `CollectionManager::get_collection` accounts for collection alias being used and provides pointer to the
            // original collection.
            ref_collection_name = ref_collection->get_name();
        }

        auto const joined_on_ref_collection = reference_filter_results.count(ref_collection_name) > 0,
                has_filter_reference = (joined_on_ref_collection &&
                                        reference_filter_results.at(ref_collection_name).count > 0);
        auto doc_has_reference = false, joined_coll_has_reference = false;
        reference_info_t ref_info{};

        // Reference include_by without join, check if doc itself contains the reference.
        if (!joined_on_ref_collection && !collection_name.empty()) {
            auto op = CollectionManager::get_instance().is_referenced_in(ref_collection_name, collection_name);
            if (op.ok()) {
                doc_has_reference = true;
                ref_info = op.get();
            }
        }

        std::string joined_coll_having_reference;
        // Check if the joined collection has a reference.
        if (!joined_on_ref_collection && !doc_has_reference) {
            for (const auto &reference_filter_result: reference_filter_results) {
                auto op = CollectionManager::get_instance().is_referenced_in(ref_collection_name,
                                                                             reference_filter_result.first);
                if (op.ok()) {
                    joined_coll_has_reference = true;
                    joined_coll_having_reference = reference_filter_result.first;
                    ref_info = op.get();
                    break;
                }
            }
        }

        if (!has_filter_reference && !doc_has_reference && !joined_coll_has_reference) {
            continue;
        }

        tsl::htrie_set<char> ref_include_fields_full, ref_exclude_fields_full;
        auto include_exclude_op = CollectionManager::populate_include_exclude_fields(ref_collection_name,
                                                                                        ref_include_exclude.include_fields,
                                                                                        ref_include_exclude.exclude_fields,
                                                                                        ref_include_fields_full,
                                                                                        ref_exclude_fields_full);
        auto error_prefix = "Referenced collection `" + ref_collection_name + "`: ";
        if (!include_exclude_op.ok()) {
            return Option<bool>(include_exclude_op.code(), error_prefix + include_exclude_op.error());
        }

        Option<bool> prune_doc_op = Option<bool>(true);
        auto const& ref_collection_alias = ref_include_exclude.alias;
        if (has_filter_reference) {
            auto const& ref_filter_result = reference_filter_results.at(ref_collection_name);
            prune_doc_op = prune_ref_doc(doc, ref_filter_result, ref_include_fields_full, ref_exclude_fields_full,
                                         ref_filter_result.is_reference_array_field, ref_include_exclude);
        } else if (doc_has_reference) {
            prune_doc_op = CollectionManager::include_related_docs(collection_name, doc, seq_id, ref_info,
                                                                   ref_include_fields_full, ref_exclude_fields_full,
                                                                   original_doc, ref_include_exclude);
        } else if (joined_coll_has_reference) {
            auto const& reference_field_name = ref_info.field;
            auto const& reference_filter_result = reference_filter_results.at(joined_coll_having_reference);
            auto const& count = reference_filter_result.count;
            auto const& docs = reference_filter_result.docs;

            std::vector<uint32_t> ids;
            CollectionManager::get_related_ids(joined_coll_having_reference, reference_field_name,
                                               std::vector<uint32_t>(docs, docs + count), ids);
            if (ids.empty()) {
                continue;
            }

            reference_filter_result_t result;
            result.count = ids.size();
            result.docs = &ids[0];
            prune_doc_op = prune_ref_doc(doc, result, ref_include_fields_full, ref_exclude_fields_full,
                                         ref_info.is_array, ref_include_exclude);
            result.docs = nullptr;
        }

        if (!prune_doc_op.ok()) {
            return prune_doc_op;
        }
    }

    return Option<bool>(true);
}

Option<bool> parse_reference_filter_helper(const std::string& filter_query, size_t& index, std::string& ref_coll_name,
                                           std::string& join) {
    auto error = Option<bool>(400, "Could not parse the reference filter: `" + filter_query.substr(index) + "`.");

    if (index + 2 >= filter_query.size() ||
            (filter_query[index] != '$' && (filter_query[index] != '!' || filter_query[index + 1] != '$'))) {
        return error;
    }

    const bool is_negate_join = filter_query[index] == '!';
    auto const start_index = index;
    auto size = filter_query.size();
    auto parenthesis_pos = filter_query.find('(', index + 1);
    if (parenthesis_pos == std::string::npos) {
        return error;
    }

    index = index + 1 + is_negate_join;
    ref_coll_name = filter_query.substr(index, parenthesis_pos - index);
    StringUtils::trim(ref_coll_name);

    index = parenthesis_pos;
    // The reference filter could have parenthesis inside it. $Foo((X && Y) || Z)
    int parenthesis_count = 1;
    while (++index < size && parenthesis_count > 0) {
        if (filter_query[index] == '(') {
            parenthesis_count++;
        } else if (filter_query[index] == ')') {
            parenthesis_count--;
        }
    }

    if (parenthesis_count != 0) {
        return error;
    }

    join = filter_query.substr(start_index, index - start_index);
    return Option<bool>(true);
}

Option<bool> Join::parse_reference_filter(const std::string& filter_query, std::queue<std::string>& tokens, size_t& index) {
    std::string ref_coll_name, join;
    auto parse_op = parse_reference_filter_helper(filter_query, index, ref_coll_name, join);
    if (!parse_op.ok()) {
        return parse_op;
    }

    tokens.push(join);
    return Option<bool>(true);
}

Option<bool> Join::split_reference_include_exclude_fields(const std::string& include_exclude_fields,
                                                          size_t& index, std::string& token) {
    auto ref_include_error = Option<bool>(400, "Invalid reference `" + include_exclude_fields + "` in include_fields/"
                                                    "exclude_fields, expected `$CollectionName(fieldA, ...)`.");
    auto const& size = include_exclude_fields.size();
    size_t start_index = index;
    while(++index < size && include_exclude_fields[index] != '(') {}

    if (index >= size) {
        return ref_include_error;
    }

    // In case of nested join, the reference include/exclude field could have parenthesis inside it.
    int parenthesis_count = 1;
    while (++index < size && parenthesis_count > 0) {
        if (include_exclude_fields[index] == '(') {
            parenthesis_count++;
        } else if (include_exclude_fields[index] == ')') {
            parenthesis_count--;
        }
    }

    if (parenthesis_count != 0) {
        return ref_include_error;
    }

    // In case of nested reference include, we might end up with one of the following scenarios:
    // $ref_include( $nested_ref_include(foo, strategy:merge)as nest ) as ref
    //                                                            ...^
    // $ref_include( $nested_ref_include(foo, strategy:merge)as nest, bar ) as ref
    //                                                           ...^
    auto closing_parenthesis_pos = include_exclude_fields.find(')', index);
    auto comma_pos = include_exclude_fields.find(',', index);
    auto alias_start_pos = include_exclude_fields.find(" as ", index);
    auto alias_end_pos = std::min(closing_parenthesis_pos, comma_pos);
    std::string alias;
    if (alias_start_pos != std::string::npos && alias_start_pos < alias_end_pos) {
        alias = include_exclude_fields.substr(alias_start_pos, alias_end_pos - alias_start_pos);
    }

    token = include_exclude_fields.substr(start_index, index - start_index) + " " + StringUtils::trim(alias);
    StringUtils::trim(token);

    index = alias_end_pos;
    return Option<bool>(true);
}

// Returns tri-state: Error while parsing filter_query (-1), Join not found (0), Join found (1)
int8_t skip_index_to_join(const std::string& filter_query, size_t& i) {
    auto const size = filter_query.size();
    while (i < size) {
        auto c = filter_query[i];
        if (c == ' ' || c == '(' || c == ')') {
            i++;
        } else if (c == '&' || c == '|') {
            if (i + 1 >= size || (c == '&' && filter_query[i + 1] != '&') || (c == '|' && filter_query[i + 1] != '|')) {
                return -1;
            }
            i += 2;
        } else {
            // Reference filter would start with $ symbol or !$.
            if (c == '$' || (i + 1 < size && c == '!' && filter_query[i + 1] == '$')) {
                return 1;
            } else {
                std::string token;
                filter::parse_filter_string(filter_query, token, i);
            }
        }
    }

    return 0;
}

void Join::get_reference_collection_names(const std::string& filter_query,
                                          ref_include_collection_names_t*& ref_include) {
    if (ref_include == nullptr) {
        ref_include = new ref_include_collection_names_t();
    }

    auto size = filter_query.size();
    for (size_t i = 0; i < size;) {
        auto const result = skip_index_to_join(filter_query, i);
        if (result == -1) {
            ref_include->collection_names.clear();
            return;
        } else if (result == 0) {
            break;
        }

        auto c = filter_query[i];
        const auto& is_negate_join = filter_query[i] == '!';
        auto open_paren_pos = filter_query.find('(', ++i);
        if (open_paren_pos == std::string::npos) {
            ref_include->collection_names.clear();
            return;
        }

        auto reference_collection_name = filter_query.substr(i + is_negate_join,
                                                             open_paren_pos - (i + is_negate_join));
        StringUtils::trim(reference_collection_name);
        if (!reference_collection_name.empty()) {
            ref_include->collection_names.insert(reference_collection_name);
        }

        i = open_paren_pos;
        int parenthesis_count = 1;
        while (++i < size && parenthesis_count > 0) {
            if (filter_query[i] == '(') {
                parenthesis_count++;
            } else if (filter_query[i] == ')') {
                parenthesis_count--;
            }
        }

        if (parenthesis_count != 0) {
            ref_include->collection_names.clear();
            return;
        }

        // Need to process the filter expression inside parenthesis in case of nested join.
        auto sub_filter_query = filter_query.substr(open_paren_pos + 1, i - open_paren_pos - 2);
        if (sub_filter_query.find('$') != std::string::npos) {
            get_reference_collection_names(sub_filter_query, ref_include->nested_include);
        }
    }
}

Option<bool> parse_nested_exclude(const std::string& exclude_field_exp,
                                  std::unordered_map<std::string, std::string>& ref_excludes) {
    // Format: $ref_collection_name(field_1, field_2, $nested_ref_coll(nested_field_1))
    size_t index = 0;
    while (index < exclude_field_exp.size()) {
        auto parenthesis_index = exclude_field_exp.find('(');
        auto ref_collection_name = exclude_field_exp.substr(index + 1, parenthesis_index - index - 1);
        std::string ref_fields;

        index = parenthesis_index + 1;
        auto nested_exclude_pos = exclude_field_exp.find('$', parenthesis_index);
        auto closing_parenthesis_pos = exclude_field_exp.find(')', parenthesis_index);
        size_t comma_pos;
        if (nested_exclude_pos < closing_parenthesis_pos) {
            // Nested reference exclude.
            // "... $product_variants(title, $inventory(qty)) ..."
            do {
                ref_fields += exclude_field_exp.substr(index, nested_exclude_pos - index);
                StringUtils::trim(ref_fields);
                index = nested_exclude_pos;
                std::string nested_exclude_field_exp;
                auto split_op = Join::split_reference_include_exclude_fields(exclude_field_exp, index,
                                                                             nested_exclude_field_exp);
                if (!split_op.ok()) {
                    return split_op;
                }

                auto parse_op = parse_nested_exclude(nested_exclude_field_exp, ref_excludes);
                if (!parse_op.ok()) {
                    return parse_op;
                }

                nested_exclude_pos = exclude_field_exp.find('$', index);
                closing_parenthesis_pos = exclude_field_exp.find(')', index);
                comma_pos = exclude_field_exp.find(',', index);
                index = std::min(closing_parenthesis_pos, comma_pos) + 1;
            } while (index < exclude_field_exp.size() && nested_exclude_pos < closing_parenthesis_pos);
        }

        // ... $inventory(qty) ...
        if (index < closing_parenthesis_pos) {
            ref_fields += exclude_field_exp.substr(index, closing_parenthesis_pos - index);
        }
        StringUtils::trim(ref_fields);

        ref_excludes[ref_collection_name] = ref_fields;
        index = closing_parenthesis_pos + 1;
    }

    return Option<bool>(true);
}

Option<bool> parse_ref_include_parameters(const std::string& include_field_exp, const std::string& parameters,
                                          ref_include::strategy_enum& strategy_enum, std::string& related_docs_field,
                                          std::string& sort_by_str, size_t& limit) {

    std::vector<std::string> tokens, kv_tokens;
    StringUtils::split(parameters, tokens, ",");

    for(const auto& tok : tokens) {
        kv_tokens.clear();
        StringUtils::split(tok, kv_tokens, ":");

        if (kv_tokens.size() < 2) {
            return Option<bool>(400, "Error parsing `" + include_field_exp + "`: " + tok);
        }

        if (kv_tokens[0] == ref_include::strategy_key) {
            auto const& include_strategy = kv_tokens[1];

            auto string_to_enum_op = ref_include::string_to_enum(include_strategy);
            if (!string_to_enum_op.ok()) {
                return Option<bool>(400, "Error parsing `" + include_field_exp + "`: " + string_to_enum_op.error());
            }
            strategy_enum = string_to_enum_op.get();
        } else if (kv_tokens[0] == ref_include::related_docs_count) {
            related_docs_field = kv_tokens[1];
        } else if(kv_tokens[0] == ref_include::limit) {
            limit = std::stoul(kv_tokens[1]);
        } else if(kv_tokens[0] == ref_include::sort_by) {
            if(kv_tokens.size() < 3) { //sort_by will have atleast 3 parts
                return Option<bool>(400, "Error parsing `" + include_field_exp + "`: " + tok);
            }

            std::string val = kv_tokens[1];
            for(auto i = 2; i < kv_tokens.size(); ++i) { //add and merge rest of parts
                val += std::string(":") + kv_tokens[i];
            }

            sort_by_str = val;
        } else {
            //if none of above then most likely a sort_by field
            //if not valid sort_by field then it will be validated and discarded later
            if(!sort_by_str.empty()) {
                auto str = kv_tokens[0] + ":" + kv_tokens[1];
                sort_by_str += (std::string(", ") + str);
            } else {
                return Option<bool>(400, "Unknown reference `include_fields` parameter: `" + kv_tokens[0] + "`.");
            }
        }
    }

    return Option<bool>(true);
}

Option<bool> parse_nested_include(const std::string& include_field_exp,
                                  ref_include_collection_names_t* const ref_include_coll_names,
                                  std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec) {
    // Format: $ref_collection_name(field_1, field_2, $nested_ref_coll(nested_field_1, strategy: nested_include_strategy) as nested_ref_alias, strategy: include_strategy) as ref_alias
    size_t index = 0;
    while (index < include_field_exp.size()) {
        auto parenthesis_index = include_field_exp.find('(');
        auto ref_collection_name = include_field_exp.substr(index + 1, parenthesis_index - index - 1);
        bool nest_ref_doc = true;
        std::string ref_fields, ref_alias;

        index = parenthesis_index + 1;
        auto nested_include_pos = include_field_exp.find('$', parenthesis_index);
        auto closing_parenthesis_pos = include_field_exp.rfind(')');
        auto colon_pos = include_field_exp.find(':', index);
        size_t comma_pos;
        std::vector<ref_include_exclude_fields> nested_ref_include_exclude_fields_vec;
        if (nested_include_pos < closing_parenthesis_pos) {
            // Nested reference include.
            // "... $product_variants(title, $inventory(qty, strategy:merge) as inventory, strategy :nest) as variants ..."
            do {
                ref_fields += include_field_exp.substr(index, nested_include_pos - index);
                StringUtils::trim(ref_fields);
                index = nested_include_pos;
                std::string nested_include_field_exp;
                auto split_op = Join::split_reference_include_exclude_fields(include_field_exp, index,
                                                                             nested_include_field_exp);
                if (!split_op.ok()) {
                    return split_op;
                }

                auto parse_op = parse_nested_include(nested_include_field_exp,
                                                     ref_include_coll_names == nullptr ? nullptr : ref_include_coll_names->nested_include,
                                                     nested_ref_include_exclude_fields_vec);
                if (!parse_op.ok()) {
                    return parse_op;
                }

                nested_include_pos = include_field_exp.find('$', index);
                closing_parenthesis_pos = include_field_exp.find(')', index);
                colon_pos = include_field_exp.find(':', index);
                comma_pos = include_field_exp.find(',', index);
                index = std::min(std::min(closing_parenthesis_pos, colon_pos), comma_pos) + 1;
            } while(index < include_field_exp.size() && nested_include_pos < closing_parenthesis_pos);
        }

        if (index < closing_parenthesis_pos) {
            ref_fields += include_field_exp.substr(index, closing_parenthesis_pos - index);
        }
        index = closing_parenthesis_pos;

        // ... $inventory(qty, strategy:merge) as inventory
        auto strategy_enum = ref_include::nest;
        std::string related_docs_field;
        std::string sort_by_str = "";
        size_t limit = 0;
        if (colon_pos < closing_parenthesis_pos) {
            colon_pos = ref_fields.find(':');
            auto const& parameters_start = ref_fields.rfind(',', colon_pos);
            std::string parameters;
            if (parameters_start == std::string::npos) {
                parameters = ref_fields;
                ref_fields.clear();
            } else {
                parameters = ref_fields.substr(parameters_start + 1);
                ref_fields = ref_fields.substr(0, parameters_start);
            }

            auto parse_params_op = parse_ref_include_parameters(include_field_exp, parameters, strategy_enum, related_docs_field,
                                                                sort_by_str, limit);
            if (!parse_params_op.ok()) {
                return parse_params_op;
            }
        }
        StringUtils::trim(ref_fields);

        auto as_pos = include_field_exp.find(" as ", index);
        comma_pos = include_field_exp.find(',', index);
        if (as_pos != std::string::npos && as_pos < comma_pos) {
            ref_alias = include_field_exp.substr(as_pos + 4, comma_pos - as_pos - 4);
        }

        // For an alias `foo`,
        // In case of "merge" reference doc, we need append `foo.` to all the top level keys of reference doc.
        // In case of "nest" reference doc, `foo` becomes the key with reference doc as value.
        nest_ref_doc = strategy_enum == ref_include::nest || strategy_enum == ref_include::nest_array;
        ref_alias = !ref_alias.empty() ? (StringUtils::trim(ref_alias) + (nest_ref_doc ? "" : ".")) : "";

        ref_include_exclude_fields_vec.emplace_back(ref_include_exclude_fields{ref_collection_name, ref_fields, "",
                                                                               ref_alias, strategy_enum, related_docs_field,
                                                                               sort_by_str, limit});
        ref_include_exclude_fields_vec.back().nested_join_includes = std::move(nested_ref_include_exclude_fields_vec);

        // Referenced collection in filter_by is already mentioned in include_fields.
        if (ref_include_coll_names != nullptr) {
            ref_include_coll_names->collection_names.erase(ref_collection_name);
        }
        if (comma_pos == std::string::npos) {
            break;
        }
        index = comma_pos + 1;
    }

    return Option<bool>(true);
}

Option<bool> Join::initialize_ref_include_exclude_fields_vec(const std::string& filter_query,
                                                             std::vector<std::string>& include_fields_vec,
                                                             std::vector<std::string>& exclude_fields_vec,
                                                             std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec) {
    ref_include_collection_names_t* ref_include_coll_names = nullptr;
    get_reference_collection_names(filter_query, ref_include_coll_names);
    std::unique_ptr<ref_include_collection_names_t> guard(ref_include_coll_names);

    std::vector<std::string> result_include_fields_vec;
    auto wildcard_include_all = true;
    for (auto const& include_field_exp: include_fields_vec) {
        if (include_field_exp[0] != '$') {
            if (include_field_exp == "*") {
                continue;
            }

            wildcard_include_all = false;
            result_include_fields_vec.emplace_back(include_field_exp);
            continue;
        }

        // Nested reference include.
        if (include_field_exp.find('$', 1) != std::string::npos) {
            auto parse_op = parse_nested_include(include_field_exp, ref_include_coll_names, ref_include_exclude_fields_vec);
            if (!parse_op.ok()) {
                return parse_op;
            }
            continue;
        }

        // Format: $ref_collection_name(field_1, field_2: include_strategy) as ref_alias
        auto as_pos = include_field_exp.find(" as ");
        auto ref_include = include_field_exp.substr(0, as_pos);
        auto alias = (as_pos == std::string::npos) ? "" :
                     include_field_exp.substr(as_pos + 4, include_field_exp.size() - (as_pos + 4));

        auto parenthesis_index = ref_include.find('(');
        auto ref_collection_name = ref_include.substr(1, parenthesis_index - 1);
        auto ref_fields = ref_include.substr(parenthesis_index + 1, ref_include.size() - parenthesis_index - 2);

        auto strategy_enum = ref_include::nest;
        std::string related_docs_field;
        std::string sort_by_str = "";
        size_t limit = 0;
        auto colon_pos = ref_fields.find(':');
        if (colon_pos != std::string::npos) {
            auto const& parameters_start = ref_fields.rfind(',', colon_pos);
            std::string parameters;
            if (parameters_start == std::string::npos) {
                parameters = ref_fields;
                ref_fields.clear();
            } else {
                parameters = ref_fields.substr(parameters_start + 1);
                ref_fields = ref_fields.substr(0, parameters_start);
            }

            auto parse_params_op = parse_ref_include_parameters(include_field_exp, parameters, strategy_enum, related_docs_field,
                                                                sort_by_str, limit);
            if (!parse_params_op.ok()) {
                return parse_params_op;
            }
        }

        StringUtils::trim(ref_fields);
        if (ref_fields == "*") {
            ref_fields.clear();
        }

        // For an alias `foo`,
        // In case of "merge" reference doc, we need append `foo.` to all the top level keys of reference doc.
        // In case of "nest" reference doc, `foo` becomes the key with reference doc as value.
        auto const& nest_ref_doc = strategy_enum == ref_include::nest || strategy_enum == ref_include::nest_array;
        auto ref_alias = !alias.empty() ? (StringUtils::trim(alias) + (nest_ref_doc ? "" : ".")) : "";
        ref_include_exclude_fields_vec.emplace_back(ref_include_exclude_fields{ref_collection_name, ref_fields, "",
                                                                               ref_alias, strategy_enum, related_docs_field,
                                                                               sort_by_str, limit});

        // Referenced collection in filter_by is already mentioned in include_fields.
        if (ref_include_coll_names != nullptr) {
            ref_include_coll_names->collection_names.erase(ref_collection_name);
        }
    }

    // Get all the fields of the referenced collection mentioned in the filter_by but not in include_fields.
    auto references = std::ref(ref_include_exclude_fields_vec);
    while (ref_include_coll_names != nullptr) {
        for (const auto &reference_collection_name: ref_include_coll_names->collection_names) {
            references.get().emplace_back(ref_include_exclude_fields{reference_collection_name, "", "", ""});
        }

        ref_include_coll_names = ref_include_coll_names->nested_include;
        if (references.get().empty()) {
            break;
        }
        references = std::ref(references.get().front().nested_join_includes);
    }

    std::unordered_map<std::string, std::string> ref_excludes;
    std::vector<std::string> result_exclude_fields_vec;
    for (const auto& exclude_field_exp: exclude_fields_vec) {
        if (exclude_field_exp[0] != '$') {
            result_exclude_fields_vec.emplace_back(exclude_field_exp);
            continue;
        }

        // Nested reference exclude.
        if (exclude_field_exp.find('$', 1) != std::string::npos) {
            auto parse_op = parse_nested_exclude(exclude_field_exp, ref_excludes);
            if (!parse_op.ok()) {
                return parse_op;
            }
            continue;
        }

        // Format: $ref_collection_name(field_1, field_2)
        auto parenthesis_index = exclude_field_exp.find('(');
        auto ref_collection_name = exclude_field_exp.substr(1, parenthesis_index - 1);
        auto ref_fields = exclude_field_exp.substr(parenthesis_index + 1, exclude_field_exp.size() - parenthesis_index - 2);
        if (!ref_fields.empty()) {
            ref_excludes[ref_collection_name] = ref_fields;
        }
    }

    if (!ref_excludes.empty()) {
        references = std::ref(ref_include_exclude_fields_vec);
        while (!references.get().empty()) {
            for (auto& ref_include_exclude: references.get()) {
                if (ref_excludes.count(ref_include_exclude.collection_name) == 0) {
                    continue;
                }

                ref_include_exclude.exclude_fields = ref_excludes[ref_include_exclude.collection_name];
            }

            references = std::ref(references.get().front().nested_join_includes);
        }
    }

    // Since no field of the collection being searched is mentioned in include_fields, include all the fields.
    if (wildcard_include_all) {
        result_include_fields_vec.clear();
    }

    include_fields_vec = std::move(result_include_fields_vec);
    exclude_fields_vec = std::move(result_exclude_fields_vec);

    return Option<bool>(true);
}

// If joins to the same collection are found in both `embedded_filter` and `query_filter`, remove the join from
// `embedded_filter` and merge its join condition with the `query_filter` join in the following manner:
// `$JoinCollectionName((<embedded_join_condition>) && <query_join_condition>)`
bool Join::merge_join_conditions(string& embedded_filter, string& query_filter) {
    std::unordered_map<std::string, std::string> coll_name_to_embedded_join;
    for (size_t i = 0; i < embedded_filter.size();) {
        auto const result = skip_index_to_join(embedded_filter, i);
        if (result == -1) {
            return false;
        } else if (result == 0) {
            break;
        }

        std::string ref_coll_name, join;
        if (!parse_reference_filter_helper(embedded_filter, i, ref_coll_name, join).ok()) {
            return false;
        }

        if (coll_name_to_embedded_join.find(ref_coll_name) != coll_name_to_embedded_join.end()) {
            // Don't merge in case there are multiple joins to the same collection since there can be scenarios where
            // merging don't make sense like:
            // ($businessLocation(hasOffers:=true) && $business(billingsStatus:=active)) ||
            //       ($businessLocation(hasDeal:=true) && $business(status:=active))
            return true;
        }

        coll_name_to_embedded_join[ref_coll_name] = join;
    }

    if (coll_name_to_embedded_join.empty()) { // No join found in the embedded filter_by.
        return true;
    }

    std::set<std::string> query_join_coll_names;
    for (size_t i = 0; i < query_filter.size();) {
        auto const result = skip_index_to_join(query_filter, i);
        if (result == -1) {
            return false;
        } else if (result == 0) {
            break;
        }

        // Merge join conditions
        auto const& join_start_index = i;
        auto const q_parenthesis_pos = query_filter.find('(', i + 1);
        if (q_parenthesis_pos == std::string::npos) {
            return false;
        }

        auto ref_coll_name = query_filter.substr(join_start_index + 1, q_parenthesis_pos - join_start_index - 1);
        StringUtils::trim(ref_coll_name);
        if (query_join_coll_names.find(ref_coll_name) != query_join_coll_names.end()) {
            // Multiple joins to the same collection found.
            return false;
        }

        auto it = coll_name_to_embedded_join.find(ref_coll_name);
        if (it != coll_name_to_embedded_join.end()) {
            auto const& embedded_join = it->second;

            auto const e_parenthesis_pos = embedded_join.find('(');
            if (e_parenthesis_pos == std::string::npos) {
                return false;
            }
            auto const embedded_join_condition = embedded_join.substr(e_parenthesis_pos + 1,
                                                                      embedded_join.size() - e_parenthesis_pos - 2);
            query_filter.insert(q_parenthesis_pos + 1, ("(" + embedded_join_condition + ") && "));

            query_join_coll_names.insert(ref_coll_name);
        }

        std::string join;
        if (!parse_reference_filter_helper(query_filter, i, ref_coll_name, join).ok()) {
            return false;
        }
    }

    // Erase the embedded joins that were merged into query filter.
    for (const auto& ref_coll_name: query_join_coll_names) {
        auto it = coll_name_to_embedded_join.find(ref_coll_name);
        if (it == coll_name_to_embedded_join.end()) {
            return false;
        }

        auto const& embedded_join = it->second;
        // In a complex embedded filter expression, there can be following cases:
        // 1.  (Join && ...          /  (Join || ...
        // 2.  ... && Join)          /  ... || Join)
        // 3.  ... && (Join) && ...  /  ... || (Join) || ...
        auto const& join_start_index = embedded_filter.find(embedded_join);
        if (join_start_index == std::string::npos) {
            return false;
        }

        // i and j point to start and end index of the join respectively. We will move i to left and j to right to probe
        // the embedded filter expression to check which case this join falls into.
        size_t i = join_start_index, j = join_start_index + embedded_join.size() - 1;
        while (i > 0 && embedded_filter[--i] == ' ');
        while (j < embedded_filter.size() && embedded_filter[++j] == ' ');

        if (i == 0 && j >= embedded_filter.size()) {
            // Embedded filter had only one expression.
            embedded_filter.clear();
            continue;
        }

        bool is_join_enclosed = embedded_filter[i] == '(' && embedded_filter[j] == ')';
        if (is_join_enclosed) {
            // ... ( Join ) ...
            // Still need to move both i and j.
            while (i > 0 && embedded_filter[--i] == ' ');
            while (j < embedded_filter.size() && embedded_filter[++j] == ' ');

            if (i == 0 && j >= embedded_filter.size()) {
                // Embedded join was enclosed within parentheses.
                embedded_filter.clear();
                continue;
            } else if (embedded_filter[i] == '(' && embedded_filter[j] == ')') {
                // Join enclosed inside multiple parenthesis.
                return false;
            }
        }

        // Smallest filter expression will have both field name and value that are only 1 character long like, f:v
        // So there has to be at least 5 characters after join in a complex filter expression like, &&f:v or before
        // join like, f:v||

        // Case 1.
        // Either join is the first expression in embedded filter like, Join ...
        // or it is the first expression in a sub-expression like, ... ( Join ... ) ...
        if ((i == 0 || embedded_filter[i] == '(') && j + 4 < embedded_filter.size()) {
            if ((embedded_filter[j] == '&' && embedded_filter[j + 1] == '&') ||
                (embedded_filter[j] == '|' && embedded_filter[j + 1] == '|')) {
                j++;
                while (j < embedded_filter.size() && embedded_filter[++j] == ' ');

                (is_join_enclosed || embedded_filter[i] == '$') ? embedded_filter.erase(0, j) :
                                                                    embedded_filter.erase(i + 1, j - i - 1);
            } else {
                return false;
            }
        }

        // Case 2.
        // Either join is the last expression in embedded filter like, ... Join
        // or it is the last expression in a sub-expression like, ... ( ... Join ) ...
        else if ((j >= embedded_filter.size() || embedded_filter[j] == ')') && i > 4) {
            if ((embedded_filter[i] == '&' && embedded_filter[i - 1] == '&') ||
                (embedded_filter[i] == '|' && embedded_filter[i - 1] == '|')) {
                i--;
                while (i > 0 && embedded_filter[--i] == ' ');

                embedded_filter.erase(i + 1, j - i - 1);
            } else {
                return false;
            }
        }

        // Case 3.
        // Join is in between filter expressions like, ... && Join && ...
        else if (i > 4 && j + 4 < embedded_filter.size()) {
            if ((embedded_filter[i] == '&' && embedded_filter[i - 1] == '&' &&
                 embedded_filter[j] == '&' && embedded_filter[j + 1] == '&') ||
                (embedded_filter[i] == '|' && embedded_filter[i - 1] == '|' &&
                 embedded_filter[j] == '|' && embedded_filter[j + 1] == '|')) {
                j++;

                embedded_filter.erase(i + 1, j - i);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    return true;
}
