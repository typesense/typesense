#include "join.h"

#include <collection_manager.h>
#include "collection.h"
#include "logger.h"
#include <timsort.hpp>

Option<bool> single_value_filter_query(nlohmann::json& document, const std::string& field_name,
                                       const std::string& ref_field_type, std::string& filter_query) {
    auto const& value = document[field_name];

    if (value.is_null()) {
        return Option<bool>(422, "Field `" + field_name + "` has `null` value.");
    }

    if (value.is_string() && ref_field_type == field_types::STRING) {
        filter_query += value.get<std::string>();
    } else if (value.is_number_integer() && (ref_field_type == field_types::INT64 ||
                                             (ref_field_type == field_types::INT32 &&
                                              StringUtils::is_int32_t(std::to_string(value.get<int64_t>()))))) {
        filter_query += std::to_string(value.get<int64_t>());
    } else {
        return Option<bool>(400, "Field `" + field_name + "` must have `" + ref_field_type + "` value.");
    }

    return Option<bool>(true);
}

Option<bool> Join::add_reference_helper_fields(nlohmann::json& document,
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

        auto reference_pair = pair.second;
        auto reference_collection_name = reference_pair.collection;
        auto reference_field_name = reference_pair.field;
        auto& cm = CollectionManager::get_instance();
        auto ref_collection = cm.get_collection(reference_collection_name);

        if (is_update && document.contains(reference_helper_field) &&
            (!document[field_name].is_array() || document[field_name].size() == document[reference_helper_field].size())) {
            // No need to look up the reference collection since reference helper field is already populated.
            // Saves needless computation in cases where references are known beforehand. For example, when cascade
            // deleting the related docs.
            document[fields::reference_helper_fields] += reference_helper_field;
            continue;
        }

        if (ref_collection == nullptr && is_async_reference) {
            document[fields::reference_helper_fields] += reference_helper_field;
            if (document[field_name].is_array()) {
                document[reference_helper_field] = nlohmann::json::array();
                // Having the same number of values makes it easier to update the references in the future.
                document[reference_helper_field].insert(document[reference_helper_field].begin(),
                                                        document[field_name].size(),
                                                        Collection::reference_helper_sentinel_value);
            } else {
                document[reference_helper_field] = Collection::reference_helper_sentinel_value;
            }

            continue;
        } else if (ref_collection == nullptr) {
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
                    auto ref_doc_id_op = ref_collection->doc_id_to_seq_id_with_lock(id);
                    if (!ref_doc_id_op.ok() && is_async_reference) {
                        auto const& value = nlohmann::json::array({i, Collection::reference_helper_sentinel_value});
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
                    auto ref_doc_id_op = ref_collection->doc_id_to_seq_id_with_lock(id);
                    if (!ref_doc_id_op.ok() && is_async_reference) {
                        document[reference_helper_field] += Collection::reference_helper_sentinel_value;
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
                auto ref_doc_id_op = ref_collection->doc_id_to_seq_id_with_lock(id);
                if (!ref_doc_id_op.ok() && is_async_reference) {
                    document[reference_helper_field] = Collection::reference_helper_sentinel_value;
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

        if (ref_collection->get_schema().count(reference_field_name) == 0) {
            return Option<bool>(400, "Referenced field `" + reference_field_name +
                                     "` not found in the collection `" += reference_collection_name + "`.");
        }

        auto const ref_field = ref_collection->get_schema().at(reference_field_name);
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
                auto filter_ids_op = ref_collection->get_filter_ids(filter_query, filter_result);
                if (!filter_ids_op.ok()) {
                    return filter_ids_op;
                }

                if (filter_result.count == 0 && is_async_reference) {
                    document[reference_helper_field] += nlohmann::json::array({i, Collection::reference_helper_sentinel_value});
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

            for (const auto &item: document[field_name].items()) {
                auto const& item_value = item.value();
                if (item_value.is_string() && ref_field_type == field_types::STRING) {
                    filter_values.emplace_back(item_value.get<std::string>());
                } else if (item_value.is_number_integer() && (ref_field_type == field_types::INT64 ||
                                                              (ref_field_type == field_types::INT32 &&
                                                               StringUtils::is_int32_t(std::to_string(item_value.get<int64_t>()))))) {
                    filter_values.emplace_back(std::to_string(item_value.get<int64_t>()));
                } else {
                    return Option<bool>(400, "Field `" + field_name + "` must only have `" += ref_field_type + "` values.");
                }
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
            auto filter_ids_op = ref_collection->get_filter_ids(filter_query, filter_result);
            if (!filter_ids_op.ok()) {
                return filter_ids_op;
            }

            if (filter_result.count == 0 && is_async_reference) {
                if (is_reference_array_field) {
                    document[reference_helper_field] += Collection::reference_helper_sentinel_value;
                } else {
                    document[reference_helper_field] = Collection::reference_helper_sentinel_value;
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
    auto& cm = CollectionManager::get_instance();
    auto ref_collection = cm.get_collection(ref_collection_name);
    if (ref_collection == nullptr) {
        return Option<bool>(400, "Referenced collection `" + ref_collection_name + "` in `include_fields` not found.");
    }

    auto const& alias = ref_include_exclude.alias;
    auto const& strategy = ref_include_exclude.strategy;
    auto error_prefix = "Referenced collection `" + ref_collection_name + "`: ";

    // One-to-one relation.
    if (strategy != ref_include::nest_array && !is_reference_array && references.count == 1) {
        auto ref_doc_seq_id = references.docs[0];

        nlohmann::json ref_doc;
        auto get_doc_op = ref_collection->get_document_from_store(ref_doc_seq_id, ref_doc);
        if (!get_doc_op.ok()) {
            if (ref_doc_seq_id == Collection::reference_helper_sentinel_value) {
                return Option<bool>(true);
            }
            return Option<bool>(get_doc_op.code(), error_prefix + get_doc_op.error());
        }

        Collection::remove_flat_fields(ref_doc);
        Collection::remove_reference_helper_fields(ref_doc);

        auto prune_op = Collection::prune_doc(ref_doc, ref_include_fields_full, ref_exclude_fields_full);
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
                                                                ref_collection.get(),
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
    for (uint32_t i = 0; i < references.count; i++) {
        auto ref_doc_seq_id = references.docs[i];

        nlohmann::json ref_doc;
        std::string key;
        auto const& nest_ref_doc = (strategy == ref_include::nest || strategy == ref_include::nest_array);

        auto get_doc_op = ref_collection->get_document_from_store(ref_doc_seq_id, ref_doc);
        if (!get_doc_op.ok()) {
            // Referenced document is not yet indexed.
            if (ref_doc_seq_id == Collection::reference_helper_sentinel_value) {
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
                                                                ref_collection.get(),
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

Option<bool> Join::include_references(nlohmann::json& doc, const uint32_t& seq_id, Collection *const collection,
                                      const std::map<std::string, reference_filter_result_t>& reference_filter_results,
                                      const std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec,
                                      const nlohmann::json& original_doc) {
    for (auto const& ref_include_exclude: ref_include_exclude_fields_vec) {
        auto ref_collection_name = ref_include_exclude.collection_name;

        auto& cm = CollectionManager::get_instance();
        auto ref_collection = cm.get_collection(ref_collection_name);
        if (ref_collection == nullptr) {
            return Option<bool>(400, "Referenced collection `" + ref_collection_name + "` in `include_fields` not found.");
        }
        // `CollectionManager::get_collection` accounts for collection alias being used and provides pointer to the
        // original collection.
        ref_collection_name = ref_collection->get_name();

        auto const joined_on_ref_collection = reference_filter_results.count(ref_collection_name) > 0,
                has_filter_reference = (joined_on_ref_collection &&
                                        reference_filter_results.at(ref_collection_name).count > 0);
        auto doc_has_reference = false, joined_coll_has_reference = false;

        // Reference include_by without join, check if doc itself contains the reference.
        if (!joined_on_ref_collection && collection != nullptr) {
            doc_has_reference = ref_collection->is_referenced_in(collection->get_name());
        }

        std::string joined_coll_having_reference;
        // Check if the joined collection has a reference.
        if (!joined_on_ref_collection && !doc_has_reference) {
            for (const auto &reference_filter_result: reference_filter_results) {
                joined_coll_has_reference = ref_collection->is_referenced_in(reference_filter_result.first);
                if (joined_coll_has_reference) {
                    joined_coll_having_reference = reference_filter_result.first;
                    break;
                }
            }
        }

        if (!has_filter_reference && !doc_has_reference && !joined_coll_has_reference) {
            continue;
        }

        std::vector<std::string> ref_include_fields_vec, ref_exclude_fields_vec;
        StringUtils::split(ref_include_exclude.include_fields, ref_include_fields_vec, ",");
        StringUtils::split(ref_include_exclude.exclude_fields, ref_exclude_fields_vec, ",");

        spp::sparse_hash_set<std::string> ref_include_fields, ref_exclude_fields;
        ref_include_fields.insert(ref_include_fields_vec.begin(), ref_include_fields_vec.end());
        ref_exclude_fields.insert(ref_exclude_fields_vec.begin(), ref_exclude_fields_vec.end());

        tsl::htrie_set<char> ref_include_fields_full, ref_exclude_fields_full;
        auto include_exclude_op = ref_collection->populate_include_exclude_fields_lk(ref_include_fields,
                                                                                     ref_exclude_fields,
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
            auto get_reference_field_op = ref_collection->get_referenced_in_field_with_lock(collection->get_name());
            if (!get_reference_field_op.ok()) {
                continue;
            }
            auto const& field_name = get_reference_field_op.get();
            auto const& reference_helper_field_name = field_name + fields::REFERENCE_HELPER_FIELD_SUFFIX;
            if (collection->get_schema().count(reference_helper_field_name) == 0) {
                continue;
            }

            if (collection->get_object_reference_helper_fields().count(field_name) != 0) {
                std::vector<std::string> keys;
                StringUtils::split(field_name, keys, ".");
                auto const& key = keys[0];

                if (!doc.contains(key)) {
                    if (!original_doc.contains(key)) {
                        return Option<bool>(400, "Could not find `" + key +
                                                 "` key in the document to include the referenced document.");
                    }

                    // The key is excluded from the doc by the query, inserting empty object(s) so referenced doc can be
                    // included in it.
                    if (original_doc[key].is_array()) {
                        doc[key] = nlohmann::json::array();
                        doc[key].insert(doc[key].begin(), original_doc[key].size(), nlohmann::json::object());
                    } else {
                        doc[key] = nlohmann::json::object();
                    }
                }

                if (doc[key].is_array()) {
                    for (uint32_t i = 0; i < doc[key].size(); i++) {
                        uint32_t ref_doc_id;
                        auto op = collection->get_object_array_related_id(reference_helper_field_name, seq_id, i, ref_doc_id);
                        if (!op.ok()) {
                            if (op.code() == 404) { // field_name is not indexed.
                                break;
                            } else { // No reference found for this object.
                                continue;
                            }
                        }

                        reference_filter_result_t result(1, new uint32_t[1]{ref_doc_id});
                        prune_doc_op = prune_ref_doc(doc[key][i], result,
                                                     ref_include_fields_full, ref_exclude_fields_full,
                                                     false, ref_include_exclude);
                        if (!prune_doc_op.ok()) {
                            return prune_doc_op;
                        }
                    }
                } else {
                    std::vector<uint32_t> ids;
                    auto get_references_op = collection->get_related_ids(field_name, seq_id, ids);
                    if (!get_references_op.ok()) {
                        LOG(ERROR) << "Error while getting related ids: " + get_references_op.error();
                        continue;
                    }
                    reference_filter_result_t result(ids.size(), &ids[0]);
                    prune_doc_op = prune_ref_doc(doc[key], result, ref_include_fields_full, ref_exclude_fields_full,
                                                 collection->get_schema().at(field_name).is_array(), ref_include_exclude);
                    result.docs = nullptr;
                }
            } else {
                std::vector<uint32_t> ids;
                auto get_references_op = collection->get_related_ids(field_name, seq_id, ids);
                if (!get_references_op.ok()) {
                    LOG(ERROR) << "Error while getting related ids: " + get_references_op.error();
                    continue;
                }
                reference_filter_result_t result(ids.size(), &ids[0]);
                prune_doc_op = prune_ref_doc(doc, result, ref_include_fields_full, ref_exclude_fields_full,
                                             collection->get_schema().at(field_name).is_array(), ref_include_exclude);
                result.docs = nullptr;
            }
        } else if (joined_coll_has_reference) {
            auto joined_collection = cm.get_collection(joined_coll_having_reference);
            if (joined_collection == nullptr) {
                continue;
            }

            auto reference_field_name_op = ref_collection->get_referenced_in_field_with_lock(joined_coll_having_reference);
            if (!reference_field_name_op.ok() || joined_collection->get_schema().count(reference_field_name_op.get()) == 0) {
                continue;
            }

            auto const& reference_field_name = reference_field_name_op.get();
            auto const& reference_filter_result = reference_filter_results.at(joined_coll_having_reference);
            auto const& count = reference_filter_result.count;
            std::vector<uint32_t> ids;
            ids.reserve(count);
            for (uint32_t i = 0; i < count; i++) {
                joined_collection->get_related_ids_with_lock(reference_field_name, reference_filter_result.docs[i], ids);
            }
            if (ids.empty()) {
                continue;
            }

            gfx::timsort(ids.begin(), ids.end());
            ids.erase(unique(ids.begin(), ids.end()), ids.end());

            reference_filter_result_t result;
            result.count = ids.size();
            result.docs = &ids[0];
            prune_doc_op = prune_ref_doc(doc, result, ref_include_fields_full, ref_exclude_fields_full,
                                         joined_collection->get_schema().at(reference_field_name).is_array(),
                                         ref_include_exclude);
            result.docs = nullptr;
        }

        if (!prune_doc_op.ok()) {
            return prune_doc_op;
        }
    }

    return Option<bool>(true);
}