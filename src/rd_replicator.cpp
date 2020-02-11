#include <iostream>
#include "rd_replicator.h"
#include "collection_manager.h"
#include "mysql_connector.h"

void RDReplicator::replicate() {
    MySQLConnector m("localhost", "root", "", "typesense", 3306);

    // first fetch current ts as reference for running queries against both updates and deletes
    std::vector<char**> rows;
    m.query("SELECT NOW()", rows);
    std::string current_ts = rows[0][0];

    std::vector<std::string> columns;
    columns.emplace_back("id");
    columns.emplace_back("name");
    columns.emplace_back("updated_at");

    std::string view_table = "stores";
    if(last_updated_at.empty()) {
        last_updated_at = "1970-01-01 00:00:01.000000";
    }
    size_t limit = 1000;

    std::string field_list = StringUtils::join(columns, ", ");

    // NOTE:
    // 1. updated_at < current_ts to prevent picking up records currently being written to
    // 2. id > last_upserted_id to avoid updating already updated records
    const std::string upsert_query = "SELECT " + field_list + " FROM " + view_table + " " +
                                     "WHERE updated_at >= '" + last_updated_at + "' AND " +
                                     "updated_at < '" + current_ts + "' AND " +
                                     "id > " + std::to_string(last_upserted_id) + " " +
                                     "ORDER BY updated_at ASC, id ASC LIMIT 0, " + std::to_string(limit);

    // NOTE:
    // 1. `id` refers to the primary key of the deletion entries
    // 2. `record_id` is the original record's id
    std::string deleted_table = view_table + "_deleted";
    const std::string delete_query = "SELECT record_id FROM " + deleted_table + " " +
                                     "WHERE id > " + std::to_string(last_deleted_id);

    upsert_delete(columns, upsert_query, delete_query);

}

void RDReplicator::upsert_delete(const std::vector<std::string> & columns,
                                 const std::string & upsert_query,
                                 const std::string & delete_query) {
    MySQLConnector m("localhost", "root", "", "typesense", 3306);

    std::vector<char**> updated_rows;
    Option<bool> update_query_res = m.query(upsert_query, updated_rows);

    if(!update_query_res.ok()) {
        std::cerr << "Failed to query: " << update_query_res.error() << std::endl;
    }

    for(const auto & row: updated_rows) {
        for(size_t i = 0; i < columns.size(); i++) {
            std::cout << row[i] << ", ";
        }
        last_upserted_id = std::stoi(row[0]);
        last_updated_at = row[2];
        std::cout << std::endl;
    }

    std::vector<char**> deleted_rows;
    Option<bool> delete_query_res = m.query(delete_query, deleted_rows);

    if(!delete_query_res.ok()) {
        std::cerr << "Failed to query: " << delete_query_res.error() << std::endl;
    }

    for(const auto & row: deleted_rows) {
        for(size_t i = 0; i < columns.size(); i++) {
            std::cout << row[i] << ", ";
        }
        last_deleted_id = std::stoi(row[0]);
        std::cout << std::endl;
    }
}

void RDReplicator::start(HttpServer* server) {
    // TODO: these must be fetched from DB
    last_upserted_id = -1;
    last_deleted_id = -1;

    size_t total_runs = 0;

    while(true) {
        std::cout << "Replication run number: " << total_runs << std::endl;

        if(total_runs++ % 20 == 0) {
            // roughly every 60 seconds
            // std::cout << "Replication run number: " << total_runs;
        }

        replicate();

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void RDReplicator::on_replication_event(void *data) {

}

