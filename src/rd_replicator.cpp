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
    columns.emplace_back("name");
    columns.emplace_back("id");

    std::string table = "stores";
    std::string prev_timestamp = "1970-01-01 00:00:01.000000";
    int64_t prev_id = -1;
    size_t limit = 1000;

    std::string field_list = StringUtils::join(columns, ", ");

    // NOTE:
    // 1. updated_at < current_ts to prevent picking up records currently being written to
    // 2. id > prev_id to avoid updating already updated records
    const std::string query_str = "SELECT " + field_list + " FROM " + table + " " +
                                  "WHERE updated_at >= '" + prev_timestamp + "' AND " +
                                  "updated_at < '" + current_ts + "' AND " +
                                  "id > " + std::to_string(prev_id) + " " +
                                  "ORDER BY updated_at ASC, id ASC LIMIT 0, " + std::to_string(limit);


    rows.clear();
    Option<bool> result = m.query(query_str, rows);

    if(!result.ok()) {
        std::cerr << "Failed to query: " << result.error() << std::endl;
    }

    for(const auto & row: rows) {
        for(size_t i = 0; i < columns.size(); i++) {
            std::cout << row[i] << ", ";
        }
        std::cout << std::endl;
    }
};

void RDReplicator::start(HttpServer* server) {

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

