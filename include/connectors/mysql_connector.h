#include <mysql.h>
#include <iostream>

class MySQLConnector {

public:
    MySQLConnector() {

    }

    void runQuery() {
        MYSQL mysql;

        mysql_init(&mysql);

        if(!mysql_real_connect(&mysql, "localhost", "typesense", "typesense", "typesense", 3306, NULL, 0)) {
            std::cout << "Failed to connect to database: Error: %s\n" << mysql_error(&mysql);
        }

        const char *query = "SELECT * FROM stores";
        mysql_real_query(&mysql, query, strlen(query));

        MYSQL_RES *result = mysql_store_result(&mysql);
        const size_t num_fields = mysql_field_count(&mysql);

        MYSQL_ROW row;

        while ((row = mysql_fetch_row(result))) {
            for(size_t i = 0; i < num_fields; i++) {
                printf("%s, ", row[i]);
            }
            printf("\n");
        }

        mysql_free_result(result);
    }
};