#include <mysql.h>
#include <iostream>
#include <string>

class MySQLConnector {
private:
    std::string host;
    std::string username;
    std::string password;
    std::string database;
    int port;

public:

    MySQLConnector(const std::string & host, const std::string & username, const std::string & password,
                   const std::string & database, int port): host(host), username(username), password(password),
                                                            database(database), port(port) {

    }

    void runQuery() {
        MYSQL mysql;

        mysql_init(&mysql);

        if(!mysql_real_connect(&mysql, host.c_str(), username.c_str(), password.c_str(),
                               database.c_str(), port, NULL, 0)) {
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