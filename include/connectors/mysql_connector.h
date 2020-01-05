#include <mysql.h>
#include <iostream>
#include <string>
#include "option.h"

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

    Option<bool> query(const std::string & query_str, std::vector<char**> & rows) {
        MYSQL mysql;

        mysql_init(&mysql);

        if(!mysql_real_connect(&mysql, host.c_str(), username.c_str(), password.c_str(),
                               database.c_str(), port, NULL, 0)) {
            std::cout << "Failed to connect to database: Error:\n" << mysql_error(&mysql);
        }

        std::cout << "query_str: " << query_str << std::endl;
        int status;

        status = mysql_real_query(&mysql, query_str.c_str(), query_str.size());

        if(status != 0) {
            return Option<bool>(500, mysql_error(&mysql));
        }

        MYSQL_RES *result = mysql_store_result(&mysql);
        MYSQL_ROW row;

        while ((row = mysql_fetch_row(result))) {
            rows.push_back(row);
        }

        mysql_free_result(result);
        mysql_close(&mysql);

        return Option<bool>(true);
    }
};