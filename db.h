#ifndef DB_H
#define DB_H

/* Standard C++ includes */
#include <stdlib.h>
#include <iostream>

#include <vector>
#include <map>

#include <memory>

#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/resultset_metadata.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

using namespace std;

typedef map<string, map<string, map<string, string>>>  table_data;
typedef map<string, map<string, string>>			   column_data;

typedef vector<vector<string>>						   db_where;
typedef vector<map<string, string>>					   db_rows;
typedef map<string, string>							   db_row;
typedef string										   db_val;


class DB {

	unique_ptr<sql::Connection> con;

	table_data tableData;

	column_data * getInfo(string table);

public:

	DB(const char *, const char *, const char *);

	void setDatabase(string database) { con->setSchema(database); };

	db_rows fetchAll(string sql);

	db_row fetchRow(string sql);

	db_val fetchOne(string sql);

	bool query(string sql);

	// update zero or more rows
	bool update(string table, db_row row, db_where where = db_where());

	// delete zero or more rows
	bool remove(string table);

	// delete zero or more rows
	bool remove(string table, db_where where);

	// insert single row
	bool insert(string table, db_row row);

	// insert single row and update if key exists
	bool insert(string table, db_row row, db_row update);

	// insert multiple rows
	bool insert(string table, db_rows rows);

	// insert multiple rows and update if key exists
	bool insert(string table, db_rows rows, db_row update);

	// helper methods
	string repeat(string c, int n, string glue = ",");

};

#endif /* DB_H */
