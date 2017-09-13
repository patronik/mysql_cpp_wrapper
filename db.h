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

    /*
    * Helper method that retrieves information about table columns
    */
	column_data * getInfo(string table);

public:

	DB(string host, string user, string pass);

	/*
	* Select database to work with
	*/
	void setDatabase(string database)
	{
	    con->setSchema(database);
    };

	/*
    * Fetch all records returned by select query
    */
	db_rows fetchAll(string sql);

	/*
    * Fetch single record returned by select query
    */
	db_row fetchRow(string sql);

	/*
    * Fetch single column value returned by select query
    */
	db_val fetchOne(string sql);

	/*
    * Execute arbitrary query
    */
	bool query(string sql);

	/*
	* Update zero or more rows
	*/
	bool update(string table, db_row row, db_where where = db_where());

	// Delete zero or one row
	bool remove(string table, db_where where);

	/*
    * Insert single row
    */
	bool insertRow(string table, db_row row);

	/*
    * Insert single row and update all columns if key exists
    */
	bool insertRow(string table, db_row row, bool update);

	/*
    * Insert single row and update specified columns if key exists
    */
	bool insertRow(string table, db_row row, db_row update);

	/*
    * Insert multiple rows
    */
	bool insertAll(string table, db_rows rows);

	/*
    * Insert multiple rows and update all columns if key exists
    */
	bool insertAll(string table, db_rows rows, bool update);

	/*
    * Insert multiple rows and update specified columns if key exists
    */
	bool insertAll(string table, db_rows rows, db_row update);

	/*
    * Helper method that generates placeholders for queries
    */
	string repeat(string c, int n, string glue = ",");

};

#endif /* DB_H */
