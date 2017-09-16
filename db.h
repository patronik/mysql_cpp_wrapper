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

	/**
	* Methods responsible for preparing parameters for query
	*/
	void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, char p);

    void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, int p);

    void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, const char * p);

    void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, double p);

    void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, float p);

    void setParam(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, string p);

    /**
    * This method accepts variable number of arguments and uses recursion to process all of them.
    */
    template<typename T, typename... Args>
    unique_ptr<sql::ResultSet> select(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, T p, Args... args) {
        setParam(pstmt, p);
        return select(pstmt, args...);
    }

    /**
    * This method is a base case for previous method and is a place where recursion stops.
    * It gets called when there are only 1 argument left and after processing one we can execute prepared statement.
    */
    template<typename T>
    unique_ptr<sql::ResultSet> select(pair<unique_ptr<sql::PreparedStatement>, int> & pstmt, T last) {
        setParam(pstmt, last);
        unique_ptr<sql::ResultSet> res(pstmt.first->executeQuery());
        return res;
    }

public:

	DB(string host, string user, string pass);

	/**
	* Select database to work with
	*/
	void setDatabase(string database)
	{
	    con->setSchema(database);
    };

	/**
    * Fetch all records returned by select query
    */
	db_rows fetchAll(string sql);

	/**
    * Fetch all records returned by select query.
    * Accepts 1 argument to set into query.
    */
    template<typename T>
	db_rows fetchAll(string sql, T p) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p);
        sql::ResultSetMetaData * info = res->getMetaData();
        db_rows rows;
        int numcols = info->getColumnCount();
        while (res->next()) {
            db_row row;
            for (int i = 1; i <= numcols; i++) {
                row[info->getColumnName(i)] = res->getString(i);
            }
            rows.push_back(row);
        }
        return rows;
	};

	/**
    * Fetch all records returned by select query.
    * Accepts variable number of arguments.
    */
    template<typename T, typename... Args>
	db_rows fetchAll(string sql, T p, Args... args) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p, args...);
        sql::ResultSetMetaData * info = res->getMetaData();
        db_rows rows;
        int numcols = info->getColumnCount();
        while (res->next()) {
            db_row row;
            for (int i = 1; i <= numcols; i++) {
                row[info->getColumnName(i)] = res->getString(i);
            }
            rows.push_back(row);
        }
        return rows;
	};

	/**
    * Fetch single record returned by select query
    */
	db_row fetchRow(string sql);

	/**
    * Fetch single record returned by select query.
    * Accepts 1 argument to set into query.
    */
    template<typename T>
	db_row fetchRow(string sql, T p) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p);
        sql::ResultSetMetaData * info = res->getMetaData();
        db_row row;
        int numcols = info->getColumnCount();
        if (res->next()) {
            for (int i = 1; i <= numcols; i++) {
                row[info->getColumnName(i)] = res->getString(i);
            }
        }
        return row;
	};

	/**
    * Fetch single record returned by select query.
    * Accepts variable number of arguments.
    */
    template<typename T, typename... Args>
	db_row fetchRow(string sql, T p, Args... args) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p, args...);
        sql::ResultSetMetaData * info = res->getMetaData();
        db_row row;
        int numcols = info->getColumnCount();
        if (res->next()) {
            for (int i = 1; i <= numcols; i++) {
                row[info->getColumnName(i)] = res->getString(i);
            }
        }
        return row;
	};

	/**
    * Fetch single column value returned by select query
    */
	db_val fetchOne(string sql);

	/**
    * Fetch single column value returned by select query.
    * Accepts 1 argument to set into query.
    */
    template<typename T>
	db_val fetchOne(string sql, T p) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p);
        db_val val("");
        if (res->next()) {
            val = res->getString(1);
        }
        return val;
	};

	/**
    * Fetch single column value returned by select query.
    * Accepts variable number of arguments.
    */
    template<typename T, typename... Args>
	db_val fetchOne(string sql, T p, Args... args) {
	    pair<unique_ptr<sql::PreparedStatement>, int> pstmt
            = make_pair(unique_ptr<sql::PreparedStatement>(con->prepareStatement(sql)), 1);
        unique_ptr<sql::ResultSet> res = select(pstmt, p, args...);
        db_val val("");
        if (res->next()) {
            val = res->getString(1);
        }
        return val;
	};


	/**
    * Execute arbitrary query
    */
	bool query(string sql);

	/**
	* Update zero or more rows
	*/
	bool update(string table, db_row row, db_where where = db_where());

	/**
    * Delete zero or one row
    */
	bool remove(string table, db_where where = db_where());

	/**
    * Insert single row
    */
	bool insertRow(string table, db_row row);

	/**
    * Insert single row and update all columns if key exists
    */
	bool insertRow(string table, db_row row, bool update);

	/**
    * Insert single row and update specified columns if key exists
    */
	bool insertRow(string table, db_row row, db_row update);

	/**
    * Insert multiple rows
    */
	bool insertAll(string table, db_rows rows);

	/**
    * Insert multiple rows and update all columns if key exists
    */
	bool insertAll(string table, db_rows rows, bool update);

	/**
    * Insert multiple rows and update specified columns if key exists
    */
	bool insertAll(string table, db_rows rows, db_row update);

	/**
    * Helper method that generates placeholders for queries
    */
	string repeat(string c, int n, string glue = ",");

};

#endif /* DB_H */
