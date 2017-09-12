#include "db.h"

DB::DB (const char * host, const char * user, const char * pass)
{
	sql::Driver * driver = get_driver_instance();
	con.reset(driver->connect("tcp://" + string(host), user, pass));
};

/**
* Update 'table' with data passed in 'row' according to optional filtering conditions passed in 'where'
*
* 'where' is a set of conditions that should be applied to the query.
* Where explained:
* If where condition has 1 part - this is joiner between conditions
* If where condition has 2 parts - first part is left side of expression,
*    second part is an operator and final value without placeholders
* If where condition has 3 or more parts - first part is left side of expression,
* second part is an operator with placeholders, third and rest are values for placeholders
*
**/
bool DB::update(string table, db_row row, db_where where)
{
	column_data * columns = getInfo(table);
	if (columns == nullptr) {
		cout << "table '" << table << "' does not exist" << endl;
		return false;
	}

	if (!(row.size() > 0)) {
		cout << "update data is not provided" << endl;
		return false;
	}

	string sql("UPDATE `" + table + "` SET ");

	auto i = 0;
	for (auto const & item : row) {
		sql += "`" + item.first + "` = ?";
		if (i < (row.size() - 1)) { sql += ", "; }
		i++;
	}

	string filter;
	if (where.size() > 0) {
		filter = " WHERE ";
 		for (auto i = 0; i < where.size(); i++) {

			if (where[i].empty()) {
				cout << "condition cannot be empty" << endl;
				filter = "";
				break;
			}

			if (where[i].size() == 2 && where[i][1].find("?") != string::npos) {
				cout << "placeholders are not allowed" << endl;
				filter = "";
				break;
			}

			// join conditions
			if (i > 0) {
                if (where[i].size() == 1) {
                    transform(where[i][0].begin(), where[i][0].end(),
                              where[i][0].begin(), ::toupper);
                   if (where[i][0] != "OR" && where[i][0] != "AND") {
                        cout << "unsupported operator '" << where[i][0] << "'" << endl;
                        filter = "";
                        break;
                   }
                   filter += " " + where[i][0] + " ";
                   continue;
                } else {
                    if (where[i-1].size() != 1) {
                        filter += " AND ";
                    }
                }
			}

			// left side of expression
			if (columns->find(where[i][0]) != columns->end()) {
				filter += "`" + where[i][0] + "`";
			} else {
				filter += where[i][0];
			}

			// operator with placeholder(s) or final value
			filter += " " + where[i][1] + " ";

		}
		sql += filter;
	}

	unique_ptr<sql::PreparedStatement> pstmt(con->prepareStatement(sql));
	int pos = 1;
	for (auto & item : row) {
		int ctype = stoi((*columns)[item.first]["type"]);
		if (ctype >= sql::DataType::CHAR && ctype <= sql::DataType::LONGVARBINARY) {
			pstmt->setString(pos++, item.second);
		} else if (ctype >= sql::DataType::BIT && ctype <= sql::DataType::BIGINT) {
			pstmt->setInt(pos++, stoi(item.second));
		} else if (ctype >= sql::DataType::REAL && ctype <= sql::DataType::NUMERIC) {
			pstmt->setDouble(pos++, stod(item.second));
		} else {
			pstmt->setString(pos++, item.second);
		}
	}

	if (where.size() > 0 && !filter.empty()) {
		for (auto i = 0; i < where.size(); i++) {
            // skip conditions without values for placeholders
            if (where[i].size() < 3) { continue; }
			if (columns->find(where[i][0]) != columns->end()) {
				int wtype = stoi((*columns)[where[i][0]]["type"]);
				if (wtype >= sql::DataType::CHAR && wtype <= sql::DataType::LONGVARBINARY) {
					pstmt->setString(pos++, where[i][2]);
					if (where[i].size() > 3) {
						for (auto n = 3; n < where[i].size(); n++) {
							pstmt->setString(pos++, where[i][n]);
						}
					}
				} else if (wtype >= sql::DataType::BIT && wtype <= sql::DataType::BIGINT) {
					pstmt->setInt(pos++, stoi(where[i][2]));
					if (where[i].size() > 3) {
						for (auto n = 3; n < where[i].size(); n++) {
							pstmt->setInt(pos++, stoi(where[i][n]));
						}
					}
				} else if (wtype >= sql::DataType::REAL && wtype <= sql::DataType::NUMERIC) {
					pstmt->setDouble(pos++, stod(where[i][2]));
					if (where[i].size() > 3) {
						for (auto n = 3; n < where[i].size(); n++) {
							pstmt->setDouble(pos++, stod(where[i][n]));
						}
					}
				} else {
					pstmt->setString(pos++, where[i][2]);
					if (where[i].size() > 3) {
						for (auto n = 3; n < where[i].size(); n++) {
							pstmt->setString(pos++, where[i][n]);
						}
					}
				}
			} else {
				pstmt->setString(pos++, where[i][2]);
				if (where[i].size() > 3) {
					for (auto n = 3; n < where[i].size(); n++) {
						pstmt->setString(pos++, where[i][n]);
					}
				}
			}
		}
	}

	pstmt->executeUpdate();
	return true;
};

/*
* Insert single row
*/
bool DB::insertRow(string table, db_row row)
{
    return insertRow(table, row, db_row());
};

/*
* Insert single row and update all columns if key exists
*/
bool DB::insertRow(string table, db_row row, bool update)
{
    db_row data;
    if (update) {
        for (auto const & item : row) {
            data[item.first] = "VALUES(`" + item.first + "`)";
        }
    }
    return insertRow(table, row, data);
};

/*
* Insert single row and update specified columns if key exists
*/
bool DB::insertRow(string table, db_row row, db_row update)
{
    column_data * columns = getInfo(table);
	if (columns == nullptr) {
		cout << "table '" << table << "' does not exist" << endl;
		return false;
	}

	if (!(row.size() > 0)) {
		cout << "insert data is not provided" << endl;
		return false;
	}

	string sql("INSERT INTO `" + table + "` (");

	auto i = 0;
	for (auto const & item : row) {
		sql += "`" + item.first + "`";
		if (i < (row.size() - 1)) { sql += ", "; }
		i++;
	}

	sql += ") VALUES (" + repeat("?", row.size()) + ")";

	if (!update.empty()) {
        sql += " ON DUPLICATE KEY UPDATE ";
        auto j = 0;
        for (auto const & col : update) {
            sql += "`" + col.first + "` = " + col.second;
            if (j < (update.size() - 1)) { sql += ", "; }
            j++;
        }
	}

	unique_ptr<sql::PreparedStatement> pstmt(con->prepareStatement(sql));
	int pos = 1;
	for (auto & item : row) {
		int ctype = stoi((*columns)[item.first]["type"]);
		if (ctype >= sql::DataType::CHAR && ctype <= sql::DataType::LONGVARBINARY) {
			pstmt->setString(pos++, item.second);
		} else if (ctype >= sql::DataType::BIT && ctype <= sql::DataType::BIGINT) {
			pstmt->setInt(pos++, stoi(item.second));
		} else if (ctype >= sql::DataType::REAL && ctype <= sql::DataType::NUMERIC) {
			pstmt->setDouble(pos++, stod(item.second));
		} else {
			pstmt->setString(pos++, item.second);
		}
	}

	pstmt->executeUpdate();
	return true;
};

db_rows DB::fetchAll(string sql)
{
	unique_ptr<sql::Statement> stmt(con->createStatement());
	unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));
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

db_row DB::fetchRow(string sql)
{
	unique_ptr<sql::Statement> stmt(con->createStatement());
	unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));
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

db_val DB::fetchOne(string sql)
{
	db_val val("");
	unique_ptr<sql::Statement> stmt(con->createStatement());
	unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));
	if (res->next()) {
		val = res->getString(1);
	}
	return val;
};

bool DB::query(string sql)
{
	unique_ptr<sql::Statement> stmt(con->createStatement());
	return stmt->execute(sql);
};

string DB::repeat(string c, int n, string glue)
{
    string res;
    while (n > 0) {
        res += c;
        if (n-- > 1) { res += glue; }
    }
    return res;
};

column_data * DB::getInfo(string table)
{
	if (tableData.find(table) == tableData.end()) {
		string val = fetchOne("SHOW TABLES LIKE '" + table + "'");
		if (val.empty()) {
			return nullptr;
		}
		unique_ptr<sql::Statement> stmt(con->createStatement());
		unique_ptr<sql::ResultSet> res(
			stmt->executeQuery("SELECT * FROM `" + table + "` LIMIT 1")
			);
		sql::ResultSetMetaData * info = res->getMetaData();
		int numcols = info->getColumnCount();
		for (int i = 1; i <= numcols; i++) {
			tableData[table][info->getColumnName(i)]["type"]
				= to_string(info->getColumnType(i));
		}
	}
	return &tableData[table];
};
