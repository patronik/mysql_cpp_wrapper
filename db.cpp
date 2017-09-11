#include "db.h"

DB::DB (const char * host, const char * user, const char * pass)
{
	sql::Driver * driver = get_driver_instance();
	con.reset(driver->connect("tcp://" + string(host), user, pass));
};

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

	int i = 0;
	for (auto const & item : row) {
		sql += "`" + item.first + "` = ?";
		if (i < (row.size() - 1)) { sql += ", "; }
		i++;
	}

	string filter;
	if (where.size() > 0) {
		filter = " WHERE ";
 		for (int i = 0; i < where.size(); i++) {

			if (where[i].size() < 2) {
				cout << "condition must have at least 2 parts" << endl;
				filter = "";
				break;
			}

			// left side of expr
			if (columns->find(where[i][0]) != columns->end()) {
				filter += "`" + where[i][0] + "`";
			} else {
				filter += where[i][0];
			}

			if (where[i].size() == 2 && where[i][1].find("?") != string::npos) {
				cout << "condition with 2 parts cannot contain placeholders" << endl;
				filter = "";
				break;
			}

			// operator with placeholder(s) or final value
			filter += " " + where[i][1] + " ";

			if (i < (where.size() - 1)) { filter += " AND "; }
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
		for (int i = 0; i < where.size(); i++) {
			if (columns->find(where[i][0]) != columns->end()) {

				// skip conditions without values for placeholders
				if (where[i].size() < 3) { continue; }

				int wtype = stoi((*columns)[where[i][0]]["type"]);
				if (wtype >= sql::DataType::CHAR && wtype <= sql::DataType::LONGVARBINARY) {
					pstmt->setString(pos++, where[i][2]);

					// if this condition has more that 1 placeholders
					if (where[i].size() > 3) {
						for (int n = 3; n < where[i].size(); n++) {
							pstmt->setString(pos++, where[i][n]);
						}
					}

				} else if (wtype >= sql::DataType::BIT && wtype <= sql::DataType::BIGINT) {
					pstmt->setInt(pos++, stoi(where[i][2]));

					// if this condition has more that 1 placeholders
					if (where[i].size() > 3) {
						for (int n = 3; n < where[i].size(); n++) {
							pstmt->setInt(pos++, stoi(where[i][n]));
						}
					}

				} else if (wtype >= sql::DataType::REAL && wtype <= sql::DataType::NUMERIC) {
					pstmt->setDouble(pos++, stod(where[i][2]));

					// if this condition has more that 1 placeholders
					if (where[i].size() > 3) {
						for (int n = 3; n < where[i].size(); n++) {
							pstmt->setDouble(pos++, stod(where[i][n]));
						}
					}

				} else {
					pstmt->setString(pos++, where[i][2]);

					// if this condition has more that 1 placeholders
					if (where[i].size() > 3) {
						for (int n = 3; n < where[i].size(); n++) {
							pstmt->setString(pos++, where[i][n]);
						}
					}

				}
			} else {
				pstmt->setString(pos++, where[i][2]);

				// if this condition has more that 1 placeholders
				if (where[i].size() > 3) {
					for (int n = 3; n < where[i].size(); n++) {
						pstmt->setString(pos++, where[i][n]);
					}
				}
			}
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
