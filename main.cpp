#include "db.h"

int main(int argc, char * argv[]) {
	try {
		DB db("127.0.0.1:3306", "root", "123");

		/*
		db.setDatabase("all_ips");
		db.query("INSERT INTO ip_address (address) VALUES ('127.0.0.1')");
		db.query("INSERT INTO ip_address (address) VALUES ('127.0.0.2')");
		db.query("INSERT INTO ip_address (address) VALUES ('127.0.0.3')");

		db_rows rows = db.fetchAll("SELECT * FROM ip_address");
		for (auto const & row : rows) {
			for (auto const & item : row) {
				cout << item.first << " = " << item.second << " ";
			}
			cout << std::endl;
		}
		*/

		//db.setDatabase("southco");

		//db_row row = { { "topic_id", "500" }};
		//db_where where = { { "topic_id", "= ?", "17" }, { "user_id", "IN(?, ?)", "59", "58"} };
		//bool res = db.update("forum_thanks", row, where);

	}
	catch (sql::SQLException &e) {
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line "
			<< __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	return 0;
}
