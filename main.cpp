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

		db.query("SET global general_log = 1");
		db.query("SET global log_output = 'table'");
		db.setDatabase("mysql");
		db.query("TRUNCATE TABLE `general_log`");

		db.setDatabase("sandbox");

		db.query("TRUNCATE TABLE `test`");
		db.query("INSERT INTO `test` (id) VALUES (15)");
		db.query("INSERT INTO `test` (id) VALUES (25)");
		db.query("INSERT INTO `test` (id) VALUES (35)");

        db.update("test", {{ "id", "125" }}, {{ "id", "= ?", "15" }});
		db.update("test", {{ "id", "225" }}, {{ "id", "IN(25)"}});
		db.update("test", {{ "id", "325" }}, {{ "id", "IN(?, ?, ?)", "15", "65", "75"}});

		db.update("test", {{ "id", "125" }}, {{ "id", "= ?", "15" }, {"OR"}, { "id", "= ?", "16"}});
		db.update("test", {{ "id", "125" }}, {{ "id", " > 156"}, {"AND"}, { "id", "= ?", "16"}});
		db.update("test", {{ "id", "325" }}, {{ "id", "IN(?, ?, ?)", "15", "65", "75"}, {"OR"}, {"id", " > 156"}, {"OR"}, {"id", "= ?", "66"}, {"id", "= ?", "166"}, {"id", "= ?", "66"}, {"id", "= ?", "66"}, {"id", "= ?", "66"}});

		db.query("SET global general_log = 0");

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
