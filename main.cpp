#include "db.h"

int main(int argc, char * argv[]) {
	try {
		DB db("127.0.0.1:3306", "root", "123");

		db.setDatabase("mysql");
		db.query("TRUNCATE TABLE `general_log`");

		db.query("SET global general_log = 1");
		db.query("SET global log_output = 'table'");

        db.query("CREATE DATABASE IF NOT EXISTS `sandbox` CHARACTER SET utf8");
		db.setDatabase("sandbox");

		db.query("CREATE TABLE IF NOT EXISTS `test` ("
                 "`id` INT(11) DEFAULT NULL,"
                 "`name` VARCHAR(255) DEFAULT NULL,"
                 "`price` DECIMAL(10,2) DEFAULT NULL,"
                 "`description` TEXT DEFAULT NULL"
                 ") ENGINE=InnoDB DEFAULT CHARSET=utf8");
		db.query("TRUNCATE TABLE `test`");

		db.query("INSERT INTO `test` (id) VALUES (5)");
		db.query("INSERT INTO `test` (id) VALUES (6)");
		db.query("INSERT INTO `test` (id) VALUES (7)");

		db.insertRow("test", {{"id", "15"}, {"name", "jacob"}}, true);
		db.insertRow("test", {{"id", "25"}}, {{"id", "15"}});
		db.insertRow("test", {{"id", "65"}, {"name", "john"}, {"id", "15"}}, {{"name", "VALUES(name)"}});
		db.insertRow("test", {{"id", "35"}});
		db.insertRow("test", {{"id", "75"}});


		db.insertRow("test", {{"id", "175"}});
		db.insertRow("test", {{"id", "185"}});
		db.insertRow("test", {{"id", "195"}});


        db.update("test", {{ "name", "ivan" }, { "description", "student" }, { "price", "20.222" }}, {{ "id", "= ?", "15" }});
		db.update("test", {{ "name", "kyle" }, { "description", "admin" }, { "price", "30.222" }}, {{ "id", "IN(6,7)"}});
		db.update("test", {{ "name", "serg" }, { "description", "CEO" }, { "price", "30.222" }}, {{ "id", "IN(?, ?, ?)", "65", "35", "75"}});

		db.update("test", {{ "id", "125" }}, {{ "id", "= ?", "15" }, {"OR"}, { "id", "= ?", "16"}});
		db.update("test", {{ "id", "125" }}, {{ "id", " > 156"}, {"AND"}, { "id", "= ?", "16"}});
		db.update("test", {{ "id", "325" }}, {{ "id", "IN(?, ?, ?)", "175", "185", "195"}, {"OR"}, {"name", " = ?", "ivan"}, {"OR"}, {"id", "= ?", "66"}, {"id", "= ?", "166"}, {"id", "= ?", "66"}, {"id", "= ?", "76"}});



		db.insertAll("test",
           {
            {{"id", "115"}, {"name", "jacob1"}},
            {{"id", "215"}, {"name", "jacob2"}},
            {{"id", "315"}, {"name", "jacob3"}},
            {{"id", "415"}, {"name", "jacob4"}},
            {{"id", "515"}, {"name", "jacob5"}},
            {{"id", "615"}, {"name", "jacob6"}},
            {{"id", "715"}, {"name", "jacob7"}}
           }
        );

		db.insertAll("test",
           {
            {{"id", "115"}, {"name", "jacob1"}},
            {{"id", "215"}, {"name", "jacob2"}},
            {{"id", "315"}, {"name", "jacob3"}},
            {{"id", "415"}, {"name", "jacob4"}},
            {{"id", "515"}, {"name", "jacob5"}},
            {{"id", "615"}, {"name", "jacob6"}},
            {{"id", "715"}, {"name", "jacob7"}}
           },
        {{"name", "VALUES(name)"}});

        db.insertAll("test",
           {
            {{"id", "115"}, {"name", "jacob1"}},
            {{"id", "215"}, {"name", "jacob2"}},
            {{"id", "315"}, {"name", "jacob3"}},
            {{"id", "415"}, {"name", "jacob4"}},
            {{"id", "515"}, {"name", "jacob5"}},
            {{"id", "615"}, {"name", "jacob6"}},
            {{"id", "715"}, {"name", "jacob7"}}
           },
        true);

		db.setDatabase("mysql");
		db_rows rows = db.fetchAll("SELECT `argument` FROM `general_log`");
		for (auto const & row : rows) {
			for (auto const & item : row) {
				cout << item.first << " = " << item.second << " ";
			}
			cout << std::endl;
		};

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
