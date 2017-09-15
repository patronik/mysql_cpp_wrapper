#include "main.h"
#include "db.h"

// Global state
boost::property_tree::ptree config;

int test()
{
    try {
        string host = config.get<string>("mysql.host");
        string user = config.get<string>("mysql.username");
        string pass = config.get<string>("mysql.password");
        string port = config.get<string>("mysql.port");

        if (host.empty() || user.empty() || pass.empty()) {
            cout << "please provide db access details" << endl;
            return -1;
        }

        if (!port.empty()) {
            host += ":" + port;
        }

		DB db(host, user, pass);

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

        db.setDatabase("sandbox");
		db.remove("test", {{"name", " = ?", "jacob1"}, {"OR"}, {"name", "= ?", "jacob5"}});
		db.remove("test", {{"name", " LIKE ?", "%jac%"}, {"id", "= ?", "115"}});

        db.setDatabase("mysql");
		rows = db.fetchAll("SELECT `argument` FROM `general_log`");
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
};

void setParam(string & sql, char p);
void setParam(string & sql, int p);
void setParam(string & sql, const char * p);
void setParam(string & sql, double p);
void setParam(string & sql, float p);
void setParam(string & sql, string p);

template<typename T>
bool query(string sql, T last) {
    setParam(sql, last);
    cout << sql << endl;
    return true;
}

void setParam(string & sql, char p) {
    cout << p << endl;
}

void setParam(string & sql, int p) {
    cout << p << endl;
}

void setParam(string & sql, const char * p) {
    cout << p << endl;
}

void setParam(string & sql, double p) {
    cout << p << endl;
}

void setParam(string & sql, float p) {
    cout << p << endl;
}

void setParam(string & sql, string p) {
    cout << p << endl;
}

template<typename SQL, typename T, typename... Args>
bool query(SQL sql, T p, Args... args) {
    setParam(sql, p);
    query(sql, args...);
    return true;
}

int main(int argc, char * argv[]) {

	ifstream file("config.ini");
    if (!file) {
        cout << "config.ini does not exists" << endl;
        return -1;
    }

    boost::property_tree::ini_parser::read_ini("config.ini", config);

    query(string("SELECT ? FROM ?"), 2, 2.2, -5, 'c' , "abcd", string("some text..."));

	return 0;
}
