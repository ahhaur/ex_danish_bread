import mysql.connector as dbconn

class DbConnector:

    def __init__(self, config=None):
        if config is None:
            print("No configs.")
            exit(1)
        self._cursor = None
        try:
            self._conn_db = dbconn.connect(user=config['user'], password=config['pass'], host=config['host'], use_pure=True, charset='utf8')

            _result = self.get_result(f"SHOW DATABASES like '{config['db']}'")
            if len(_result) == 0:
                self.create_table(config['db'])

        except Exception as ex:
            print("Error occurred while connecting Database. Error Message : {}".format(ex.args[1]))
            self._conn_db = False
        else:
            self._cursor = self._conn_db.cursor()
            self._cursor.execute(f"USE {config['db']};")
            
            # Enforce UTF-8 for the connection.
            self._cursor.execute('SET NAMES utf8mb4')
            self._cursor.execute("SET CHARACTER SET utf8mb4")
            self._cursor.execute("SET character_set_connection=utf8mb4")

    def check_connection(self):
        return False if self._conn_db is False else True

    def create_table(self, dbname):
        print(f"Create database: {dbname}")
        sql = """
        CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
        """.format(dbname)
        self._execute(sql)

        print(f"Create branch table.")
        sql = """
        CREATE TABLE IF NOT EXISTS {}.branch_table (
            branch_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
            name VARCHAR(100) NOT NULL ,
            sha VARCHAR(64) NOT NULL ,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
            PRIMARY KEY (branch_id)
            ) ENGINE = InnoDB;
        """.format(dbname)

        self._execute(sql)

        print(f"Create commit table.")
        sql = """
        CREATE TABLE IF NOT EXISTS {}.commit_table (
            commit_id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
            branch_id INT UNSIGNED NOT NULL , 
            commit_sha VARCHAR(64) NOT NULL , 
            author_id INT UNSIGNED NOT NULL , 
            author_login VARCHAR(100) NOT NULL , 
            author_name VARCHAR(100) NOT NULL , 
            committer_id INT UNSIGNED NOT NULL , 
            committer_login VARCHAR(100) NOT NULL , 
            committer_name VARCHAR(100) NOT NULL , 
            commit_date VARCHAR(20) NOT NULL , 
            commit_datetime DATETIME NOT NULL , 
            message VARCHAR(255) NOT NULL , 
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
            PRIMARY KEY (commit_id)
            ) ENGINE = InnoDB;
        """.format(dbname)

        self._execute(sql)

    def insert_batch(self, tbl_name, columns, data):
        col = ", ".join(columns)
        val = ", ".join(['%s']*len(columns))
        sql = rf"INSERT INTO {tbl_name} ({col}) VALUES ({val})"
        self._cursor.executemany(sql, data)
        self._conn_db.commit()
        print(self._cursor.rowcount, "was inserted.")

    def check_record(self, tbl_name, col, cond):
        sql = f"select {col} from {tbl_name} where {cond} limit 1"
        results = self.get_result(sql)
        return results[0][col] if len(results) == 1 else 0

    def insert(self, tbl_name, columns, data):
        col = ", ".join(columns)
        val = ", ".join(['%s']*len(columns))
        sql = rf"INSERT INTO {tbl_name} ({col}) VALUES ({val})"
        self._cursor.execute(sql, data)
        self._conn_db.commit()
        print("1 record inserted, ID:", self._cursor.lastrowid)
        return self._cursor.lastrowid

    def get_record(self, sql_query, has_header=True):
        if not self._execute(sql_query):
            return False
        # get column name
        columns = [column[0] for column in self._cursor.description]
        row = self._cursor.fetchone()
        if row is None:
            return None

        if has_header:
            retval = {}
            for i, val in enumerate(row):
                retval[columns[i]] = val
        else:
            retval = list(row)
        return retval

    def get_result(self, sql_query, has_header=True):
        if not self._execute(sql_query):
            return False
        # get column name
        columns = [column[0] for column in self._cursor.description]
        result = []
        for row in self._cursor.fetchall():
            if has_header:
                tmp = {}
                for i, val in enumerate(row):
                    tmp[columns[i]] = val
            else:
                tmp = list(row)
            result.append(tmp)
        return result

    def _execute(self, sql_query):
        if self.check_connection() is False:
            return False

        self._cursor = self._conn_db.cursor()
        try:
            self._cursor.execute(sql_query)
        except Exception as ex:
            print("Error occurred while running query. Error Message : {}".format(ex.args[1]))
            return False
        else:
            return True

    def __del__(self):
        print('Close DB connection...')
        self._conn_db.close()
