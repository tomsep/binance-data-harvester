import logging
log = logging.getLogger(__name__)
import sqlite3
import mysql.connector
import time

class DataBase:

    def __init__(self, db_path, field_naming_rules, commit_freq=10):

        self._field_naming_rules = field_naming_rules
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        self._conn.execute('PRAGMA foreign_keys = 1')

        self._existing_tables = None
        self.update_table_names()
        self._commit_freq = commit_freq
        self._action_counter = self._commit_freq

    def update_table_names(self):
        with self._conn as conn:
            res = conn.execute('SELECT name FROM sqlite_master WHERE type = "table"')
        self._existing_tables = [x[0] for x in res.fetchall()]

    def insert(self, table, data):

        if table not in self._existing_tables:
            c = DataBase._schema_template_from_rules(table, self._field_naming_rules)
            with self._conn as conn:
                conn.execute(c)
            self.update_table_names()

        c_t, values = DataBase._insert_template(table, data, self._field_naming_rules)

        self._conn.execute(c_t, values)

        self._commit()

    @staticmethod
    def _insert_template(table_name, data, rules):
        ruleset_name = table_name.split('@')[-1]
        rules = rules[ruleset_name]

        field_names = []
        field_values = []

        for key, item in rules.items():
            if key != 'primary key':
                field_names.append(item['alias'])
                if item['type'] == 'TEXT':
                    field_values.append(str(data[key]))
                elif item['type'] == 'INT':
                    field_values.append(int(data[key]))
                else:
                    raise ValueError('Field type "{}" not supported.'.format(item['type']))

        qmarks = '(' + ('?,' * len(field_names))[:-1] + ')'  # (?, ?, ?, ...)
        field_names = str(tuple(field_names)).replace('\'', '"')

        c = f'INSERT INTO "{table_name}" {field_names} VALUES {qmarks}'
        return c, field_values

    @staticmethod
    def _schema_template_from_rules(table_name, rules):

        ruleset_name = table_name.split('@')[-1]
        rules = rules[ruleset_name]

        c = ['CREATE TABLE IF NOT EXISTS "{}" ('.format(table_name)]
        for key in rules.keys():
            if key != 'primary key':
                alias, field_type = rules[key]['alias'], rules[key]['type']
                c.append(f'"{alias}" {field_type},')

        c.append('PRIMARY KEY {})'.format(rules['primary key']))
        return ' '.join(c)

    def _commit(self, immediately=False):

        if self._action_counter <= 0 or immediately:
            self._conn.commit()
            self._action_counter = self._commit_freq
            log.debug('Committed DB transactions.')
        else:
            self._action_counter -= 1


class MySQLDB:

    def __init__(self, host, port, user, passwd, db_name, commit_freq=0):

        self._commit_freq = commit_freq
        self._action_counter = self._commit_freq

        self._db = MySQLDB._connect(host, port, user, passwd)
        self._cursor = self._db.cursor()
        if db_name not in self.get_databases():
            MySQLDB._create_database(self._cursor, db_name)
        self._db.database = db_name

        self._tables = self.tables()

    def get_databases(self):
        self._cursor.execute('SHOW DATABASES')
        return [x[0] for x in self._cursor]

    def tables(self):
        self._cursor.execute('SELECT table_name FROM information_schema.tables;')
        return [x for x in self._cursor]

    def insert_ticker(self, ticker):

        symbol = ticker['s'].lower()
        table = f'{symbol}_ticker'
        event_time = ticker['E']
        last_price = ticker['c']
        close_time = ticker['C']

        sql = f'INSERT INTO {table} (close_time, last_price, event_time) VALUES (%s, %s, %s)'
        val = (close_time, last_price, event_time)

        try:
            self._cursor.execute(sql, val)
        except mysql.connector.errors.ProgrammingError as err:
            log.error(f'Ticker insert failed with error: "{str(err)}".')
            self._create_ticker_table(symbol)
            self._cursor.execute(sql, val)

        self._commit()

    def insert_depth(self, msg: dict):

        symbol = msg['stream'].split('@')[0]
        table_book, table_asks, table_bids = MySQLDB.depth_table_names(symbol)
        data = msg['data']

        last_update_id = data['lastUpdateId']
        levels = len(data['asks'])

        index_prices, index_quantity = MySQLDB.depth_column_names(levels)

        value_placeholders = MySQLDB._value_placeholders(levels * 2 + 1)

        sql = f"""
        INSERT INTO {table_asks} (last_update_id, {index_prices}, {index_quantity})
        VALUES {value_placeholders};
        """

        vals = [last_update_id]
        vals += [x[0] for x in data['asks']]
        vals += [x[1] for x in data['asks']]

        try:
            self._cursor.execute(sql, vals)
        except mysql.connector.errors.ProgrammingError as err:
            log.error(f'Depth insert failed with error: "{str(err)}".')
            self._create_depth_table(symbol, levels)
            self._cursor.execute(sql, vals)

        sql = f"""
        INSERT INTO {table_bids} (last_update_id, {index_prices}, {index_quantity})
        VALUES {value_placeholders};
        """

        vals = [last_update_id]
        vals += [x[0] for x in data['bids']]
        vals += [x[1] for x in data['bids']]

        self._cursor.execute(sql, vals)

        sql = f"""
        INSERT INTO {table_book} (last_update_id)
        VALUES (%s);
        """
        self._cursor.execute(sql, (last_update_id,))

        self._commit()

    def close(self):
        self._commit(immediately=True)
        self._db.close()
        log.info(f'Closing DB connection to "{self._db.database}".')
        print('closed')

    @staticmethod
    def depth_column_names(levels: int, datatype=''):

        def _create_index(prefix_):
            index_ = [f'{prefix_}_{i + 1} {datatype}' for i in range(levels)]
            index_ = str(index_)[2:-2]  # "prefix_1 datatype, prefix__2 da...
            index_ = index_.replace('\'', '')
            return index_

        index_prices = _create_index('price')
        index_qty = _create_index('quantity')
        return index_prices, index_qty

    @staticmethod
    def depth_table_names(symbol):
        symbol = symbol.lower()
        table_book = f'{symbol}_order_book'
        table_asks = f'{symbol}_asks'
        table_bids = f'{symbol}_bids'
        return table_book, table_asks, table_bids

    def _create_ticker_table(self, symbol):
        table = f'{symbol.lower()}_ticker'
        sql = f"""
        CREATE TABLE {table}
        (close_time BIGINT PRIMARY KEY, last_price DEC(16,8), event_time BIGINT);
        """
        self._cursor.execute(sql)
        log.info(f'Created table {table}.')

    def _create_depth_table(self, symbol, levels: int):

        table_book, table_asks, table_bids = MySQLDB.depth_table_names(symbol)

        index_prices, index_qty = MySQLDB.depth_column_names(levels, datatype='DEC(16,8)')

        sql = f"""
        CREATE TABLE {table_asks}
        (last_update_id BIGINT PRIMARY KEY, {index_prices}, {index_qty});
        """
        self._cursor.execute(sql)

        sql = f"""
        CREATE TABLE {table_bids}
        (last_update_id BIGINT PRIMARY KEY, {index_prices}, {index_qty});
        """
        self._cursor.execute(sql)

        sql = f"""
        CREATE TABLE {table_book}
        (last_update_id BIGINT PRIMARY KEY,
        FOREIGN KEY (last_update_id) REFERENCES {table_asks} (last_update_id) ON DELETE CASCADE,
        FOREIGN KEY (last_update_id) REFERENCES {table_bids} (last_update_id) ON DELETE CASCADE);
        """
        self._cursor.execute(sql)
        log.info(f'Created tables {table_book}, {table_asks}, {table_bids}.')

    def _commit(self, immediately=False):
        if self._action_counter <= 0 or immediately:
            self._db.commit()
            self._action_counter = self._commit_freq
            log.debug('Committed DB transactions.')
        else:
            self._action_counter -= 1

    @staticmethod
    def _value_placeholders(n: int):
        return '(' + ('%s, ' * n)[:-2] + ')'  # (%s, %s, ... %s)

    @staticmethod
    def _connect(host, port, user, passwd, patience=15):

        for i in range(patience):
            try:
                db = mysql.connector.connect(
                    host=host,
                    port=port,
                    user=user,
                    passwd=passwd
                )
                log.info(f'Connected to MySQL, host: {host}, port: {port}, user: {user}.')
                return db
            except mysql.connector.errors.DatabaseError as err:
                if i+1 == patience:
                    raise err
                else:
                    time.sleep(1)

    @staticmethod
    def _create_database(cursor, db_name):
        cursor.execute(f'CREATE DATABASE {db_name}')
        log.info(f'Database "{db_name}" was created.')
