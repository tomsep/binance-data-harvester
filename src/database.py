import logging
log = logging.getLogger(__name__)
import sqlite3
import time
import os


class Database:

    def __init__(self, db_path):

        self._db_path = db_path
        self._conn = sqlite3.connect(self._db_path, timeout=30, check_same_thread=False)
        log.info('Connected to database.')

        self._conn.execute('PRAGMA foreign_keys = 1')
        log.info('Database foreign keys enabled.')

        # WAL reduces disk writes, https://www.sqlite.org/wal.html
        self._conn.execute('PRAGMA journal_mode=WAL;')
        log.info('Database WAL mode enabled.')

        self._tables = self.tables()

    def tables(self):
        with self._conn as conn:
            res = conn.execute('SELECT name FROM sqlite_master WHERE type = "table"')
        return [x[0] for x in res.fetchall()]

    def insert_ticker(self, ticker, timestamp, ignore_if_exists=False):

        if ignore_if_exists:
            ignore = 'OR IGNORE'
        else:
            ignore = ''

        symbol = ticker['s'].lower()
        table = f'{symbol}_ticker'
        last_price = ticker['c']
        close_time = ticker['C']

        sql = f'INSERT {ignore} INTO {table} (close_time, last_price, timestamp) VALUES (?, ?, ?)'
        val = (close_time, last_price, timestamp)

        try:
            self._conn.execute(sql, val)
        except sqlite3.OperationalError as err:
            log.error(f'Ticker insert failed with error: "{str(err)}".')
            self._create_ticker_table(symbol)
            self._conn.execute(sql, val)

        try:
            self._conn.commit()
            return True
        except sqlite3.OperationalError as err:
            log.error(err)
            return False

    def insert_depth(self, msg: dict, timestamp, ignore_if_exists=False):

        if ignore_if_exists:
            ignore = 'OR IGNORE'
        else:
            ignore = ''

        symbol = msg['stream'].split('@')[0]
        table_book, table_asks, table_bids = Database.depth_table_names(symbol)
        data = msg['data']

        last_update_id = data['lastUpdateId']
        levels = len(data['asks'])

        index_prices, index_quantity = Database.depth_column_names(levels)

        value_placeholders = Database._value_placeholders(levels * 2 + 1)

        sql = f"""
        INSERT {ignore} INTO {table_asks} (last_update_id, {index_prices}, {index_quantity})
        VALUES {value_placeholders};
        """

        vals = [last_update_id]
        vals += [x[0] for x in data['asks']]
        vals += [x[1] for x in data['asks']]

        try:
            self._conn.execute(sql, vals)
        except sqlite3.OperationalError as err:
            log.error(f'Depth insert failed with error: "{str(err)}".')
            self._create_depth_table(symbol, levels)
            self._conn.execute(sql, vals)

        sql = f"""
        INSERT {ignore} INTO {table_bids} (last_update_id, {index_prices}, {index_quantity})
        VALUES {value_placeholders};
        """

        vals = [last_update_id]
        vals += [x[0] for x in data['bids']]
        vals += [x[1] for x in data['bids']]

        self._conn.execute(sql, vals)

        sql = f"""
        INSERT {ignore} INTO {table_book} (last_update_id, timestamp)
        VALUES (?, ?);
        """
        self._conn.execute(sql, (last_update_id, timestamp))

        try:
            self._conn.commit()
            return True
        except sqlite3.OperationalError as err:
            log.error(err)
            return False

    def close(self):
        self._conn.commit()
        self._conn.close()
        log.info(f'Closing DB connection to "{self._db_path}".')

    def reconnect(self):
        while not os.path.isfile(self._db_path):
            log.error('Waiting for database to be back up.')
            time.sleep(5)

        self._conn = sqlite3.connect(self._db_path, timeout=5, check_same_thread=False)
        log.info('Reconnected to database.')

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
        (close_time BIGINT PRIMARY KEY, last_price DEC(16,8), timestamp BIGINT);
        """
        self._conn.execute(sql)
        log.info(f'Created table {table}.')

    def _create_depth_table(self, symbol, levels: int):

        table_book, table_asks, table_bids = Database.depth_table_names(symbol)

        index_prices, index_qty = Database.depth_column_names(levels, datatype='DEC(16,8)')

        sql = f"""
        CREATE TABLE {table_asks}
        (last_update_id BIGINT PRIMARY KEY, {index_prices}, {index_qty});
        """
        self._conn.execute(sql)

        sql = f"""
        CREATE TABLE {table_bids}
        (last_update_id BIGINT PRIMARY KEY, {index_prices}, {index_qty});
        """
        self._conn.execute(sql)

        sql = f"""
        CREATE TABLE {table_book}
        (last_update_id BIGINT PRIMARY KEY, timestamp BIGINT,
        FOREIGN KEY (last_update_id) REFERENCES {table_asks} (last_update_id) ON DELETE CASCADE,
        FOREIGN KEY (last_update_id) REFERENCES {table_bids} (last_update_id) ON DELETE CASCADE);
        """
        self._conn.execute(sql)
        log.info(f'Created tables {table_book}, {table_asks}, {table_bids}.')

    @staticmethod
    def _value_placeholders(n: int):
        return '(' + ('?, ' * n)[:-2] + ')'  # (?, ?, ... ?)
