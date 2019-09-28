import logging
log = logging.getLogger(__name__)
import sqlite3


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

