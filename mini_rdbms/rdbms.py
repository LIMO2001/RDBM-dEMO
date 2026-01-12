# rdbms.py
import re

class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or set()
        self.rows = []
        self.indexes = {}

        if primary_key:
            self.indexes[primary_key] = {}

        for key in self.unique_keys:
            self.indexes[key] = {}

    def insert(self, values):
        row = dict(zip(self.columns.keys(), values))

        # Enforce primary & unique keys
        for col, idx in self.indexes.items():
            val = row[col]
            if val in idx:
                raise Exception(f"Duplicate value for UNIQUE column '{col}'")

        row_id = len(self.rows)
        self.rows.append(row)

        for col, idx in self.indexes.items():
            idx[row[col]] = row_id

    def select(self, where=None):
        if not where:
            return self.rows

        col, val = where
        if col in self.indexes:
            idx = self.indexes[col]
            if val in idx:
                return [self.rows[idx[val]]]
            return []

        return [r for r in self.rows if r[col] == val]

    def update(self, where, updates):
        rows = self.select(where)
        for row in rows:
            for k, v in updates.items():
                row[k] = v

    def delete(self, where):
        col, val = where
        self.rows = [r for r in self.rows if r[col] != val]


class Database:
    def __init__(self):
        self.tables = {}

    def execute(self, sql):
        sql = sql.strip().rstrip(";")

        if sql.upper().startswith("CREATE TABLE"):
            return self._create(sql)
        if sql.upper().startswith("INSERT"):
            return self._insert(sql)
        if sql.upper().startswith("SELECT"):
            return self._select(sql)
        if sql.upper().startswith("UPDATE"):
            return self._update(sql)
        if sql.upper().startswith("DELETE"):
            return self._delete(sql)

        raise Exception("Unsupported SQL")

    def _create(self, sql):
        name = re.findall(r"CREATE TABLE (\w+)", sql, re.I)[0]
        cols = re.findall(r"\((.*)\)", sql)[0].split(",")

        columns = {}
        pk = None
        uniques = set()

        for c in cols:
            parts = c.strip().split()
            col = parts[0]
            columns[col] = parts[1]
            if "PRIMARY" in c.upper():
                pk = col
            if "UNIQUE" in c.upper():
                uniques.add(col)

        self.tables[name] = Table(name, columns, pk, uniques)
        return "OK"

    def _insert(self, sql):
        name = re.findall(r"INSERT INTO (\w+)", sql, re.I)[0]
        values = re.findall(r"\((.*)\)", sql)[0]
        values = [v.strip().strip("'") for v in values.split(",")]
        self.tables[name].insert(values)
        return "OK"

    def _select(self, sql):
        # JOIN query
        if "JOIN" in sql.upper():
            return self._select_join(sql)

        # Normal SELECT
        name = re.findall(r"FROM (\w+)", sql, re.I)[0]
        where = None

        if "WHERE" in sql.upper():
            col, val = re.findall(r"WHERE (\w+) = (.+)", sql, re.I)[0]
            where = (col, val.strip("'"))

        return self.tables[name].select(where)

    def _select_join(self, sql):
        # Parse selected columns
        select_part = re.findall(r"SELECT (.+?) FROM", sql, re.I)[0]
        columns = [c.strip() for c in select_part.split(",")]

        # Parse table names
        left_table = re.findall(r"FROM (\w+)", sql, re.I)[0]
        right_table = re.findall(r"JOIN (\w+)", sql, re.I)[0]

        # Parse join condition
        left_col, right_col = re.findall(
            r"ON (\w+\.\w+) = (\w+\.\w+)", sql, re.I
        )[0]

        lt, lc = left_col.split(".")
        rt, rc = right_col.split(".")

        results = []

        for lrow in self.tables[left_table].rows:
            for rrow in self.tables[right_table].rows:
                if lrow[lc] == rrow[rc]:
                    joined = {}
                    for col in columns:
                        t, c = col.split(".")
                        joined[col] = lrow[c] if t == left_table else rrow[c]
                    results.append(joined)

        return results

    def _update(self, sql):
        name = re.findall(r"UPDATE (\w+)", sql, re.I)[0]
        set_part = re.findall(r"SET (.+) WHERE", sql, re.I)[0]
        col, val = set_part.split("=")
        col, val = col.strip(), val.strip().strip("'")

        wcol, wval = re.findall(r"WHERE (\w+) = (.+)", sql, re.I)[0]
        self.tables[name].update((wcol, wval.strip("'")), {col: val})
        return "OK"

    def _delete(self, sql):
        name = re.findall(r"DELETE FROM (\w+)", sql, re.I)[0]
        col, val = re.findall(r"WHERE (\w+) = (.+)", sql, re.I)[0]
        self.tables[name].delete((col, val.strip("'")))
        return "OK"
