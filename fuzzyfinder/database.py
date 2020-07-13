import json
import re
import sqlite3

from .utils import dict_factory
from .record import Record


class SearchDatabaseBuilder:
    """Create and populate a SQLite database
    that contains the records we want to search within
    """

    def __init__(self, db_filename: str = None):
        """
        Args:
            filename (str, optional):  The filename for the database.  If none, the database will be an in-memory
            sqlite database
        """

        if not db_filename:
            db_filename = ":memory:"

        self.conn = sqlite3.connect(db_filename)

        # The connection will 'render' query results as list of dicts
        self.conn.row_factory = dict_factory

        if self.db_is_empty:
            self.initialise_db()

        self.example_record = None

    @property
    def db_is_empty(self):
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master")
        results = c.fetchall()
        c.close()
        return len(results) == 0

    @property
    def db_has_column_raw_token_tables(self):
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master")
        results = c.fetchall()
        c.close()

        tokens_tables = [
            d["name"] for d in results if re.search("raw_tokens$", d["name"])
        ]
        expected_tokens_tables = [
            f"{c}_raw_tokens" for c in self.example_record.columns_except_unique_id
        ]

        return set(tokens_tables) == set(expected_tokens_tables)

    def initialise_db(self):
        c = self.conn.cursor()

        c.execute(
            """CREATE TABLE df
                    (unique_id TEXT NOT NULL PRIMARY KEY,
                     original_record JSON,
                     concat_all TEXT)
                  """
        )
        # https://stackoverflow.com/questions/1711631/improve-insert-per-second-performance-of-sqlite
        c.execute("PRAGMA synchronous = EXTRA")
        c.execute("PRAGMA journal_mode = WAL")

        c.close()

    def initialise_token_tables(self):
        rec = self.example_record
        columns = rec.columns_except_unique_id
        c = self.conn.cursor()
        for col in columns:
            sql = f"""
                    CREATE TABLE {col}_raw_tokens
                    (token text)
                   """
            c.execute(sql)
        c.close()

    def set_example_record(self, record):
        self.example_record = record
        if not self.db_has_column_raw_token_tables:
            self.initialise_token_tables()

    def write_record(self, record: Record):

        # Want to avoid user explicity having to pass an example record in on __init__
        # Instead, we set up the databse when we first see a record
        if self.example_record is None:
            self.set_example_record(record)

        uid = record.id
        jsond = json.dumps(record.record_dict)
        concat = record.tokenised_stringified_with_misspellings

        c = self.conn.cursor()

        c.execute("INSERT INTO df VALUES (?, ?, ?)", (uid, jsond, concat))

        tfd = record.tokenised_including_mispellings

        columns = record.columns_except_unique_id
        for col in columns:
            tokens = tfd[col]
            for t in tokens:
                c.execute(f"INSERT INTO {col}_raw_tokens VALUES (?)", (t,))
        c.close()
