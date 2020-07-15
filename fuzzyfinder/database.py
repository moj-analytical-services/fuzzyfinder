import json
import sqlite3

from .utils import dict_factory
from .record import Record

import logging

logger = logging.getLogger(__name__)


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

        if not self._table_df_exists:
            self.initialise_db()

        # If connected to a database that already has records
        self.example_record = None
        if not self._table_df_is_empty:
            self.set_example_record_from_db()

        self._records_written_counter = 0

    @property
    def _table_df_exists(self):
        c = self.conn.cursor()
        try:
            c.execute("SELECT * FROM df limit 1")
        except sqlite3.OperationalError:
            # df does not exist
            c.close()
            return False
        c.close()
        return True

    @property
    def _table_df_is_empty(self):
        c = self.conn.cursor()
        try:
            c.execute("SELECT count(*) as rec_count FROM df")
        except sqlite3.OperationalError:
            # df does not exist
            c.close()
            return True

        result = c.fetchone()
        c.close()
        if result["rec_count"] == 0:
            return True
        else:
            return False

    @property
    def db_status(self):
        if not self._table_df_exists:
            return "blank"

        if not self._table_df_is_empty:
            return "build_in_progress"

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
        if self._table_df_is_empty:
            self.initialise_token_tables()

    def _write_record_no_commit(self, record: Record):
        # Do not want to commit after every write because it slows things down too much

        # Want to avoid user explicity having to pass an example record in on __init__
        # Instead, we set up the databse when we first see a record
        if self.example_record is None:
            self.set_example_record(record)

        uid = record.id
        jsond = json.dumps(record.record_dict)
        concat = record.tokenised_stringified_with_misspellings

        c = self.conn.cursor()

        try:
            c.execute("INSERT INTO df VALUES (?, ?, ?)", (uid, jsond, concat))
        except sqlite3.IntegrityError:
            logger.debug(f"Record id {uid} already exists in db, ignoring")
            c.close()
            return None

        self._records_written_counter += 1

        tfd = record.tokenised_including_mispellings

        columns = record.columns_except_unique_id
        for col in columns:
            tokens = tfd[col]
            for t in tokens:
                c.execute(f"INSERT INTO {col}_raw_tokens VALUES (?)", (t,))
        c.close()

    def write_record(self, record: Record):

        self._write_record_no_commit(record)

        if self._records_written_counter % 10000 == 0:
            logger.info(f"Records written: {self._records_written_counter }")
            self.conn.commit()

    def write_records(self, records: list):

        for r in records:
            self.write_record(r)
        self.conn.commit()
        logger.info(f"Records written: {self._records_written_counter }")

    def set_example_record_from_db(self):
        c = self.conn.cursor()

        c.execute("select original_record from df limit 1")
        record = c.fetchone()
        c.close()
        record = record["original_record"]
        record = json.loads(record)
        self.example_record = Record(record)

    def write_pandas_dataframe(self, pd_df, unique_id_col: str = "unique_id"):

        for d in pd_df.to_dict(orient="records"):
            rec = Record(d)
            self.write_record(rec)

        self.conn.commit()
        logger.debug(f"Records written: {self._records_written_counter }")

    def create_or_replace_token_stats_tables(self):
        rec = self.example_record
        columns = rec.columns_except_unique_id

        c = self.conn.cursor()
        for col in columns:
            c.execute(f"""DROP TABLE IF EXISTS {col}_token_proportions""")

            logger.debug(f"Creating table {col}_token_proportions")
            sql = f"""
            create table {col}_token_proportions as
            select token, cast(count(*) as float)/(select count(*) from {col}_raw_tokens) as token_proportion
            from {col}_raw_tokens
            group by token
            """
            c.execute(sql)
            self.conn.commit()
            logger.debug(f"Created table {col}_token_proportions.  Indexing...")

            sql = f"""
            CREATE INDEX {col}_token_proportions_idx ON {col}_token_proportions (token);
            """
            c.execute(sql)
            self.conn.commit()
            logger.debug(f"Indexed table {col}_token_proportions")
        c.close()

    def create_or_replace_fts_table(self):
        c = self.conn.cursor()
        logger.debug("Starting to create FTS table")
        # Create FTS
        c.execute("""DROP TABLE IF EXISTS fts_target""")
        sql = """
        CREATE VIRTUAL TABLE fts_target
        USING fts5(unique_id, concat_all);
        """
        c.execute(sql)
        c.execute(
            "INSERT INTO fts_target(unique_id, concat_all) SELECT unique_id, concat_all FROM df"
        )
        self.conn.commit()
        c.close()
        logger.debug("Created FTS table")

    def index_unique_id(self):
        c = self.conn.cursor()
        logger.debug("Starting to index unique_id field")
        sql = """
            CREATE INDEX df_unique_id_idx ON df (unique_id);
            """
        c.execute(sql)
        self.conn.commit()
        c.close()
        logger.debug("Unique_id field indexing completed")

    def build_or_replace_stats_tables(self):
        self.create_or_replace_token_stats_tables()
        self.create_or_replace_fts_table()
        self.index_unique_id()

    def clean_and_optimise_database(self):

        rec = self.example_record
        columns = rec.columns_except_unique_id

        c = self.conn.cursor()

        logger.debug("Dropping raw tokens tables")

        for col in columns:
            logger.debug(f"Starting dropping table {col}")
            c.execute(f"""DROP TABLE IF EXISTS {col}_raw_tokens""")
            self.conn.commit()

        logger.debug("Starting database vacuum")
        c.execute("""vacuum""")
        self.conn.commit()
        c.close()
        logger.debug("Completed database vacuum")
