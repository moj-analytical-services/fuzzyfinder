import json
import sqlite3
from multiprocessing import Pool
from functools import partial
from collections import Counter

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
        self.db_filename = db_filename
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
                    CREATE TABLE {col}_token_counts
                    (token text, token_count int, token_proportion float)
                   """
            c.execute(sql)

            sql = f"""
            CREATE INDEX {col}_token_proportions_idx ON {col}_token_counts (token);
            """
            c.execute(sql)
        c.close()

    def set_example_record(self, record):
        self.example_record = record
        if self._table_df_is_empty:
            self.initialise_token_tables()

    @staticmethod
    def _record_dict_to_insert_data(record_dict: dict):

        record = Record(record_dict)
        uid = record.id
        jsond = json.dumps(record.record_dict)
        concat = record.tokenised_stringified_with_misspellings

        df_tuple = (uid, jsond, concat)



        columns = record.columns_except_unique_id
        tfd = record.tokenised_including_mispellings

        col_token_counts = {}
        for col in columns:
            tokens = tfd[col]
            col_counter = Counter()
            col_counter.update(tokens)
            col_token_counts[col] = col_counter

        return {'df_tuple': df_tuple, 'col_token_counts': col_token_counts}



    def write_list_dicts_parallel(self, list_dicts:list):


        if self.example_record is None:
            record = Record(list_dicts[0])
            self.set_example_record(record)

        p = Pool()
        fn = self._record_dict_to_insert_data
        results = p.map(fn, list_dicts, chunksize=1000)

        p.close()
        p.join()

        col_token_counts = {}
        columns = record.columns_except_unique_id
        for col in columns:
            col_token_counts[col] = Counter()

        c = self.conn.cursor()
        for r in results:
            insert_tuple = r['df_tuple']
            try:
                c.execute("INSERT INTO df VALUES (?, ?, ?)", insert_tuple)
            except sqlite3.IntegrityError:
                logger.debug(f"Record id {insert_tuple[0]} already exists in db, ignoring")
                return None

            counter_dict = r['col_token_counts']
            for col in columns:
                col_token_counts[col].update(counter_dict[col])

        # sql creates or updates key
        for col in columns:
            for token, value in col_token_counts[col].items():

                sql = f"""
                    INSERT OR IGNORE INTO {col}_token_counts VALUES (?, ?, ?)
                """
                c.execute(sql, (token, value, None))

                sql = f"""
                UPDATE {col}_token_counts SET token_count = token_count + {value}
                    WHERE token = '{token}';
                """
                c.execute(sql)

        c.close()


    def set_example_record_from_db(self):
        c = self.conn.cursor()

        c.execute("select original_record from df limit 1")
        record = c.fetchone()
        c.close()
        record = record["original_record"]
        record = json.loads(record)
        self.example_record = Record(record)

    def write_pandas_dataframe(self, pd_df, unique_id_col: str = "unique_id"):

        records_as_dict = pd_df.to_dict(orient="records")
        self.set_example_record(Record(records_as_dict[0]))

        self.write_list_dicts_parallel(records_as_dict)

        logger.debug(f"Records written: {self._records_written_counter }")

    def create_or_replace_token_stats_tables(self):
        rec = self.example_record
        columns = rec.columns_except_unique_id

        c = self.conn.cursor()
        for col in columns:
            c.execute(f"""DROP TABLE IF EXISTS {col}_token_proportions""")

            logger.debug(f"Creating table {col}_token_proportions")
            sql = f"""
            update  {col}_token_counts
            set token_proportion = (select token_count/count(*) from {col}_token_counts)
            """
            c.execute(sql)
            self.conn.commit()
            logger.debug(f"Created table {col}_token_proportions.  Indexing...")


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


# def chunk_list(lst, n):
#     """Yield successive n-sized chunks from lst."""
#     for i in range(0, len(lst), n):
#         yield lst[i:i + n]


