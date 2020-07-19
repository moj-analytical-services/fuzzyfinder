from collections import Counter
from multiprocessing import Pool
import json

import sqlite3

from .record import Record
from .utils import dict_factory

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

        # The connection will render query results as list of dicts
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
                    (token text primary key, token_count int, token_proportion float)
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
        """Process a single record dict into data reaady to be entered into the database

        Args:
            record_dict (dict): A dictionary representing a record

        Returns:
            dict: A dictionary containing the tuple needed for an INSERT statmenent, and
                  'col_token_counts' a dictionary of Counters(), one for each column, containing token counts
        """

        record = Record(record_dict)
        uid = record.id
        jsond = json.dumps(record.record_dict)
        concat = record.tokenised_stringified_with_misspellings

        df_tuple = (uid, jsond, concat)

        columns = record.columns_except_unique_id
        tfd = record.tokenised_including_mispellings

        column_counters = ColumnCounters(record)
        for col in columns:
            tokens = tfd[col]
            column_counters.update_single_column(col, tokens)

        return {"df_tuple": df_tuple, "column_counters": column_counters}

    @staticmethod
    def _record_batch_to_insert_data(record_dicts: list):
        """Process a list of record dictionaries into data ready to be entered into the database.

        Args:
            record_dicts (list): A list of dictionaries representing records

        Returns:
            dict: A dict containing 'result_tuples', a list of tuples ready to insert into the table df and
                  'column_counters' a dict of Counters(), one for each column, containing aggregated token counts
                  'record_dicts' the original record dictionaries.  Needed in case the batch fails database integrity
                  constraints and records need to be added one by one.
        """
        results_batch = {
            "result_tuples": [],
            "column_counters": ColumnCounters(Record(record_dicts[0])),
            "original_dicts": record_dicts,
        }

        for rd in record_dicts:
            single_record_results = SearchDatabaseBuilder._record_dict_to_insert_data(
                rd
            )
            rt = single_record_results["df_tuple"]
            results_batch["result_tuples"].append(rt)
            new_column_counter = single_record_results["column_counters"]
            results_batch["column_counters"].update(new_column_counter)

        return results_batch

    def bulk_insert_batch(self, results_batch, column_counters):

        # See here: https://stackoverflow.com/questions/52912010
        result_tuples = results_batch["result_tuples"]
        c = self.conn.cursor()

        try:
            c.executemany("INSERT INTO df VALUES (?, ?, ?)", result_tuples)
        except sqlite3.IntegrityError:
            c.execute("rollback")
            c.close()
            raise sqlite3.IntegrityError()
        self.conn.commit()

        # Passed by reference so can mutate object
        # Note counters only updated if whole transaction completes successfully
        new_column_counters = results_batch["column_counters"]

        column_counters.update(new_column_counters)

    def insert_batch_one_by_one(self, batch, column_counters):
        # If bulk insert failed, we want to insert records one by one
        # logging integrity errors
        # Where integrity checks fail, we do not want to increment counters
        c = self.conn.cursor()
        for record_dict in batch:
            insert_data = self._record_dict_to_insert_data(record_dict)
            insert_tuple = insert_data["df_tuple"]
            try:
                c.execute("INSERT INTO df VALUES (?, ?, ?)", insert_tuple)
            except sqlite3.IntegrityError:
                logger.debug(
                    f"Record id {insert_tuple[0]} already exists in db, ignoring"
                )
                continue

            new_column_counters = insert_data["column_counters"]
            column_counters.update(new_column_counters)
        c.close()
        self.conn.commit()

    def write_list_dicts_parallel(self, list_dicts: list, batch_size=10_000):
        """Process a list of dicts containing records in parallel, turning them into data ready to be inserted
        into the databse, then insert

        Args:
            list_dicts (list): A list of dictionaries, each one representing a record
            batch_size (int, optional): How many records to send to each parallel worker. Defaults to 10000.
        """

        record = Record(list_dicts[0])
        if self.example_record is None:
            self.set_example_record(record)

        batches_of_records = chunk_list(list_dicts, batch_size)

        ##########################
        # Start of parallelisation
        ##########################
        p = Pool()
        fn = self._record_batch_to_insert_data
        results_batches = p.imap(fn, batches_of_records)
        c = self.conn.cursor()

        column_counters = ColumnCounters(self.example_record)

        # Since it's an imap, which results in a generator
        # this might start executing before the final batch completes?
        for results_batch in results_batches:
            # If an insert fails it's because one of the unique_ids already exists
            # If so, insert the records one by one, logging integrity errors
            try:
                self.bulk_insert_batch(results_batch, column_counters)
            except sqlite3.IntegrityError:
                self.insert_batch_one_by_one(
                    results_batch["original_dicts"], column_counters
                )

        p.close()
        p.join()

        ##########################
        # End of parallelisation
        ##########################

        # sql creates or updates key

        for col in column_counters.columns:

            for token, value in column_counters.col_items(col):
                sql = f"""
                    INSERT OR IGNORE INTO {col}_token_counts VALUES (?, ?, ?)
                """
                c.execute(sql, (token, 0, None))

                sql = f"""
                UPDATE {col}_token_counts SET token_count = token_count + {value}
                    WHERE token = '{token}';
                """
                c.execute(sql)

        c.close()
        self.conn.commit()

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

        self.write_list_dicts_parallel(records_as_dict)

        logger.debug(f"Records written: {self._records_written_counter }")

    def update_token_stats_tables(self):
        rec = self.example_record
        columns = rec.columns_except_unique_id

        c = self.conn.cursor()
        for col in columns:
            sql = f"""
            update  {col}_token_counts
            set token_proportion = (select token_count/count(*) from {col}_token_counts)
            """
            c.execute(sql)
            self.conn.commit()
            logger.debug(f"Updated table {col}_token_counts.")

        c.close()

    def create_or_replace_fts_table(self):
        c = self.conn.cursor()
        logger.debug("Starting to create FTS table")
        # Create FTS
        c.execute("DROP TABLE IF EXISTS fts_target")
        sql = """
        CREATE VIRTUAL TABLE fts_target
        USING fts5(unique_id, concat_all);
        """
        c.execute(sql)

        sql = """
        INSERT INTO fts_target(unique_id, concat_all)
        SELECT unique_id, concat_all
        FROM df
        """
        c.execute(sql)

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
        self.update_token_stats_tables()
        self.create_or_replace_fts_table()
        self.index_unique_id()


def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class ColumnCounters:
    """
    Stores token counts for each column in a table
    """

    def __init__(self, record: Record):
        # Initialise one counter per column to store token counts
        col_token_counts = {}
        columns = record.columns_except_unique_id
        for col in columns:
            col_token_counts[col] = Counter()

        self.col_token_counts = col_token_counts

    def __getitem__(self, item):
        return self.col_token_counts[item]

    @property
    def columns(self):
        return self.col_token_counts.keys()

    @property
    def column_counters(self):
        return self.col_token_counts

    def col_items(self, col):
        return self.col_token_counts[col].items()

    def update_single_column(self, col, new_counter):
        self.col_token_counts[col].update(new_counter)

    def update(self, new_column_counters):
        for col in self.columns:
            new_counter = new_column_counters[col]
            self.update_single_column(col, new_counter)

    def __repr__(self):
        return self.col_token_counts.__repr__()
