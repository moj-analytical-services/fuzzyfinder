from collections import Counter
from multiprocessing import Pool
import json
from functools import partial
import uuid
import warnings
import copy

from datetime import datetime

import sqlite3

from .record import Record
from .finder import MatchFinder
from .utils import dict_factory

import logging

logger = logging.getLogger(__name__)


class SearchDatabase:
    """Create and populate a SQLite database
    that contains the records we want to search within
    """

    def __init__(
        self,
        db_filename: str = None,
        cols_to_ignore: list = [],
        dmeta_cols: list = None,
    ):
        """
        Args:
            filename (str, optional):  The filename for the database.  If none, the database will be an in-memory
                sqlite database
            cols_to_ignore (list, optional): Ignore these columns when conducting searches.  Must be set
                before adding rows
            dmeta_cols (list, optional): If provided, only these named columns will be used to generate
                dmetaphone token variants.  If None, all columns will be used.  If empty list, no columns
                will be used

        """

        if not db_filename:
            db_filename = ":memory:"
        self.db_filename = db_filename

        self.conn = sqlite3.connect(db_filename)

        # The connection will render query results as list of dicts
        self.conn.row_factory = dict_factory

        self.cols_to_ignore = cols_to_ignore
        self.dmeta_cols = dmeta_cols
        # Check whether user has opened a previously-created database or this is a new database

        if self._table_df_exists:
            if cols_to_ignore:
                raise ValueError(
                    "You cannot set cols to ignore on an existing databsae"
                )
            if dmeta_cols:
                raise ValueError("You cannot set dmeta cols on an existing databsae")

        else:
            self.initialise_db()

        self.unique_id_col = None
        self.example_record = None
        self.token_tables_empty = True

        # If connected to a database that already has records
        if not self._table_df_is_empty:
            self.set_unique_id_col_from_db()
            self.set_example_record_from_db()
            self.set_cols_to_ignore_from_db()
            self.set_dmeta_cols_from_db()
            self.check_col_counters()

        # If the user is adding multiple talbes (e.g. calling write_pandas_dataframe several times)
        # it's more performant to retain column counters across these tables and write them once
        # rather than writing after each table
        self.column_counters = None

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

    def initialise_db(self):
        c = self.conn.cursor()
        #                     (unique_id TEXT NOT NULL PRIMARY KEY,
        c.execute(
            """CREATE TABLE df
                    (unique_id TEXT NOT NULL PRIMARY KEY,
                     original_record JSON,
                     concat_all TEXT)
                  """
        )

        # This is a key/value tore used to store various items of state, such as name of unique_id_col
        # whether any token counts have been written so far
        c.execute(
            """
            CREATE TABLE db_state

            (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """
        )
        self.conn.commit()

        self.set_key_value_to_db_state_table("unique_id_col", None)
        self.set_key_value_to_db_state_table("col_counters_in_sync", "true")
        self.set_key_value_to_db_state_table(
            "cols_to_ignore", json.dumps(self.cols_to_ignore)
        )
        self.set_key_value_to_db_state_table("dmeta_cols", json.dumps(self.dmeta_cols))

        # Create FTS table
        sql = """
        CREATE VIRTUAL TABLE fts_target
        USING fts5(unique_id, concat_all);
        """
        c.execute(sql)
        self.conn.commit()

        # https://stackoverflow.com/questions/1711631/improve-insert-per-second-performance-of-sqlite
        c.execute("PRAGMA synchronous = EXTRA")
        c.execute("PRAGMA journal_mode = WAL")

        c.close()

    def set_key_value_to_db_state_table(self, key, value):

        c = self.conn.cursor()
        sql = """
            INSERT OR IGNORE INTO db_state VALUES (?, ?)
        """
        c.execute(sql, (key, value))

        sql = """
        UPDATE db_state SET value = ?
            WHERE key = ?;
        """
        c.execute(sql, (value, key))

        c.close()
        self.conn.commit()

    def get_value_from_db_state_table(self, key):

        sql = """
            select * from db_state
            where key = ?
        """
        c = self.conn.execute(sql, (key,))
        results = c.fetchall()
        return results[0]["value"]

    def check_col_counters(self):
        val = self.get_value_from_db_state_table("col_counters_in_sync")
        if val != "true":
            warnings.warn(
                "Your token counters are out of sync with the database, rebuild recommended."
            )

    def initialise_token_tables(self):
        rec = self.example_record
        columns = rec.columns_to_index
        c = self.conn.cursor()

        for col in columns:
            sql = f"""
                    CREATE TABLE {col}_token_counts
                    (token text PRIMARY KEY, token_count int, token_proportion float)
                   """
            c.execute(sql)

        c.close()

    @staticmethod
    def _record_dict_to_insert_data(
        record_dict: dict,
        unique_id_col: str,
        cols_to_ignore: list = [],
        dmeta_cols: list = None,
    ):
        """Process a single record dict into data reaady to be entered into the database

        Args:
            record_dict (dict): A dictionary representing a record

        Returns:
            dict: A dictionary containing the tuple needed for an INSERT statmenent, and
                  'col_token_counts' a dictionary of Counters(), one for each column, containing token counts
        """

        record = Record(
            record_dict,
            unique_id_col=unique_id_col,
            cols_to_ignore=cols_to_ignore,
            dmeta_cols=dmeta_cols,
        )
        uid = record.id
        jsond = json.dumps(record.record_dict)
        concat = record.tokenised_stringified_with_misspellings

        df_tuple = (uid, jsond, concat)

        columns = record.columns_to_index
        tfd = record.tokenised_including_mispellings

        column_counters = ColumnCounters(record)
        for col in columns:
            tokens = tfd[col]
            column_counters.update_single_column(col, tokens)

        return {"df_tuple": df_tuple, "column_counters": column_counters}

    @staticmethod
    def _record_batch_to_insert_data(
        record_dicts: list, unique_id_col: str, cols_to_ignore: list, dmeta_cols
    ):
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
            "column_counters": ColumnCounters(
                Record(
                    record_dicts[0],
                    unique_id_col=unique_id_col,
                    cols_to_ignore=cols_to_ignore,
                    dmeta_cols=dmeta_cols,
                )
            ),
            "original_dicts": record_dicts,
        }

        for rd in record_dicts:
            single_record_results = SearchDatabase._record_dict_to_insert_data(
                rd,
                unique_id_col=unique_id_col,
                cols_to_ignore=cols_to_ignore,
                dmeta_cols=dmeta_cols,
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

        # This will have errored out and rolled back already if there was an integrity error
        # i.e. these lines will not be hit

        # Insert FTS entries
        result_tuples = [(t[0], t[2]) for t in result_tuples]

        sql = """
        INSERT INTO fts_target
        VALUES (?, ?)
        """
        c.executemany(sql, result_tuples)
        c.close()
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
            insert_data = self._record_dict_to_insert_data(
                record_dict, unique_id_col=self.unique_id_col
            )
            insert_tuple = insert_data["df_tuple"]
            try:
                c.execute("INSERT INTO df VALUES (?, ?, ?)", insert_tuple)
                fts_tuple = (insert_tuple[0], insert_tuple[2])
                c.execute("INSERT INTO fts_target VALUES (?, ?)", fts_tuple)
            except sqlite3.IntegrityError:
                logger.debug(
                    f"Record id {insert_tuple[0]} already exists in db, ignoring"
                )
                continue

            new_column_counters = insert_data["column_counters"]
            column_counters.update(new_column_counters)
        c.close()
        self.conn.commit()

    def write_list_dicts_parallel(
        self,
        list_dicts: list,
        unique_id_col: str,
        batch_size=10_000,
        write_column_counters=True,
    ):
        """Process a list of dicts containing records in parallel, turning them into data ready to be inserted
        into the databse, then insert

        Args:
            list_dicts (list): A list of dictionaries, each one representing a record
            batch_size (int, optional): How many records to send to each parallel worker. Defaults to 10000.
        """

        # This may be the first time we've seen a record.  If so, need to do some setup
        if self.unique_id_col is None:
            self.unique_id_col = unique_id_col
            self.set_key_value_to_db_state_table("unique_id_col", unique_id_col)

        if self.example_record is None:
            record = Record(
                list_dicts[0],
                unique_id_col=self.unique_id_col,
                cols_to_ignore=self.cols_to_ignore,
                dmeta_cols=self.dmeta_cols,
            )
            self.example_record = record
            self.initialise_token_tables()

        batches_of_records = chunk_list(list_dicts, batch_size)

        ##########################
        # Start of parallelisation
        ##########################
        p = Pool()
        fn = partial(
            self._record_batch_to_insert_data,
            unique_id_col=self.unique_id_col,
            cols_to_ignore=self.cols_to_ignore,
            dmeta_cols=self.dmeta_cols,
        )
        results_batches = p.imap_unordered(fn, batches_of_records)

        if self.column_counters is None:
            self.column_counters = ColumnCounters(self.example_record)

        # Since it's an imap, which results in a generator
        # This starts executing as soon as the first batch completes.
        # So these writes don't need parallising (it's much faster to run the insert than to process the batch)
        # Meaning we can just do them slowly while we're waiting for batches to compute
        # There's a gist here which demonstrates the principle:
        # https://gist.github.com/RobinL/4e6a266f0287df32f2aa7aee1b3a5450
        for results_batch in results_batches:
            # If an insert fails it's because one of the unique_ids already exists
            # If so, insert the records one by one, logging integrity errors
            try:
                self.bulk_insert_batch(results_batch, self.column_counters)
            except sqlite3.IntegrityError:
                self.insert_batch_one_by_one(
                    results_batch["original_dicts"], self.column_counters
                )

        p.close()
        p.join()

        self.set_key_value_to_db_state_table("col_counters_in_sync", "false")

        ##########################
        # End of parallelisation
        ##########################

        if write_column_counters:
            self.write_all_col_counters_to_db()

    def write_all_col_counters_to_db(self):

        logger.info("starting to write all col counters")

        columns = self.column_counters.columns
        c = self.conn.cursor()
        for col in columns:
            counter = self.column_counters[col]

            start_time = datetime.now()

            try:
                # This is slightly faster, but requires a relative new version of sqlite
                for token, value in counter.items():

                    sql = f"""
                        INSERT INTO {col}_token_counts VALUES (?, ?, ?)
                        ON CONFLICT(token) DO UPDATE SET
                        token_count = token_count + ?
                        WHERE token = ?;
                    """

                    c.execute(sql, (token, value, None, value, token))
            except sqlite3.OperationalError:
                # Older versions of sqlite that do not support upsert

                for token, value in counter.items():

                    sql = f"""
                        INSERT OR IGNORE INTO {col}_token_counts VALUES (?, ?, ?)
                    """
                    c.execute(sql, (token, 0, None))

                    sql = f"""
                    UPDATE {col}_token_counts SET token_count = token_count + {value}
                        WHERE token = '{token}';
                    """
                    c.execute(sql)

            duration = datetime.now() - start_time
            logger.debug(f"Writing column counters for {col} took {duration}")

        c.close()
        self.conn.commit()

        self.set_key_value_to_db_state_table("col_counters_in_sync", "true")

        # reset column counters
        self.column_counters = None

    def set_example_record_from_db(self):
        c = self.conn.cursor()

        c.execute("select original_record from df limit 1")
        record = c.fetchone()
        c.close()
        record = record["original_record"]
        record = json.loads(record)
        self.example_record = Record(
            record,
            unique_id_col=self.unique_id_col,
            cols_to_ignore=self.cols_to_ignore,
            dmeta_cols=self.dmeta_cols,
        )

    def set_cols_to_ignore_from_db(self):
        cols_to_ignore = self.get_value_from_db_state_table("cols_to_ignore")
        self.cols_to_ignore = json.loads(cols_to_ignore)

    def set_dmeta_cols_from_db(self):
        dmeta_cols = self.get_value_from_db_state_table("dmeta_cols")
        self.dmeta_cols = json.loads(dmeta_cols)

    def set_unique_id_col_from_db(self):
        unique_id_col = self.get_value_from_db_state_table("unique_id_col")

        self.unique_id_col = unique_id_col

    def write_pandas_dataframe(
        self,
        pd_df,
        unique_id_col: str,
        batch_size: int = 10_000,
        write_column_counters=True,
    ):

        records_as_dict = pd_df.to_dict(orient="records")

        self.write_list_dicts_parallel(
            records_as_dict,
            unique_id_col=unique_id_col,
            batch_size=batch_size,
            write_column_counters=write_column_counters,
        )

    def _update_token_stats_tables(self):
        rec = self.example_record
        columns = rec.columns_to_index

        c = self.conn.cursor()
        for col in columns:
            sql = f"""
            update  {col}_token_counts
            set token_proportion = cast(token_count as float)/(select sum(token_count) from {col}_token_counts)
            """
            c.execute(sql)
            self.conn.commit()
            logger.debug(f"Updated table {col}_token_counts.")

        c.close()

    def build_or_replace_stats_tables(self):
        self._update_token_stats_tables()

    def find_potental_matches(
        self,
        search_dict,
        return_records_limit=50,
        search_intensity=500,
        individual_search_limit=50,
    ):
        if self.unique_id_col not in search_dict:
            search_dict[self.unique_id_col] = "search_record_" + uuid.uuid4().hex
        else:
            search_dict[self.unique_id_col] = (
                str(search_dict[self.unique_id_col]) + uuid.uuid4().hex
            )

        finder = MatchFinder(
            search_dict,
            self,
            return_records_limit=return_records_limit,
            search_intensity=search_intensity,
            individual_search_limit=individual_search_limit,
        )
        finder.find_potential_matches()
        return finder.found_records

    def find_potential_matches_as_pandas(
        self,
        search_dict,
        return_records_limit=50,
        search_intensity=500,
        individual_search_limit=50,
    ):

        search_dict = copy.deepcopy(search_dict)
        if self.unique_id_col not in search_dict:
            search_dict[self.unique_id_col] = "search_record_" + uuid.uuid4().hex
        else:
            search_dict[self.unique_id_col] = (
                str(search_dict[self.unique_id_col]) + " " + uuid.uuid4().hex
            )

        finder = MatchFinder(
            search_dict,
            self,
            return_records_limit=return_records_limit,
            search_intensity=search_intensity,
            individual_search_limit=individual_search_limit,
        )
        finder.find_potential_matches()
        return finder.found_records_as_df


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
        columns = record.columns_to_index
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
