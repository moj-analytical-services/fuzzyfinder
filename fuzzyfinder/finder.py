import json
from math import inf
import random


from .comparison import RecordComparisonScorer
from .record import Record

import logging

logger = logging.getLogger(__name__)


class MatchFinder:
    def __init__(
        self,
        search_dict,
        sqlite_db_conn,
        return_records_limit=50,
        best_score_threshold=inf,
        search_intensity=500,
    ):

        self.conn = sqlite_db_conn
        if "unique_id" not in search_dict:
            search_dict["unique_id"] = "search_record"
        self.record = Record(search_dict, sqlite_db_conn)

        self.number_of_searches = 0
        self.return_records_limit = return_records_limit
        self.best_score_threshold = best_score_threshold
        self.search_intensity = search_intensity

        self.found_records = {}

        self.best_score = -inf

        self._searches = set()

    @property
    def found_records_as_df(self):
        try:
            import pandas as pd

            df = pd.DataFrame(self.found_records.values())
            return df.sort_values("score", ascending=False)
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "You've asked for the results as a pandas dataframe but pandas is not installed"
            )

    def find_potential_matches(self):

        strategies = [
            self._search_specific_to_general_all_tokens,
            self._search_specific_to_general_band,
            self._search_random,
        ]

        for strategy in strategies:
            if self.stop_searching(None):
                return None
            strategy()
            logger.debug(f"Total searches executed so far: {self.number_of_searches}")
        logger.info(f"Total records found: {len(self.found_records.keys())}")
        logger.info(f"Total searches executed: {self.number_of_searches}")

    def add_record_if_not_exists(self, r):
        rec_id = r["unique_id"]
        if rec_id not in self.found_records:
            record_dict = self.get_record_dict_from_id(rec_id)

            found_record = Record(record_dict, self.conn)
            scorer = RecordComparisonScorer(self.record, found_record)
            score = scorer.score

            record_dict["score"] = score
            record_dict["bm25_score"] = r["bm25_score"]

            self.found_records[rec_id] = record_dict

            self.best_score = max(score, self.best_score)

    def get_record_dict_from_id(self, rec_id):

        sql = f"""
        select original_record
        from df
        where unique_id = "{rec_id}"
        """
        c = self.conn.cursor()
        c.execute(sql)
        r = c.fetchone()
        c.close()
        rec_json = r["original_record"]
        rec_dict = json.loads(rec_json)
        return rec_dict

    def _fts_using_tokens(self, tokens, unique_id_field="unique_id"):

        # If a different search strategy has already tried this search do othing
        if frozenset(tokens) in self._searches:
            return

        self._searches.add(frozenset(tokens))

        self.number_of_searches = self.number_of_searches + 1

        num_ids_before = len(self.found_records.keys())

        # Tokens escaped in case sqlite keywords like NOT, AND etc appear in string
        # https://stackoverflow.com/questions/28971633/how-to-escape-string-for-sqlite-fts-query
        escaped_tokens = [f'"{t}"' for t in tokens]

        fts_string = " ".join(escaped_tokens)

        sql = f"""
            SELECT unique_id, bm25(fts_target) as bm25_score
            FROM fts_target
            WHERE concat_all
            MATCH
            '{fts_string}'
            LIMIT {self.return_records_limit}
            """
        logger.debug(f"Searching for {fts_string}")
        cur = self.conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        num_results = len(results)

        if (
            num_results < self.return_records_limit
        ):  # If query hit results limit then results unlikely to be useful
            for r in results:
                self.add_record_if_not_exists(r)

        num_ids_after = len(self.found_records.keys())
        num_new = num_ids_after - num_ids_before
        return {"num_new_recs_found": num_new, "num_results": num_results}

    def stop_searching(self, results):
        if self.best_score > self.best_score_threshold:
            return True
        if len(self.found_records.keys()) > self.return_records_limit:
            return True
        if results:
            if results["num_results"] == self.return_records_limit:
                return True
        return False

    ####################
    # SEARCH STRATEGIES
    # A search strategy is responsible for firing off various searches
    # And knowing when to stop searching
    ####################

    def _search_specific_to_general_all_tokens(self):
        """
        Search using all tokens.  Then remove the rarest, and search again.

        [a,b,c,d]
        [b,c,d]
        [c,d]
        [d]
        """
        logger.debug("Starting specific to general all tokens search")

        num_found_records_old = len(self.found_records.keys())

        tkns_rarity_order = self.record.tokens_in_order_of_rarity

        for i in range(len(tkns_rarity_order)):
            sub_tokens = tkns_rarity_order[i:]
            results = self._fts_using_tokens(sub_tokens)
            if self.stop_searching(results):
                break

        num_found_records_new = len(self.found_records.keys())
        num_new = num_found_records_new - num_found_records_old
        logger.debug(f"{num_new} new matches found")

    def _search_specific_to_general_band(self):
        """
        Search in blocks e.g. if tokens a b c d go
        [abcd]
        [abc]
        [bcd]
        [ab]
        [bc]
        [cd]
        [a]
        [b]
        [c]
        [d]
        """

        logger.debug("Starting specific to general band search")

        num_found_records_old = len(self.found_records.keys())

        tkns_rarity_order = self.record.tokens_in_order_of_rarity
        num_tokens = len(tkns_rarity_order)

        for band_size in range(num_tokens, 0, -1):

            take = num_tokens - band_size + 1
            for start_pos in range(0, take):
                end_pos = start_pos + band_size
                sub_tokens = tkns_rarity_order[start_pos:end_pos]

                results = self._fts_using_tokens(sub_tokens)

                if self.stop_searching(results):
                    break

            if self.stop_searching(results):
                break

        num_found_records_new = len(self.found_records.keys())
        num_new = num_found_records_new - num_found_records_old
        logger.debug(f"{num_new} new matches found")

    def _search_random(self):

        logger.debug("Starting random search")

        num_found_records_old = len(self.found_records.keys())

        tkns_rarity_order = self.record.tokens_in_order_of_rarity

        if len(tkns_rarity_order) > 2:

            for i in range(self.search_intensity):
                random_tokens = self._get_random_tokens(tkns_rarity_order)
                self._fts_using_tokens(random_tokens)

                if self.stop_searching(None):
                    break

        num_found_records_new = len(self.found_records.keys())
        num_new = num_found_records_new - num_found_records_old
        logger.debug(f"{num_new} new matches found")

    def _get_random_tokens(self, tokens):
        num_tokens = len(tokens)
        n = random.randint(2, num_tokens - 1)
        random_tokens = random.sample(tokens, n)
        return tuple(random_tokens)
