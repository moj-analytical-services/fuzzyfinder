from copy import deepcopy
from functools import lru_cache
import re
import sqlite3
from metaphone import doublemetaphone
import math


class Record:
    """Represents a row of a dataset.

    A row is represented as a dictionary called 'row_dict' like :

    {
        "col1": "val1",
        "col2": "val2"
    }

    The record object has methods to clean (homogenise) and tokenise and transform these values to make
    the record easier to match

    Records will be members of Python sets, so contains a method to make sure the record can be hashed.
    """

    def __init__(
        self,
        record_dict: dict,
        unique_id_col: str,
        sqlite_db_conn: sqlite3.Connection = None,
        cols_to_ignore: list = [],
        dmeta_cols: list = None,
    ):
        """
        Args:
            record_dict (dict): A row of data represented as a dictionary with colnames as keys and col values as values
            unique_id_col (str): The column that contains the unique record identifier.
            cols_to_ignore (list): List of columns that should be ignored when populating the FTS search database
            dmeta_cols (list): List of columns to create dmetaphone token variants for
            sqlite_db_conn (sqlie3.Connection):  A connection to a sqlite database that contains column statistics

        """

        self.record_dict = deepcopy(record_dict)
        self.conn = sqlite_db_conn
        self.unique_id_col = unique_id_col

        self.cols_to_ignore = cols_to_ignore
        self.dmeta_cols = dmeta_cols

        if self.unique_id_col not in self.record_dict:
            raise KeyError(
                f"The unique_id_col {self.unique_id_col} you specified does not exist in the record"
            )

    def __hash__(self):
        return hash(self.record_dict[self.unique_id_col])

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    @property
    def id(self):
        return self.record_dict[self.unique_id_col]

    @property
    def columns_except_unique_id(self):
        cols = self.record_dict.keys()
        return [c for c in cols if c != self.unique_id_col]

    @property
    def columns_to_index(self):
        cols = self.record_dict.keys()
        cols = [c for c in cols if c != self.unique_id_col]
        cols = [c for c in cols if c not in self.cols_to_ignore]
        return cols

    @staticmethod
    @lru_cache(maxsize=int(1e6))
    def tokenise_value(value):
        if value is None:
            return []

        if type(value) == float:
            if math.isnan(value):
                value = ""
            else:
                value = f"{value:.4g}".replace(".", "")
                value = re.sub(r"e\+\d{1,4}", "", value)
        else:
            value = str(value)

        if value.strip() == "":
            return []

        value = value.upper()

        value = re.sub(r"\s{2,100}", " ", value)  # Multiple spaces become a space
        value = re.sub(r"[^\w\s]", " ", value)  # Any punctuation becomes a space

        if len(value) > 5:  # Tokenise long words at boundary between char and num
            value = re.sub(r"([A-Z])(\d)", r"\1 \2", value)
            value = re.sub(r"(\d)([A-Z])", r"\1 \2", value)  # Vice versa

        # Bad idea?  Split up into words of max length 8
        value = re.sub(r"(\w{8})", r"\1 ", value)
        value = re.sub(r"\s{2,100}", " ", value)

        value = value.strip()

        return value.split(" ")

    @property
    def tokenised(self):
        """The original record dictionary with values tokenised into arrays {"col1": [tkn1, tkn2], "col2":[]}"""
        record_tokenised = {}
        for col, value in self.record_dict.items():
            if col in self.columns_to_index:
                record_tokenised[col] = Record.tokenise_value(value)
        return record_tokenised

    @staticmethod
    @lru_cache(maxsize=int(1e6))
    def get_dmetaphone_tokens(token):
        if len(token) > 2 and not any(i.isdigit() for i in token):
            misspellings = doublemetaphone(token)
            misspellings = [t for t in misspellings if t != ""]
        else:
            misspellings = []
        return misspellings

    @staticmethod
    def tokens_to_misspelling_tokens(tokens: list):
        misspelling_tokens = []
        for t in tokens:
            misspelling_tokens.extend(Record.get_dmetaphone_tokens(t))
        return misspelling_tokens

    @property
    def tokenised_misspellings(self):
        """The original record dictionary with dmetaphone tokens instead of original values"""
        tokenised_misspellings = {}
        for col, tokens in self.tokenised.items():
            if self.dmeta_cols is None:
                tokenised_misspellings[col] = Record.tokens_to_misspelling_tokens(
                    tokens
                )
            elif col in self.dmeta_cols:
                tokenised_misspellings[col] = Record.tokens_to_misspelling_tokens(
                    tokens
                )

        return tokenised_misspellings

    @property
    def tokenised_including_mispellings(self):
        tfd = self.tokenised
        tfdm = self.tokenised_misspellings
        for col in tfd:
            if self.dmeta_cols is None:
                tfd[col] = tfd[col] + tfdm[col]
            elif col in self.dmeta_cols:
                tfd[col] = tfd[col] + tfdm[col]
        return tfd

    @property
    def tokenised_stringified_no_misspellings(self):
        all_tokens = []
        for col_tokens in self.tokenised.values():
            all_tokens.extend(col_tokens)
        return " ".join(all_tokens)

    @property
    def tokenised_stringified_with_misspellings(self):
        all_tokens = []
        for col_tokens in self.tokenised_including_mispellings.values():
            all_tokens.extend(col_tokens)
        return " ".join(all_tokens)

    @property
    @lru_cache(maxsize=int(1e6))
    def token_probabilities(self):

        tfd = self.tokenised_including_mispellings

        tfdp = {}

        tokens = []
        for col, tokens in tfd.items():
            tfdp[col] = {}
            for token in tokens:
                value = get_token_proportion(token, col, self.conn)
                tfdp[col][token] = value

        return tfdp

    @property
    def tokens_in_order_of_rarity(self):
        tfdp = self.token_probabilities

        token_list = []
        for col in tfdp.keys():
            for v in tfdp[col].values():
                token_list.append(v)
        token_list = [
            t for t in token_list if t["proportion"] != "does_not_exist_in_db"
        ]
        token_list.sort(key=lambda x: x["proportion"])
        return tuple([t["token"] for t in token_list])

    def __repr__(self):
        return f"Record: {self.record_dict.__repr__()}."


@lru_cache(maxsize=int(1e6))
def get_token_proportion(token, column, conn):
    c = conn.cursor()
    sql = f"""
    select token_proportion
    from {column}_token_counts
    where token = "{token}"
    """

    c.execute(sql)
    d = c.fetchone()
    c.close()

    if not d:
        # If the token NEVER appears in the search database, 'deprioritise' it in searches
        value = {"token": token, "proportion": "does_not_exist_in_db"}
    else:
        value = {"token": token, "proportion": d["token_proportion"]}

    return value
