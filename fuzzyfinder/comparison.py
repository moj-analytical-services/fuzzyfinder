from math import log10
import logging

from .leven import levenshtein_distance

logger = logging.getLogger(__name__)


def leven_ratio(str_1, str_2):
    return 1 - levenshtein_distance(str_1, str_2) / max(len(str_1), len(str_2))


# Another possibility would be to use something like
# https://github.com/simonw/sqlite-fts4/blob/9f4912f078d47a87aa534b3e835a3422a6d4ad96/sqlite_fts4/__init__.py#L221


class RecordComparisonScorer:
    def __init__(self, search_rec, potenital_match_rec):
        self.search_rec = search_rec
        self.potenital_match_rec = potenital_match_rec

        self.search_rec_tkns = search_rec.tokenised_including_mispellings
        self.potential_match_rec_tkns = (
            potenital_match_rec.tokenised_including_mispellings
        )

        # Create dict of token probs from both records
        token_probs_s = search_rec.token_probabilities
        token_probs_p = potenital_match_rec.token_probabilities
        self.token_probs = {}
        for col in token_probs_p:
            try:
                self.token_probs[col] = {**token_probs_p[col], **token_probs_s[col]}
            # If this column not specified in serach record
            except KeyError:
                self.token_probs[col] = token_probs_p[col]

    @property
    def score(self):
        probability = 1
        # logger.debug('starting scoring')
        # logger.debug(f'search rec is {self.search_rec.record_dict}')
        # logger.debug(f'comparison rec is {self.potenital_match_rec.record_dict}')
        for col in self.search_rec.columns_except_unique_id:
            # logger.debug(f'scoring col {col}')
            p = self.column_probability(col)
            probability = probability * p

        return self.prob_to_score(probability)

    def column_probability(self, col):

        col_search_tkns = set(self.search_rec_tkns[col])
        col_potential_match_tkns = set(self.potential_match_rec_tkns[col])

        matching_tokens = col_search_tkns.intersection(col_potential_match_tkns)
        # logger.debug(f'matching tokens are {matching_tokens}')
        unmatching_tokens_in_search_rec = col_search_tkns.difference(
            col_potential_match_tkns
        )  # Tokens in search record not in found rec
        # logger.debug(f'unmatching tokens are {unmatching_tokens_in_search_rec}')

        prob_matching = self._get_prob_matching(matching_tokens, col)
        prob_unmatching = self._get_prob_unmatching(
            unmatching_tokens_in_search_rec, col
        )

        return prob_matching * prob_unmatching

    def _get_prob_matching(self, matching_tokens, col):

        prob = 1
        for t in matching_tokens:
            p = self.token_probs[col][t]["proportion"]
            # logger.debug(f'Scoring matching token {t} from search record as {p}')
            prob = p * prob
        return prob

    def _get_prob_unmatching(self, unmatching_tokens_from_search_record, col):
        # If the unmatching token is not a misspelling, then undo its probability
        prob = 1

        for (
            t
        ) in (
            unmatching_tokens_from_search_record
        ):  # Tokens in the search record which are NOT in the comparison record

            if self.token_is_misspelling(
                col, t
            ):  # If it's a potential misspelling neither punish nor reqard
                p = 1
            else:
                p = self.token_probs[col][t]["proportion"]
            # logger.debug(f'Scoring unmatching token {t} from search record as {p}')
            prob = p * prob

        return 1 / prob

    def token_is_misspelling(self, col, token_from_search_record):
        for t in self.potential_match_rec_tkns[col]:
            if leven_ratio(token_from_search_record, t) > 0.65:
                # logger.debug(f'Token {token_from_search_record} is misspelling of {t}')
                return True
        return False

    @staticmethod
    def prob_to_score(prob):
        return -(log10(prob)) / 30
