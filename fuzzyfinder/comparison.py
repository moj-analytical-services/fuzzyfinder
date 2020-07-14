from math import log10

# Another possibility would be to use something like
# https://github.com/simonw/sqlite-fts4/blob/9f4912f078d47a87aa534b3e835a3422a6d4ad96/sqlite_fts4/__init__.py#L221


class RecordComparisonScorer:
    def __init__(self, record_left, record_right):
        self.rl = record_left
        self.rr = record_right

        self.rl_tkn = record_left.tokenised_including_mispellings
        self.rr_tkn = record_right.tokenised_including_mispellings

        self.rl_prob = record_left.token_probabilities
        self.rr_prob = record_right.token_probabilities

    @property
    def score(self):
        probability = 1
        for col in self.rl.columns_except_unique_id:
            p = self.column_probability(col)
            probability = probability * p

        return self.prob_to_score(probability)

    def column_probability(self, col):

        tokens_left = set(self.rl_tkn[col])
        tokens_right = set(self.rr_tkn[col])

        matching_tokens = tokens_left.intersection(tokens_right)
        unmatching_tokens_left = tokens_left.difference(tokens_right)

        prob_matching = self._get_prob_matching(matching_tokens, col)
        prob_unmatching_l = self._get_prob_unmatching(unmatching_tokens_left, col)

        return prob_matching * prob_unmatching_l

    def _get_prob_matching(self, matching_tokens, col):
        prob = 1
        for t in matching_tokens:
            p = self.rr_prob[col][t]["proportion"]
            prob = p * prob
        return prob

    def _get_prob_unmatching(self, unmatching_tokens, col):
        # If the unmatching token is not a misspelling, then undo its probability
        prob = 1
        for t in unmatching_tokens:
            try:
                p = self.rr_prob[col][t]["proportion"]
            except KeyError:
                p = 1

            prob = p * prob

        return 1 / prob

    @staticmethod
    def prob_to_score(prob):
        return -(log10(prob)) / 30
