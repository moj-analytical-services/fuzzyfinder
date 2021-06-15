import warnings
from difflib import ndiff

try:
    from rapidfuzz.string_metric import levenshtein

    levenshtein_distance = levenshtein
except ModuleNotFoundError:
    levenshtein_distance = None


def backup_levenshtein(str_1, str_2):
    """
    The Levenshtein distance is a string metric for measuring the difference between two sequences.
    It is calculated as the minimum number of single-character edits necessary to transform one string into another
    """

    distance = 0
    buffer_removed = buffer_added = 0
    for x in ndiff(str_1, str_2):
        code = x[0]
        # Code ? is ignored as it does not translate to any modification
        if code == " ":
            distance += max(buffer_removed, buffer_added)
            buffer_removed = buffer_added = 0
        elif code == "-":
            buffer_removed += 1
        elif code == "+":
            buffer_added += 1
    distance += max(buffer_removed, buffer_added)
    return distance


if not levenshtein_distance:
    warnings.warn(
        "Using a native Python levenstein function.  pip install rapidfuzz for a faster implementation"
    )
    levenshtein_distance = backup_levenshtein
