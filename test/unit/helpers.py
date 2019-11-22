import re


def strings_match_ignore_whitespace(a, b):
    """
    Compare two base strings, disregarding whitespace
    """
    return re.sub("\\s*", "", a) == re.sub("\\s*", "", b)
