import re


def regex(IN: str, pattern: str, replacement: str) -> str:
    '''
    Matches with a REGEX pattern and replaces the matching portion with a specific string

    Args:
        IN (str): Input string to be masked
        pattern (str): REGEX pattern to match part to be replaced
        replacement (str): text to replace match with

    Returns:
        str: The masked string
    '''
    if IN == None: return None
    if pattern == None: raise ValueError("No Pattern to match")
    return re.sub(pattern,replacement,IN)