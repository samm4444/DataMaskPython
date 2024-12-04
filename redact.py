def redact(IN: str, CHAR: str) -> str:
    '''
    Replaces all characters of a string with a specific character

    Args:
        IN (str): Input string to be masked
        maskingChar (str): Character to replace the characters with

    Returns:
        str: The masked string
    '''
    if IN == None: return None
    OUT = ""
    for i in IN:
        OUT += CHAR
    return OUT