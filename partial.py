def partial(IN: str, visiblePrefix: int, visibleSuffix: int, maskingChar: str):
    '''
    Replaces a portion of a string with a specific character 

    Args:
        IN (str): Input string to be masked
        visiblePrefix (int): Number of characters, at the start, to be visable
        visibleSuffix (int): Number of characters, at the end, to be visable
        maskingChar (str): Character to replace the non-visable characters with

    Returns:
        str: The masked string
    '''
    if IN == None: return None
    OUT = IN
    for i in range(visiblePrefix,len(IN) - visibleSuffix):
        OUT = OUT[:i] + maskingChar + OUT[i +1:]
    return OUT