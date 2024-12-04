import random


def scrambleInt(IN: str, MIN: int = 0, MAX: int = 9, length: int = None) -> str:
    """
    Generates a random sequence of numbers

    Args:
        IN (str): Input string to be masked 
        MIN (int, optional): The smallest a number in the sequence can be. Defaults to 0.
        MAX (int, optional): The largest a number in the sequence can be. Defaults to 9.
        length (int, optional): The length of the output sequence. If not specified it will default to the length of the input.

    Returns:
        str: The output sequence
    """
    OUT = ""
    if length != None:
        for _ in range(length):
            OUT += str(random.randint(MIN,MAX))
    else:
        for i in str(IN):
            OUT += str(random.randint(MIN,MAX))
    return OUT