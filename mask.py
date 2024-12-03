#!/usr/bin/env python3
import re
import requests
import random
import arguably
import mysql.connector
import json

__version__ = "1.0.0"

secrets = {}
with open("secrets") as f:
    for line in f.readlines():
        ld = line.split("=")
        secrets[ld[0]] = ld[1]

@arguably.command
def mask(input: str, output: str, config: str = None):
    '''
    Mask the sensitive data in your mySQL or postgreSQL database

    Args:
        input: The address of the input database containing potentially sensitive data
        output: The address for the output database where the masked data will be inserted
        config: JSON file containing masking options for each field in the tables
    '''
    # mydb = mysql.connector.connect(
    # host="192.168.1.115",
    # user="root",
    # password="h4mster",
    # port="3306"
    # )
    if config != None:
        data = json.loads(open(config).read())
        

    print(regex("ahflkjsdhfakhdsm@skf.com","(?<=^.).*(?=.*@)","******"))

    print(partial("abcdefghijklmnopqrstuvwxyz",0,0,"#"))




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
    return re.sub(pattern,replacement,IN)




def partial(IN: str, visablePrefix: int, visableSuffix: int, maskingChar: str):
    '''
    Replaces a portion of a string with a specific character 

    Args:
        IN (str): Input string to be masked
        visablePrefix (int): Number of characters, at the start, to be visable
        visableSuffix (int): Number of characters, at the end, to be visable
        maskingChar (str): Character to replace the non-visable characters with

    Returns:
        str: The masked string
    '''
    OUT = IN
    for i in range(visablePrefix,len(IN) - visableSuffix):
        OUT = OUT[:i] + maskingChar + OUT[i +1:]
    return OUT




def redact(IN: str, CHAR: str) -> str:
    '''
    Replaces all characters of a string with a specific character

    Args:
        IN (str): Input string to be masked
        maskingChar (str): Character to replace the characters with

    Returns:
        str: The masked string
    '''
    OUT = ""
    for i in IN:
        OUT += CHAR
    return OUT

def scramble(IN: int, MIN: int = 0, MAX: int = 9) -> int:
    OUT = ""
    for i in str(IN):
        OUT += str(random.randint(MIN,MAX))
    return int(OUT)

def address(IN: list) -> list:
    url = "https://my.api.mockaroo.com/address.json"

    payload = {}
    headers = {
    'X-API-Key': secrets["mockaroo"]
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.text.split(",")
    addressData = {}
    addressData["streetNumber"] = data[0].strip()
    addressData["streetName"] = data[1].strip()
    addressData["streetSuffix"] = data[2].strip()
    addressData["street"] = addressData["streetName"] + " " + addressData["streetSuffix"]
    addressData["streetAddress"] = addressData["streetNumber"] + " " + addressData["street"]
    addressData["city"] = data[3].strip()
    addressData["country"] = data[4].strip()
    addressData["postcode"] = data[5].strip()

    OUT = []
    for i in IN:
        OUT.append(addressData[i])
    return OUT
    

if __name__ == "__main__":
    arguably.run()

