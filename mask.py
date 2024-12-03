#!/usr/bin/env python3
import re
import requests
import random
import arguably

@arguably.command
def mask(input: str, output: str, options: str):
    """
    Mask the sensitive data in your mySQL or postgreSQL database

    Args:
        input: The address of the input database containing potentially sensitive data
        output: The address for the output database where the masked data will be inserted
        options: JSON file containing masking options for each field in the tables
    """

    print(redact(input,"#"))


def redact(IN: str, CHAR: str) -> str:

    OUT = ""
    for i in IN:
        OUT += CHAR
    return OUT

def scramble(IN: int, MIN: int = 0, MAX: int = 9) -> int:
    OUT = ""
    for i in str(IN):
        OUT += str(random.randint(MIN,MAX))
    return int(OUT)

def address(IN: list):
    url = "https://my.api.mockaroo.com/address.json"

    payload = {}
    headers = {
    'X-API-Key': '992b93b0'
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

