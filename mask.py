#!/usr/bin/env python3
import requests
import random
import arguably
import mysql.connector
import json
from getpass import getpass
from tqdm import tqdm
from partial import partial
from redact import redact
from regex import regex
import logging

logger = logging.getLogger(__name__)

__version__ = "1.0.0"

secrets = {}
with open("secrets") as f:
    for line in f.readlines():
        ld = line.split("=")
        secrets[ld[0]] = ld[1]

@arguably.command
def mask(inputDB: str, outputDB: str, *, config: str = None, logLevel: str = "INFO"):
    '''
    Mask the sensitive data in your mySQL or postgreSQL database

    Args:
        inputDB (str): The address of the input database containing potentially sensitive data
        outputDB (str): The address for the output database where the masked data will be inserted
        config (str, optional): [-c] JSON file containing masking options for each field in the tables
        logLevel (str, optional): [-L] Log level for output (e.g., DEBUG, INFO, WARNING) 
    '''
    logger.setLevel(logLevel)
    console_handler = logging.StreamHandler()
    
    formatter = logging.Formatter(
        "{levelname} - {message}",
        style="{"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    inputDBdata = inputDB.split(":")
    inputHost = inputDBdata[0]
    inputDatabase = inputDBdata[1]
    inputTable = inputDBdata[2]
    logger.info("Logging into " + inputHost)
    inputUsername = input("username =>")
    inputPassword = getpass()
    logger.info("Connecting...")
    inputDBconnection = mysql.connector.connect(
    host=inputHost,
    user=inputUsername,
    password=inputPassword,
    database=inputDatabase
    )
    logger.info("Connection Successful")
    inputCursor = inputDBconnection.cursor()
    logger.debug("Created input database cursor")
    logger.info("Loading data from table: " + inputTable)
    inputCursor.execute("DESCRIBE "+inputTable+";")
    columns = {}
    for i,column in enumerate(inputCursor.fetchall()):
        columns[column[0]] = i
    logger.debug("Got input database column names: " + str(len(columns.items())) + " columns")
    inputCursor.execute("SELECT * FROM "+inputTable+";")
    rows = inputCursor.fetchall()
    logger.debug("Got " + str(len(rows)) + " rows")

    outputTable = []
    if config != None:
        configData = json.loads(open(config).read())["fields"]
        for row in tqdm(rows, total=len(rows),desc="Masking data"):
            outputRow = {}
            # print(columns)
            for columnName,i in columns.items():
                inputData = row[i]
                if columnName not in configData.keys(): outputRow[columnName] = inputData; continue; # ignore fields not specified for masking
                maskData = configData[columnName]

                maskingType = maskData["maskingType"]
                if maskingType == None: raise ValueError("No Masking type for field: " + fieldName)

                if maskingType == "regex":
                    pattern = maskData["pattern"]
                    replacement = maskData["replacement"]
                    outputData = regex(inputData,pattern,replacement)
                    outputRow[columnName] = outputData
                elif maskingType == "redact":
                    replacement = maskData["replacement"]
                    outputData = redact(inputData,replacement)
                    outputRow[columnName] = outputData
                elif maskingType == "partial":
                    visiblePrefix = maskData["visiblePrefix"]
                    visibleSuffix = maskData["visibleSuffix"]
                    replacement = maskData["replacement"]
                    outputData = partial(inputData,visiblePrefix,visibleSuffix,replacement)
                    outputRow[columnName] = outputData
                else:
                    raise ValueError("Unsupported Masking Type: " + maskingType)
            outputTable.append(outputRow)


    outputDBdata = outputDB.split(":")
    outputHost = outputDBdata[0]
    outputDatabase = outputDBdata[1]
    outputTableName = outputDBdata[2]
    logger.info("Logging into " + outputHost)
    outputUsername = ""
    outputPassword = ""
    if outputHost == inputHost:
        outputUsername = inputUsername
        outputPassword = inputPassword
    else:
        outputUsername = input("username =>")
        outputPassword = getpass()
    logger.info("Connecting...")
    outputDBconnection = mysql.connector.connect(
    host=outputHost,
    user=outputUsername,
    password=outputPassword,
    database=outputDatabase
    )
    logger.info("Connection Successful")
    outputCursor = outputDBconnection.cursor()
    fails = []
    for row in tqdm(outputTable,total=len(outputTable),desc="Writing output"):
        outputColumns = "("
        outputValues = "("
        first = True
        for columnName,columnValue in row.items():
            if columnValue == None: continue;
            if not first: 
                outputColumns += ',' + columnName
                outputValues += ',"' + str(columnValue) + '"'
                continue
            else:
                outputColumns += columnName
                outputValues += '"' + str(columnValue) + '"'
                first = False
        outputColumns += ")"
        outputValues += ")"

        try:
            outputCursor.execute("INSERT INTO " + outputTableName + " " + outputColumns + " VALUES " + outputValues)
            
            outputDBconnection.commit()
        except mysql.connector.errors.IntegrityError:
            fails.append(row)

    if len(fails) > 0:
        logger.error(str(len(fails)) + " rows couldn't be inserted")
                




def scramble(IN: int, MIN: int = 0, MAX: int = 9) -> int:
    OUT = ""
    for i in str(IN):
        OUT += str(random.randint(MIN,MAX))
    return int(OUT)

def address(IN: list) -> dict:
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

    OUT = {}
    for i in IN:
        OUT[i] = addressData[i]
    return OUT
    

if __name__ == "__main__":
    
    arguably.run()

