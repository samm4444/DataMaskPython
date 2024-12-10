#!/usr/bin/env python3
import multiprocessing.pool
import requests
import arguably
import mysql.connector
import json
from getpass import getpass
from tqdm import tqdm
import logging
import json
import re
import random
import multiprocessing
from functools import partial

logger = logging.getLogger(__name__)

__version__ = "1.0.1"

# open secrets file and import all client secrets.
secrets = {}
with open("secrets", "w+") as f:
    for line in f.readlines():
        ld = line.split("=")
        secrets[ld[0]] = ld[1]

@arguably.command
def mask(inputDB: str, outputDB: str, config: str, *, logLevel: str = "INFO"):
    '''
    Mask the sensitive data in your mySQL database

    Args:
        inputDB (str): The address of the input database. Format: host:database:table
        outputDB (str): The address for the output database. Format: host:database:table
        config (str): JSON file containing masking options for each field in the tables. This can be generated using the setup command.
        logLevel (str, optional): [-L] Log level for output (e.g., DEBUG, INFO, WARNING) 
    '''
    # set up logger to specified log level
    logger.setLevel(logLevel)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "{levelname} - {message}",
        style="{"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # read config file
    try:
        configData = json.loads(open(config).read())["fields"]
    except FileNotFoundError:
        logger.error("Could not find config file: " + config)
        return

    inputDBdata = inputDB.split(":")
    try:
        inputDBHost = inputDBdata[0]
        inputDatabaseName = inputDBdata[1]
        inputDBTable = inputDBdata[2]
    except IndexError:
        logger.error("Input database malformed: " + inputDB)
        return
    # get username and password input for db
    logger.info("Logging into input database: " + inputDBHost)
    inputDBUsername = input("Username:")
    inputDBPassword = getpass()

    try:
        # establish database connection with credentials
        logger.info("Connecting...")
        inputDBconnection = mysql.connector.connect(
        host=inputDBHost,
        user=inputDBUsername,
        password=inputDBPassword,
        database=inputDatabaseName
        )
    except mysql.connector.errors.ProgrammingError:
        logger.error("Failed to connect to database")
        return
    logger.info("Connection Successful")

    inputDBCursor = inputDBconnection.cursor()
    logger.debug("Created input database cursor")

    logger.info("Loading data from table: " + inputDBTable)
    
    # get the names of each field and their index within a row
    inputDBCursor.execute("DESCRIBE "+inputDBTable+";")
    columns = {}
    for i,column in enumerate(inputDBCursor.fetchall()):
        columns[column[0]] = i
    logger.debug("Got input database column names: " + str(len(columns.items())) + " columns")

    # get rows of data to be masked
    inputDBCursor.execute("SELECT * FROM "+inputDBTable+";")
    inputDBRows = inputDBCursor.fetchall()
    logger.debug("Got " + str(len(inputDBRows)) + " rows")


    # create SQL style names of columns for INSERT statement
    outputColumns = "("
    for columnName,i in columns.items():
        outputColumns += columnName + ","
    outputColumns = outputColumns[:-1]
    outputColumns += ")"

    outputDBdata = outputDB.split(":")
    try:
        outputDBHost = outputDBdata[0]
        outputDatabaseName = outputDBdata[1]
        outputDBTable = outputDBdata[2]
    except IndexError:
        logger.error("Output database malformed: " + outputDB)
        return

    # get username and password input for db if the host is different to the input db
    logger.info("Logging into output database: " + outputDBHost)
    outputDBUsername = ""
    outputDBPassword = ""
    if outputDBHost == inputDBHost:
        outputDBUsername = inputDBUsername
        outputDBPassword = inputDBPassword
        logger.info("Reusing username and password")
    else:
        outputDBUsername = input("Username:")
        outputDBPassword = getpass()

    try:
        # establish database connection with credentials
        logger.info("Connecting...")
        outputDBconnection = mysql.connector.connect(
        host=outputDBHost,
        user=outputDBUsername,
        password=outputDBPassword,
        database=outputDatabaseName
        )
    except mysql.connector.errors.ProgrammingError:
        logger.error("Failed to connect to database")
        return
    logger.info("Connection Successful")

    outputDBCursor = outputDBconnection.cursor()

    # multiprocessing
    pool = multiprocessing.Pool()
    partialMaskRow = partial(maskRow,columns=columns, configData=configData, outputDBTable=outputDBTable, outputColumns=outputColumns)
    results = pool.map(partialMaskRow, inputDBRows)
    rowsMasked = 0
    for stmt in tqdm(results, desc="Masking"):
        try:    
            outputDBCursor.execute(stmt)
            rowsMasked += 1
        except mysql.connector.errors.IntegrityError:
            pass
        
    pool.close()
    


    # for row in tqdm(inputDBRows, total=len(inputDBRows),desc="Masking"):
    #     # print(columns)
    #     outputValues = []
    #     for columnName,i in columns.items():
    #         inputData = row[i]

    #         if columnName not in configData.keys(): outputValues.append("".join(['"',str(inputData),'"'])); continue; # ignore fields not specified for masking

    #         maskSettings = configData[columnName] # get masking settings for this column

    #         maskingType = maskSettings["maskingType"]
    #         if maskingType == None: raise ValueError("No Masking type for field: " + columnName)

    #         if maskingType == "regex": # Mask data with REGEX
    #             outputData = regex(inputData,maskSettings["pattern"],maskSettings["replacement"])
    #             outputValues.append("".join(['"',str(outputData),'"']))
    #             continue
    #         elif maskingType == "redact": # Mask data with Redact
    #             outputData = redact(inputData,maskSettings["replacement"])
    #             outputValues.append("".join(['"',str(outputData),'"']))
    #             continue
    #         elif maskingType == "partial": # Mask data with Partial Redact
    #             outputData = partialRedact(inputData,maskSettings["visiblePrefix"],maskSettings["visibleSuffix"],maskSettings["replacement"])
    #             outputValues.append("".join(['"',str(outputData),'"']))
    #             continue
    #         elif maskingType == "scrambleInt":
    #             length = None
    #             if maskSettings["length"].lower() != "none":
    #                 length = int(maskSettings["length"])
    #             outputData = scrambleInt(inputData,maskSettings["min"],maskSettings["max"],length)
    #             outputValues.append("".join(['"',str(outputData),'"']))
    #             continue
    #         else: # Fail due to unknown masking type in config file
    #             logger.error("Unsupported Masking Type: " + maskingType)
    #             return
        
    #     outputRowSTR = "".join(["(",','.join(outputValues),")"])

    #     outputDBCursor.execute("INSERT INTO " + outputDBTable + " " + outputColumns + " VALUES " + outputRowSTR + ";")    

    #insertFails = []

    outputDBconnection.commit()

    logger.info("Masked " + str(rowsMasked) + " rows.")

    inputDBconnection.close()
    outputDBconnection.close()
        
        #insertFails.append({"row": row, "error": "IntegrityError"})

    # if len(insertFails) > 0:
    #     logger.error(str(len(insertFails)) + " rows couldn't be inserted")


def maskRow(row, columns, configData, outputDBTable, outputColumns):
    # print(columns)
    outputValues = []
    for columnName,i in columns.items():
        inputData = row[i]

        if columnName not in configData.keys(): outputValues.append("".join(['"',str(inputData),'"'])); continue; # ignore fields not specified for masking

        maskSettings = configData[columnName] # get masking settings for this column

        maskingType = maskSettings["maskingType"]
        if maskingType == None: raise ValueError("No Masking type for field: " + columnName)

        if maskingType == "regex": # Mask data with REGEX
            outputData = regex(inputData,maskSettings["pattern"],maskSettings["replacement"])
            outputValues.append("".join(['"',str(outputData),'"']))
            continue
        elif maskingType == "redact": # Mask data with Redact
            outputData = redact(inputData,maskSettings["replacement"])
            outputValues.append("".join(['"',str(outputData),'"']))
            continue
        elif maskingType == "partial": # Mask data with Partial Redact
            outputData = partialRedact(inputData,maskSettings["visiblePrefix"],maskSettings["visibleSuffix"],maskSettings["replacement"])
            outputValues.append("".join(['"',str(outputData),'"']))
            continue
        elif maskingType == "scrambleInt":
            length = None
            if maskSettings["length"].lower() != "none":
                length = int(maskSettings["length"])
            outputData = scrambleInt(inputData,maskSettings["min"],maskSettings["max"],length)
            outputValues.append("".join(['"',str(outputData),'"']))
            continue
        else: # Fail due to unknown masking type in config file
            logger.error("Unsupported Masking Type: " + maskingType)
            return
    
    outputRowSTR = "".join(["(",','.join(outputValues),")"])

    return "".join(["INSERT INTO ",outputDBTable," ",outputColumns," VALUES ",outputRowSTR,";"])    


### POSTGRESQL TEST

@arguably.command
def postgreSQL(host:str, db: str, user: str, password: str):
    '''
    postresql test 

    Args:
        host (str): host
        db (str): db
        user (str): user
        password (str): pass
    '''
    import psycopg2

    connection = psycopg2.connect(database=db, user=user, password=password, host=host)

    cursor = connection.cursor()

    cursor.execute("SELECT * FROM userSensitive;")

    # Fetch all rows from database
    record = cursor.fetchall()

    print("Data from Database:- ", record[0])

    connection.close()


### JSON CONFIG SETUP


@arguably.command
def setup(filename: str):
    '''
    Set the masking type for each field 

    Args:
        filename (str): The filename for the file to be created
    '''
    config = {}

    while True:
        fieldName = input("Field Name (q to end) =>")
        if fieldName == "q": break
        fieldConfig = getMaskConfig()
        config[fieldName] = fieldConfig

    file = {"fields": config}
    jsonData = json.dumps(file)


    with open(filename,"w+") as f:
        f.write(jsonData)
@arguably.command
def clean(database: str):
    '''
    Remove all the records from the table 

    Args:
        database (str): The address of the input database. Format: host:database:table
    '''

    DBdata = database.split(":")
    try:
        DBHost = DBdata[0]
        DatabaseName = DBdata[1]
        DBTable = DBdata[2]
    except IndexError:
        logger.error("Input database malformed: " + database)
        return
    # get username and password input for db
    logger.info("Logging into " + DBHost)
    DBUsername = input("username =>")
    DBPassword = getpass()
    try:
    # establish database connection with credentials
        logger.info("Connecting...")
        DBconnection = mysql.connector.connect(
        host=DBHost,
        user=DBUsername,
        password=DBPassword,
        database=DatabaseName
        )
    except mysql.connector.errors.ProgrammingError:
        logger.error("Failed to connect to database")
        return
    logger.info("Connection Successful")

    DBCursor = DBconnection.cursor()
    logger.debug("Created input database cursor")

    logger.info("Loading data from table: " + DBTable)
    
    print("Are you sure you want to remove all records from " + DatabaseName + " - " + DBTable + "? Type YES to confirm.")
    confirmation = input()
    if confirmation == "YES":
        print("Deleting...")
        DBCursor.execute("DELETE FROM "+DBTable+";")
        DBconnection.commit()
        print(str(DBCursor.rowcount) + " rows removed!")
    else:
        print("Stopping without removing any rows!")


masks = [{"id": "redact",
          "displayName":"Redact",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          }]},
         {"id": "partial",
          "displayName":"Partial",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          },
          {
            "id": "visiblePrefix",
            "displayName": "Visible Prefix",
            "description": "The Number of visible characters at the start of the text",
            "type": "int"
          },
          {
            "id": "visibleSuffix",
            "displayName": "Visible Suffix",
            "description": "The Number of visible characters at the end of the text",
            "type": "int"
          }]},
         {"id": "regex",
          "displayName":"Regular Expression",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          },
          {
            "id": "pattern",
            "displayName": "Regular Expression",
            "description": "The regualar expression pattern to match",
            "type": "str"
          }]},
         {"id": "scrambleInt",
          "displayName":"Scramble Integer",
          "params": [
            {
            "id": "min",
            "displayName": "Minimum",
            "description": "The smallest a number in the sequence can be.",
            "type": "int"
          },
          {
            "id": "max",
            "displayName": "Maximum",
            "description": "The largest a number in the sequence can be.",
            "type": "int"
          },
          {
            "id": "length",
            "displayName": "Length",
            "description": "The Length of the masked sequence. Type None to keep the length of the input.",
            "type": "str"
          }]}]

def getMaskConfig() -> dict:
  config = {}
  print("Select Mask Type")
  for i,mask in enumerate(masks):
    print(str(i+1) + ". " + mask["displayName"])
  
  while True:
    try:
      maskID = int(input("=>"))
      mask = masks[maskID-1]
      break
    except TypeError and KeyError:
      continue
  config["maskingType"] = mask["id"]
  for param in mask["params"]:
    paramId = param["id"]
    paramDisplayName = param["displayName"]
    paramDescription = param["description"]
    paramType = param["type"]

    print(paramDisplayName)
    print(paramDescription)
    if paramType == "str":
      paramValue = input("=>")
      config[paramId] = paramValue
    elif paramType == "int":
      try:
        paramValue = int(input("=>"))
        config[paramId] = paramValue
      except TypeError:
        print("Enter a number")
  return config


### REDACT FUNCTIONS


def partialRedact(IN: str, visiblePrefix: int, visibleSuffix: int, maskingChar: str):
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
        OUT = "".join([OUT[:i],maskingChar,OUT[i +1:]])
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
    if IN == None: return None
    return CHAR * len(IN)


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
    # OUT = ""
    # outNums = []
    if length != None:
        return ''.join(str(random.randint(0, 9)) for _ in range(length))
        # for _ in range(length):
        #     outNums.append(str(random.randint(MIN,MAX)))
    else:
        return ''.join(str(random.randint(0, 9)) for _ in range(len(IN)))
        # for i in str(IN):
        #     outNums.append(str(random.randint(MIN,MAX)))
    # return "".join(outNums)

# def address(IN: list) -> dict:
#     url = "https://my.api.mockaroo.com/address.json"

#     payload = {}
#     headers = {
#     'X-API-Key': secrets["mockaroo"]
#     }

#     response = requests.request("GET", url, headers=headers, data=payload)
#     data = response.text.split(",")
#     addressData = {}
#     addressData["streetNumber"] = data[0].strip()
#     addressData["streetName"] = data[1].strip()
#     addressData["streetSuffix"] = data[2].strip()
#     addressData["street"] = addressData["streetName"] + " " + addressData["streetSuffix"]
#     addressData["streetAddress"] = addressData["streetNumber"] + " " + addressData["street"]
#     addressData["city"] = data[3].strip()
#     addressData["country"] = data[4].strip()
#     addressData["postcode"] = data[5].strip()

#     OUT = {}
#     for i in IN:
#         OUT[i] = addressData[i]
#     return OUT
    

if __name__ == "__main__":
    
    arguably.run()

