from IrisDataMasker import logger
import mysql.connector
from getpass import getpass

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