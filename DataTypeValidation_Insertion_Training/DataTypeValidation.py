import csv
import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
from application_logging.logger import App_Logger

class dBOperation:
    """
    This class handles all the sql operations
    """
    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_Files_validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self, DatabaseName):
        """
        this method is used to connect to the database with given name
        and if database already exists then it connects to that database
        """
        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')
            log_file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(log_file, 'Connected successfully to database ::{}'.format(DatabaseName))
            log_file.close()

        except ConnectionError:
            log_file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(log_file, "Error occured while connecting to the database:{}".format(DatabaseName))
            log_file.close()
            raise ConnectionError
        return conn

    def createTableDb(self, DatabaseName, column_names):
        """
        This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            c = conn.cursor()
            # It's the mediator between Python and SQLite database. We have to use this cursor object to execute SQL commands.
            # for simple understanding: https://www.tutorialspoint.com/sql-using-python-and-sqlite
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed {} database successfully".format(DatabaseName))
                file.close()

            else:
                for key in column_names.keys():
                    type_ = column_names[key]
                    # here dict:column_names & keys: actual column names & values: datatypes
                    try:
                        # here we assume that the table already exists and alter its schema
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key, dataType=type_))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type_))
                        # it try fails, means that table doesn't exist
                        # so we create the table

                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed {} database successfully".format(DatabaseName))
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: {} ".format(e))
            file.close()
            # conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed '{}' database successfully".format(DatabaseName))
            file.close()
            raise e



    def insertIntoTableGoodData(self,Database):
        """
        This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
        """
        conn = self.dataBaseConnection(Database)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        logfile = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath +'/'+file, 'r') as f:
                    next(f) # to skip the header i.e. the 1st row containing the column names
                    reader = csv.reader(f, delimiter = '\n' )
                    for line in enumerate(reader): # enumerate is used when we need to iterate as well as keep the count of the iterations
                        for list_ in (line[1]):  # here, line[1] is used
                                                # because enumerate :(no. of iteration,value) i.e. e.g 3rd iteration:(3,[col1val,col2val,col3val....])
                            try:
                                #?????????
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(logfile, " {}: File loaded successfully!!".format(file))
                                conn.commit() # saves all the changes in database
                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback() #This method rolls back any changes to the database since the last call to commit().
                self.logger.log(logfile,"Error while creating table: {}".format(e))
                shutil.move(goodFilePath+'/'+file, badFilePath)
                self.logger.log(logfile, "File Moved Successfully {}".format(e))
                logfile.close()
                conn.close()
        conn.close()
        logfile.close()



    def selectingDatafromtableintocsv(self,Database):
        """
        This method exports the data in GoodData table as a CSV file. in a given location.
                                            above created .
        """
        self.fileFromDb = "Training_FileFromDB/"
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')

        try:
            conn = self.dataBaseConnection(Database)
            sql_select_query = "SELECT * from Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sql_select_query)
            results = cursor.fetchall()

            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]    #??????????

            #making the csv output file
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # opening the csv file for writing
            f = open(self.fileFromDb + self.fileName, 'w', newline='')
            csv_file = csv.writer(f, delimiter=',', lineterminator = '\r\n',quoting=csv.QUOTE_ALL,escapechar='\\' )

            # adding the headers and the data inthe csv file
            csv_file.writerow(headers)
            csv_file.writerows(results)

            self.logger.log(log_file, "File exported successfully" )
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, 'File exporting failed. Error :{}'.format(e))
            log_file.close()



