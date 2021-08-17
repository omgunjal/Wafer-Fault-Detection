import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger

class dBOperation:
    """
    this class will handle all the SQL operations
    """
    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self, DatabaseName):
        """
        This method creates the database with given name ,and if the database already exist ,it returns the connection
        """
        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "opened {} database successfully!".format(DatabaseName))
            file.close()

        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error connecting to the database:: ".format(ConnectionError))
            file.close()
            raise ConnectionError
        return conn



    def createTableDb(self, DatabaseName , column_names):
        """
        This method will create a table in the database with given name and add column names in the Table
        """

        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type_ = column_names[key]

                try:
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {datatype}'.format(column_name = key, datatype = type_))
                except:
                    conn.execute('CREATE TABLE Good_Raw_Data ({col_name} {dtype})'.format(col_name = key, dtype = type_))
            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: {} ".format(e))
            file.close()
            # conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed {} database successfully".format(DatabaseName))
            file.close()
            raise e


    def insertIntoTableGoodData(self, Database):
        """
        this method inserts teh Good data from Good_Raw folder into the above created table
        """

        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        conn = self.dataBaseConnection(Database)
        onlyfiles = [f for f in listdir(self.goodFilePath)]

        for file in onlyfiles:
            try:
                with open(self.goodFilePath +'/'+ file, 'r') as f:
                    next(f)
                    reader = csv.reader(f, delimiter = "\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values = (list_)))
                                self.logger.log(log_file, "{} File loaded successfully".format(file))
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:
                conn.rollback()
                self.logger.log( log_file, "Error while creating the table :: {}".format(e))
                shutil.move(self.goodFilePath +'/'+ file, self.badFilePath)
                self.logger.log(log_file , "File moved successfully :: {}".format(file))
                log_file.close()
                conn.close()
                raise e

        conn.close()
        log_file.close()


    def selectingDatafromtableintocsv(self, Database):
        """
        this method exports the data in good_data folder in form of csv file at given location
        """
        self.fileFromDB = 'Prediction_FileFromDB/'
        self.filename = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')

        try:
            conn = self.dataBaseConnection(Database)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Good_Raw_Data")
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]

            if not os.path.isdir(self.fileFromDB):
                os.makedirs(self.fileFromDB)

            csv_file = csv.writer(open(self.fileFromDB + self.filename, 'w', newline=''), delimiter =",", lineterminator = "\r\n" , quoting = csv.QUOTE_ALL, escapechar = '\\')
            csv_file.writerow(headers)
            csv_file.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error :{}".format(e))
            raise e




