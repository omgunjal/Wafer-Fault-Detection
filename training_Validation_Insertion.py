from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        self.file_obj = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_obj, 'Start of Validation on files')

            self.log_writer.log(self.file_obj, "Raw Data Validation started")
            # Extracting the values from schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, n_columns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate the filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating the column length
            self.raw_data.validateColumnLength(n_columns)
            # validating if any column has missing values
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_obj, "Raw Data Validation Complete!!")


            self.log_writer.log(self.file_obj, "Starting Data Transforamtion !!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_obj,  "DataTransformation Completed!!!")


            self.log_writer.log(self.file_obj, "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Training', column_names)
            self.log_writer.log(self.file_obj, "Table creation Completed!!")

            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_obj, "Insertion in Table completed!!!")

            self.log_writer.log(self.file_obj, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_obj, "Good_Data folder deleted!!!")

            self.log_writer.log(self.file_obj, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_obj, "Bad files moved to archive!! Bad folder Deleted!!")

            self.log_writer.log(self.file_obj, "Validation Operation completed!!")

            self.log_writer.log(self.file_obj, "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Training')
            self.file_obj.close()

        except Exception as e:
            raise e
