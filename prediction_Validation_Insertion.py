from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging.logger import App_Logger

class pred_validation:

    def __init__(self, path ):
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform = dataTransformPredict()
        self.dBOperation = dBOperation()
        self.file_obj = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = App_Logger()

    def prediction_validation(self):

        try:

            self.log_writer.log(self.file_obj, 'Start of Validation on files for prediction!!')
            # extracting the values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()

            # getting the regex to validate the filename
            self.log_writer.log(self.file_obj, 'Start of manualRegexCreation method!!')
            regex = self.raw_data.manualRegexCreation()

            # validating filename of the prediction files
            self.log_writer.log(self.file_obj, 'Start of validationFileNameRaw method!!')
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile )

            # validating the column length in the file
            self.log_writer.log(self.file_obj, 'Start of validateColumnLength method!!')
            self.raw_data.validateColumnLength(noofcolumns)

            # validating if any column has missing values
            self.log_writer.log(self.file_obj, 'Start of validateMissingValuesInWholeColumn method!!')
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_obj, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_obj, "Starting Data Transformations!!")
            # replacing the blanks in the csv file with 'NULL' values to insert in the table
            self.log_writer.log(self.file_obj, 'Start of replaceMissingWithNUll method!!')
            self.dataTransform.replaceMissingWithNUll()
            self.log_writer.log(self.file_obj, "DataTransformation Completed!!!")

            self.log_writer.log(self.file_obj, "Creating Prediction_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Prediction', column_names)
            self.log_writer.log(self.file_obj, "Table creation Completed!!")

            # insert csv files in the table
            self.log_writer.log(self.file_obj, "Insertion of Data into Table started!!!!")
            self.dBOperation.insertIntoTableGoodData('Prediction')
            self.log_writer.log(self.file_obj, "Insertion in Table completed!!!")

            # Delete the good data folder after loading files in table
            self.log_writer.log(self.file_obj, "Deleting Good Data Folder!!!")
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_obj, "Good_Data folder deleted!!!")

            # Move the bad files to archive folder
            self.log_writer.log(self.file_obj, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_obj, "Bad files moved to archive!! Bad folder Deleted!!")

            self.log_writer.log(self.file_obj, "Validation Operation completed!!")

            # export data in table to csvfile
            self.log_writer.log(self.file_obj, "Extracting csv file from table")
            self.dBOperation.selectingDatafromtableintocsv('Prediction')

        except Exception as e:
            raise e














