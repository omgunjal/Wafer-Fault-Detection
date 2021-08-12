import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class Prediction_Data_validation:
    """
    this class is used for handling all the validations performed on the Raw prediction data
    """

    def __init__(self, path):
        self.logger = App_Logger()
        self.schema_path = 'schema_prediction.json'
        self.Batch_Directory = path


    def valuesFromSchema(self):
        """
        This method extracts all the info from the schema given i.e. "Schema" file
        Output = LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()

            pattern = dic["SampleFileName"]
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            msg = "LengthOfDateStampInFile::{} ,LengthOfTimeStampInFile:: {} ,NumberofColumns:: {}".format(LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns)
            self.logger.log(file , msg)

        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):
        """
        this method creates the regular expression to check
        if the filename is in the specific format
        """

        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):
        """
        This method will create the good_data and the bad data files at given path
        """
        try:
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory {}:".format(e))
            file.close()
            raise OSError


    def deleteExistingGoodDataTrainingFolder(self):
        """
        this method deletes the current good data folder
        """
        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Good_Raw'):
                shutil.rmtree(path + 'Good_Raw')

                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file , "Good Raw data file is deleted successfully")
                file.close()

        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error occurred while deleting the existing good data file ::{}".format(e))
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
         This method deletes the directory made to store the bad Data
        """

        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : {}".format(e))
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):
        """
        this function moves the bad data files to archieve folder
        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path = 'PredictionArchivedBadData'
            if not os.path.isdir(path):
                os.makedirs(path)

            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            destination = 'PredictionArchivedBadData/BadData_' + str(date) + '_' + str(time)

            if not os.path.isdir(destination):
                os.makedirs(destination)

            onlyfiles = os.listdir(source)
            for file in onlyfiles:
                if file not in os.listdir(destination):
                    shutil.move(source + file,destination )

            log_file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(log_file,"Bad files moved to archive" )

            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(log_file, "Bad Raw Data Folder Deleted successfully!!")
            log_file.close()

        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: {}".format(e))
            file.close()
            raise OSError


    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
        This function validates the name of the prediction csv file as per given name in the schema!
        Regex pattern is used to do the validation.If name format do not match the file is moved
        to Bad Raw Data folder else in Good raw data.
        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in os.listdir(self.Batch_Directory)]
        try:
            log_file = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if re.match(regex, filename):
                    splitAtDot = re.split(".csv", filename)
                    splitAtDot = re.split('_', splitAtDot[0])
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Good_Raw" )
                            self.logger.log(log_file, "Valid File name!! File moved to GoodRaw Folder :: {}".format(filename))

                        else:
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(log_file, "Invalid File Name!! File moved to Bad Raw Folder ::{}".format(filename))
                    else:
                        shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(log_file, "Invalid File Name!! File moved to Bad Raw Folder ::{}".format(filename))
                else:
                    shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(log_file, "Invalid File Name!! File moved to Bad Raw Folder ::{}".format(filename))

            log_file.close()
        except Exception as e:
            file = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file, "Error occured while validating FileName {}".format(e))
            file.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
        """
        this method is used to validate the column length in the csv file
        If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
        If the column number matches, file is kept in Good Raw Data for processing.
        The csv file is missing the first column name, this function changes the missing name to "Wafer".
        """
        try:
            log_file = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(log_file, "Column Length Validation Started!!")
            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(log_file,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: {}".format(file) )
            self.logger.log(log_file, " Column Length Validation completed")

        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file ::{}".format(OSError))
            f.close()
            raise OSError

        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        log_file.close()

    def deletePredictionFile(self):
        if os.path.isdir('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def validateMissingValuesInWholeColumn(self):
        """
        this method checks if in any column of the csv file contains all the missing values
        If such col is found, then we move that file into bad data file
        """
        try:
            log_file = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(log_file, "Missing value Validation has been started")

            for file in os.listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(log_file, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: {} ".format(file) )
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/"+file , index = None, header = True)

        except OSError:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: {}".format(OSError))
            f.close()
            raise OSError

        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: {}".format(e))
            f.close()
            raise e
        log_file.close()


















