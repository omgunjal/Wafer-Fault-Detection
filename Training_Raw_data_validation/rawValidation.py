import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

class Raw_Data_validation:

    """
    This class is used for handling all the validation done on the Raw Training Data.
    """
    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """
            Method Name: valuesFromSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
            column_names = dic['ColName']
            NumberofColumns = dic["NumberofColumns"]

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt",'a+')
            msg = "LengthOfDateStampInFile:: {}   LengthOfTimeStampInFile:: {}  Numberofcolumns::{} \n".format(LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberofColumns)
            self.logger.log(file, msg)

            file.close()
        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error,incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile,LengthOfTimeStampInFile, column_names,NumberofColumns


    def manualRegexCreation(self):
        """
            Method Name: manualRegexCreation
            Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                        This Regex is used to validate the filename of the training data.
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
             Method Name: createDirectoryForGoodBadRawData
            Description: This method creates directories to store the Good Data and Bad Data
                        after validating the training data.

        """
        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory {}:".format(ex))
            file.close()
            raise OSError



    def deleteExistingGoodDataTrainingFolder(self):

        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data
                          after loading the data in the table. Once the good files are
                          loaded in the DB,deleting the directory ensures space optimization.
            Output: None
            On Failure: OSError
        """
        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw directory deleted successfully!")
                file.close()

        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, 'Error while Deleting Directory : {}'.format(e))
            file.close()
            raise OSError


    def deleteExistingBadDataTrainingFolder(self):

        """
            Method Name: deleteExistingBadDataTrainingFolder
            Description: This method deletes the directory made to store the bad Data.
            Output: None
            On Failure: OSError

        """
        try:
            path = "Training_Raw_files_validated/"
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, 'BadRaw directory is deleted successfully  before starting validation')
                file.close()

        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, 'Error occured while deleting the directory:{}'.format(e))
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
            Method Name: moveBadFilesToArchiveBad
            Description: This method deletes the directory made  to store the Bad Data
                          after moving the data in an archive folder. We archive the bad
                          files to send them back to the client for invalid data issue.
            Output: None
            On Failure: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                destination = 'TrainingArchiveBadData/BadData_' + str(date) + "_" + str(time)
                if not os.path.isdir(destination):
                    os.makedirs(destination)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(destination):
                        shutil.move(source + f, destination)

                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file=open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:{}".format(e))
            file.close()
            raise e


    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the training csv files as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception
        """
        # pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new good and bad data directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in os.listdir(self.Batch_Directory)]
        try:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    splitAtDot = re.split('.csv',filename)
                    splitAtDot = (re.split('_',splitAtDot[0]))
                    if len(splitAtDot[1])==LengthOfDateStampInFile:
                        if len(splitAtDot[2])==LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/"+filename, "Training_Raw_Files_validated/Good_Raw" )
                            self.logger.log(file, 'filename is valid! File moved to Good Raw data folder:{}'.format(filename))

                        else:
                            shutil.copy("Training_Batch_Files/"+filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(file,"Invalid File Name!! <len timestamp> File moved to Bad Raw Folder :{}".format(filename))

                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(file, "Invalid File Name!! <len datestamp> File moved to Bad Raw Folder :{}".format(filename))
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(file, "Invalid File Name!! <regex> File moved to Bad Raw Folder :{}".format(filename))
            file.close()

        except Exception as e:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file,"Error occured while validating FileName {}".format(e))
            file.close()
            raise e


    def validateColumnLength(self,NumberofColumns):
        """
              Method Name: validateColumnLength
              Description: This function validates the number of columns in the csv files.
                           It is should be same as given in the schema file.
                           If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                           If the column number matches, file is kept in Good Raw Data for processing.
                          The csv file is missing the first column name, this function changes the missing name to "Wafer".
              Output: None
              On Failure: Exception
        """
        try:
            file = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(file,"Column Length Validation has Started" )
            for f in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv_file = pd.read_csv('Training_Raw_files_validated/Good_Raw/' + f)
                if csv_file.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + f, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(file, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: {}".format(f))
                self.logger.log(file, 'Column length valiidation completed')
                ############
            file.close()
                #############
        except OSError:
            file = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(file,"Error Occured while moving the file :: {}".format(OSError))
            file.close()
            raise OSError

        except Exception as e:
            file =  open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(file, "Error occured in no. of column validation ::{}".format(e))
            file.close()
            raise e

    def validateMissingValuesInWholeColumn(self):
        """
              Method Name: validateMissingValuesInWholeColumn
              Description: This function validates if any column in the csv file has all values missing.
                           If all the values are missing, the file is not suitable for processing.
                           SUch files are moved to bad raw data.
              Output: None
              On Failure: Exception
        """
        try:
            file = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(file,"Missing Values Validation Started!!")
            for f in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv_file = pd.read_csv("Training_Raw_files_validated/Good_Raw/"+f)
                count = 0
                for columns in csv_file:
                    if ( len(csv_file[columns]) - csv_file[columns].count() ) == len(csv_file[columns]):
                        count +=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + f,"Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(file,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: {}".format(f) )
                        break
                if count == 0:
                    csv_file.rename(columns={"Unnamed: 0":"Wafer"}, inplace = True)
                    csv_file.to_csv("Training_Raw_files_validated/Good_Raw/"+f , index = None, header = True)
            #######
            file.close()
            ########
        except OSError:
            file = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(file, "Error Occured while moving the file :: {}".format(OSError))
            file.close()
            raise OSError

        except Exception as e:
            file = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(file ,"Error Occured :: {}".format(e) )
            file.close()
            raise e














