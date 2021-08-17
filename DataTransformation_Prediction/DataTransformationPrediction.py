from datetime import datetime
from application_logging.logger import App_Logger
from os import listdir
import pandas as pd

class dataTransformPredict:
     """
     this class transforms the Good raw training data b4 loading it in the databse
     """

     def __init__(self):
          self.goodDataPath  = "Prediction_Raw_Files_Validated/Good_Raw"
          self.logger = App_Logger()

     def replaceMissingWithNUll(self):
          """
          This method is used to replace the missing values NULL to store them in the table
          and we substring the 1st col to keep it as a integer
          """

          try:
               log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
               onlyfiles = [f for f in listdir(self.goodDataPath)]

               for file in onlyfiles:
                    csv = pd.read_csv(self.goodDataPath + '/' + file)
                    csv.fillna('NULL', inplace=True)

                    csv['Wafer'] = csv['Wafer'].str[6:]
                    csv.to_csv(self.goodDataPath + '/' + file, index = None, header = True)
                    self.logger.log(log_file,  "{}: File Transformed successfully!!".format(file) )

          except Exception as e:
               self.logger.log(log_file,  "Data Transformation failed because:: {}".format(e))
               log_file.close()
               raise e

          log_file.close()
