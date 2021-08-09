from datetime import datetime
from os import listdir
import pandas as pd
from application_logging.logger import App_Logger

class dataTransform:
     """
     This class is used to transform good raw training data before adding to database
     """
     def __init__(self):
          self.logger = App_Logger()
          self.good_data_path = "Training_Raw_files_validated/Good_Raw"

     def replaceMissingWithNull(self):
          """
          this method is used to replace the missing values with Null to store in table
          and
          we are substringing the 1st col and keep "integer" only
          This column is anyways removed during the training
          """
          log_file = open("Training_logs/dataTransformLog.txt", "a+")
          try:
               onlyfiles = [f for f in listdir(self.good_data_path)]
               for file in onlyfiles:
                    csv_file = pd.read_csv(self.good_data_path + '/' + file)
                    csv_file.fillna('NULL', inplace = True)
                    csv_file['Wafer'] = csv_file['Wafer'].str[6:]
                    csv_file.to_csv(self.good_data_path + '/' + file, index = None, header = True)
                    self.logger.log(log_file, 'Missing values replaced with Null and file transformed successfully::{}'.format(file))

          except Exception as e:
               self.logger.log(log_file, 'Data Transformation failed because of error:{}'.format(e))
               log_file.close()
               ############
               # raise e
          log_file.close()
