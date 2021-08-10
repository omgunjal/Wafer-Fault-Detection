import pandas as pd

class Data_Getter:
    """
    this class is to get the data from source file for training
    """

    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.training_file = 'Training_FileFromDB/InputFile.csv'

    def get_data(self):
        """
        this function collects the data from source file for training
        :return: data in pandas dataframe
        """
        self.logger_obj.log(self.file_obj, 'entered into the get_data method of Data_Getter classs')
        try:
            self.data  = pd.read_csv(self.training_file)
            self.logger_obj.log(self.file_obj, 'Data loaded successfully, Exited the get_data method of the Data_Getter class')
            return self.data

        except Exception as e :
            self.logger_obj.log(self.file_obj, 'Dta load failed !!, Exited from the get_data method of the Data_Getter Class. EXception:{}'.format(e))
            raise e
            #########