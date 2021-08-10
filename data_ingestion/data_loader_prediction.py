import pandas as pd

class Data_Getter_Pred:
    """
    This class is used to get the data from the source csv file for prediction
    """

    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.prediction_file = 'Prediction_FileFromDB/InputFile.csv'

    def get_data(self):
        """
        this method is used to get the data from source csv file
        :return: pandas dataframe containing the data in the source csv file
        """

        self.logger_obj.log(self.file_obj, "Entered into the get_data method of the Data_Getter_Pred class")
        try:
            self.data = pd.read_csv(self.prediction_file)

            self.logger_obj.log(self.file_obj, "The data is loaded successfully!! and Exited from the get_data method of the Data_Getter_pred class")

            return self.data

        except Exception as e:
            self.logger_obj.log(self.file_obj, "Error occued while loading the data in get_Data method of Data_Getter_Pred class::{}".format(e))
            self.logger_obj.log(self.file_obj, "Data load Unsuccessful !, Exited from the get_data method of the Data_Gette_Pred class")
            raise e




