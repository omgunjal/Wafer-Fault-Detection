import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class Preprocessor:
    """
    This will clean and pre-process the data before the training
    """

    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj

    def remove_columns(self, data, columns):
        """
        this method is to remove the given columns from the data
        :return: data after removing given columns
        """
        self.data = data
        self. columns = columns
        self.logger_obj.log(self.file_obj, 'entered into the remove_columns function of preprocessor class ')
        try:
            self.useful_data = self.data.drop(self.columns , axis = 1 ) # dropping the columns given
            self.logger_obj.log(self.file_obj, 'columns are removed successfully!! Exited from the remove_column method of the preprocessor class')
            return self.data

        except Exception as e:
            self.logger_obj.log(self.file_obj, "removal of colums unsuccessful !! Exited from remove_columns method of the preprocessor class. Exxception::{}".format(e))
            raise e
            ##############



    def separate_label_feature(self, data , label_column):
        """
        this method is used to separate the features and the labels i.e. X and y(to be predicted)
        :return:
        """
        self.logger_obj.log(self.file_obj, "enterd into the separate_label_feature method of the preprocessor class")
        try:
            self.X = data.drop(label_column, axis =1) # taking whole data except label_column
            self.Y = data[label_column] # picking up the label column only
            self.logger_obj.log(self.file_obj, "seperated features and labels successfully!! in separate_label_feature method of the preprocessor class")
            return self.X , self.Y

        except Exception as e:
            self.logger_obj.log(self.file_obj,
                                'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:{}'.format(e))
            self.logger_obj.log(self.file_obj,'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise e


    def is_null_present(self, data):
        """
        this method checks if there are any null values in data and sends back its report csv file in log
        :param data:
        :return:
        """

        self.logger_obj.log(self.file_obj, 'Entered into the is_null_present method of the preprocessor class')
        self.null_present = False
        try:
            self.null_counts = data.isnull().sum()  # to get no. of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present = True
                    break

            if self.null_present:
                df_with_null = pd.DataFrame()
                df_with_null['columns'] = data.columns
                df_with_null['null_value_count'] = np.asarray(data.isnull().sum())
                df_with_null.to_csv("preprocessing_data/null_values.csv")

            self.logger_obj.log(self.file_obj,
                               'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present

        except Exception as e:
            self.logger_obj.log(self.file_obj,
                                   'Exception occured in is_null_present method of the Preprocessor class. Exception message:{}'.format(e))
            self.logger_obj.log(self.file_obj,
                                   'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise e


    def impute_missing_values(self, data):
        """
        This methods imputes all the missing values in data unsing KNNImputer
        :param data:
        :return:
        """
        self.logger_obj.log(self.file_obj, "Enterd into the impute_missing_values method of the preprocessor class")
        self.data = data

        try:
            imputer = KNNImputer(missing_values=np.nan, n_neighbors= 3, weights="uniform")
            self.new_data_array = imputer.fit_transform(self.data)
            self.new_data = pd.DataFrame(data= self.new_data_array, columns=self.data.columns)
            self.logger_obj.log(self.file_obj, "Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class")
            return self.new_data

        except Exception as e:
            self.logger_obj.log(self.file_obj, "Exception occured in impute_missing_values method of the Preprocessor class. Exception message:{}".format(e))
            self.logger_obj.log(self.file_obj, "Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class'")
            raise e

    def get_columns_with_zero_std_deviation(self, data):
        """
        this method is used to find out the columns with zero std deviation
        :param data:
        :return:
        """

        self.logger_obj.log(self.file_obj, "Entered into the get_columns_with_zero_std_deviation function of the preprocessing class " )
        self.cols_to_drop = []
        self.col_describe = data.describe()
        try:

            for col in data.columns:
                if self.col_describe[col]['std'] == 0 :
                    self.cols_to_drop.append(col)

            self.logger_obj.log(self.file_obj , "Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class")
            return self. cols_to_drop

        except Exception as e:
            self.logger_obj.log(self.file_obj, "Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message: {}".format(e))
            self.logger_obj.log(self.file_obj, "Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class")
            raise e




