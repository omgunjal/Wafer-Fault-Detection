"""
This is the Entry point for Training the Machine Learning Model.

Written By: iNeuron Intelligence
Version: 1.0
Revisions: None

"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger

# Creating the common Logging object

class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_obj = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def trainingModel(self):
        """
        this model is used to train the ML model
        """
        self.log_writer.log(self.file_obj, "Start of model training")
        try:
            """getting the data from the source"""
            data_getter = data_loader.Data_Getter(self.file_obj, self.log_writer)
            data = data_getter.get_data()

            """doing the data preprocessing"""
            preprocessor = preprocessing.Preprocessor(self.file_obj, self.log_writer)

            # remove the unnamed column as it doesn't contribute to the prediction
            data = preprocessor.remove_columns(data, ['Wafer'])

            # Create separate features and labels
            X, Y = preprocessor.separate_label_feature(data, label_column='Output')

            # check if missing values are present in the dataset
            is_null_present = preprocessor.is_null_present(X)

            # if missing values are there , replace them appropriately
            if is_null_present:
                X = preprocessor.impute_missing_values(X)

            # check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output both for good and bad sensors
            # prepare the list of such columns to drop
            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)

            # drop the columns obtained above
            X = preprocessor.remove_columns(X, cols_to_drop)

            """Applying the clustering approach"""
            kmeans = clustering.KMeansClustering(self.file_obj, self.log_writer)
            number_of_clusters = kmeans.elbow_plot(X)

            #divide data into clusters
            X = kmeans.create_cluster(X, number_of_clusters)

            # create a new col in the dataset consisting corresponding cluster assignment
            X['Labels'] = Y

            # getting unique clusters from the dataset
            list_of_clusters = X['Cluster'].unique()

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]   # filter the data for one cluster

                # prepare the feature and the label column
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train , x_test , y_train , y_test = train_test_split(cluster_features, cluster_label, test_size = 1/3 , random_state = 355 )

                # getting the best model for each of the clusters
                model_finder = tuner.Model_Finder(self.file_obj, self.log_writer)
                best_model_name , best_model = model_finder.get_best_model(x_train,y_train, x_test,y_test )

                # saving the best model for each of the clusters
                file_op = file_methods.File_Operation(self.file_obj, self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name +str(i))

            # logging the successful training
            self.log_writer.log(self.file_obj, 'Unsuccessful End of the Training')
            self.file_obj.close()

        except Exception as e:
            self.log_writer.log(self.file_obj, "Unsuccessful End of the Training")
            self.file_obj.close()
            raise e
