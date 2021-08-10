import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods

class KMeansClustering:
    """
    this class is used to divide the data into the clusters before training the ML models
    """
    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj

    def elbow_plot(self, data):
        """
        this method will plot the wcss score along the number clusters
        and return the optimal number of the clusters
        """
        self.logger_obj.log(self.file_obj , "entered into the elbow_plot method of the KMeansClustering class")
        wcss = []
        try:
            for i in range(1, 11):
             kmeans = KMeans(n_clusters=i, init='k-means++', random_state= 42)
             kmeans.fit(data)
             wcss.append(kmeans.inertia_)

            plt.plot(range(1,11), wcss)
            plt.title('Elbow Plot')
            plt.xlabel('Number of clusters')
            plt.ylabel('wcss score')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')

            self.kn = KneeLocator(range(1,11), wcss, curve= 'convex', direction ='decreasing')
            self.logger_obj.log(self.file_obj, "optimum number of clusters is found:" + str(self.kn.knee) + "exited from the elbow_plot method of the KMeansClustering class")
            return self.kn.knee

        except Exception as e:
            self.logger_obj.log(self.file_obj, "Exception occured in elbow_plot method of the KMeansClustering class. Exception message: {}".format(e))
            self.logger_obj.log(self.file_obj, 'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise e

    def create_cluster(self,data , n_of_clusters):
        """
        this method creates a dataframe with cluster column and saves the model
        :param data:
        :param n_of_clusters:
        :return:
        """
        self.logger_obj.log(self.file_obj, "entered into the create_cluster method of the KMeansClustering class ")
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters = n_of_clusters, init='k-means++', random_state=42)
            self.cluster_prediction = self.kmeans.fit_predict(self.data)

            self.file_op = file_methods.File_Operation(self.file_obj, self.logger_obj)
            self.save_model = self.file_op.save_model(self.kmeans , 'KMeans')

            self.data['Cluster'] = self.cluster_prediction
            self.logger_obj.log(self.file_obj,'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class' )

            return self.data

        except Exception as e:
            self.logger_obj.log(self.file_obj,
                                   'Exception occured in create_clusters method of the KMeansClustering class. Exception message: {} '.format(e))
            self.logger_obj.log(self.file_obj,
                                   'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise e




