from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

class Model_Finder:
    """
    This class is intended to find out the model with best accuracyAUC score.
    also it tunes the hyperparameters
    """

    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.clf = RandomForestClassifier()
        self.xgb = XGBClassifier(objective="binary:logistic")

    def get_best_params_for_random_forest(self, x_train, y_train):
        """
        gives us the best parameters for random forest classifier by performing hyperparameter tuning
        """
        self.logger_obj.log(self.file_obj, "Entered the 'get_best_params_for_random_forest' method of the Model_finder class")
        try:
            self.param_grid = {"n_estimators":[10, 50, 100, 130],
                               "criterion":['gini','entropy'],
                               "max_depth":[2, 3],
                               "max_features":['sqrt','log2']}
            self.grid = GridSearchCV(estimator= self.clf, param_grid = self.param_grid, cv=5, verbose=3)
            self.grid.fit(x_train, y_train)

            # collect the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']

            # creating the new model on best parameters
            self.clf = RandomForestClassifier(n_estimators=self.n_estimators, criterion=self.criterion, max_depth= self.max_depth, max_features=self.max_features)
            self.clf.fit(x_train, y_train)

            self.logger_obj.log(self.file_obj, "Best parameters for random forest are:"+ str(self.grid.best_params_) + "Exited from the get_best_params_for_random_forest method of the Model_Finder class")
            return  self.clf

        except Exception as e:
            self.logger_obj.log(self.file_obj, "Exception occured during the process of getting the best parameters for the random forest classifier. error::{}".format(e))
            self.logger_obj.log(self.file_obj, "Exited from the get_best_params_for_random_forest method of the Model_Finder class" )
            raise e



    def get_best_params_for_xgboost(self, x_train, y_train):
        """
        This method finds the best parameters for the xgboost model by performing hyperparameter tuning
        """
        self.logger_obj.log(self.file_obj, "entered into get_best_params_for_xgboost method of the model-finder class")
        try:
            self.param_grid_xgb = { 'learning_rate': [0.5, 0.1, 0.01, 0.001],
                                    'max_depth': [3, 5, 10, 20],
                                    'n_estimators': [10, 50, 100, 200]}
            self.grid = GridSearchCV(estimator = self.xgb, param_grid= self.param_grid_xgb, cv = 5, verbose = 3)
            self.grid.fit(x_train, y_train)

            # collecting the best parameters
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # creating the model with best parameters
            self.xgb = XGBClassifier(max_depth=self.max_depth, learning_rate = self.learning_rate, n_estimators=self.n_estimators)
            self.xgb.fit(x_train, y_train)

            self.logger_obj.log(self.file_obj, "The best parameters for the xgboost model are :"+str(self.grid.best_params_)+ ".Exited from the get_best_params_for_xgboost method of the model_finder class")
            return self.xgb

        except Exception as e:
            self.logger_obj.log(self.file_obj,'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message: {}'.format(e))
            self.logger_obj.log(self.file_obj,'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise e

    def get_best_model(self, x_train, y_train, x_test, y_test):
        """
        this method finds out the best model which has best AUC score
        """
        self.logger_obj.log(self.file_obj, "entered the get_best_model method of the model_finder class")
        try:
            # get te best model of xgboost
            self.xgboost = self.get_best_params_for_xgboost(x_train, y_train)
            self.prediction_xgboost = self.xgboost.predict(x_test)

            if len(y_test.unique()) == 1:
                # if there is only one label present in y_test then AUC score gives error
                self.xgboost_score = accuracy_score(y_test, self.prediction_xgboost)
                self.logger_obj.log(self.file_obj, "the accuracy score of the xgboost model is :{}".format(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(y_test, self.prediction_xgboost)
                self.logger_obj.log(self.file_obj, "the AUC score of the xgboost model is :{}".format(self.xgboost_score))

            # get the best model for random forest
            self.random_forest = self.get_best_params_for_random_forest(x_train, y_train)
            self.prediction_random_forest = self.random_forest.predict(x_test)

            if len(y_test.unique())==1:
                # here the roc_aoc score will give error since there is only one label present in y_test
                self.random_forest_score = accuracy_score(y_test, self.prediction_random_forest)
                self.logger_obj.log(self.file_obj, "the accuracy score for random forest model is:{}".format(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(y_test, self.prediction_random_forest)
                self.logger_obj.log(self.file_obj, "the AUC score for random forest model is:{}".format(self.random_forest_score))

            # comparing the two models
            if (self.random_forest_score < self.xgboost_score):
                return 'XGBoost', self.xgboost
            else:
                return 'RandomForest', self.random_forest

        except Exception as e:
            self.logger_obj.log(self.file_obj,'Exception occured in get_best_model method of the Model_Finder class. Exception message:{}'.format(e))
            self.logger_obj.log(self.file_obj,'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise e




