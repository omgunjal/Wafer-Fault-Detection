import pickle
import os
import shutil

class File_Operation:

    def __init__(self, file_obj, logger_obj):
        self.file_obj = file_obj
        self.logger_obj = logger_obj
        self.model_dir = 'models/'

    def save_model(self, model, filename):
        self.logger_obj.log(self.file_obj, 'Entered into save model method of File_Operation Class')
        try:
            # create seperate directory for each cluster
            path = os.path.join(self.model_dir, filename)
            # if condition to remove previously existing models for each clusters
            if os.path.isdir(path):   #used to check whether the specified path is an existing directory or not.
                shutil.rmtree(self.model_dir) # used to delete an entire directory tree, path must point to a directory (but not a symbolic link to a directory).
                os.makedirs(path) #used to create a directory recursively.
            # if no previously created models , directly make the directory recursively
            else:
                os.makedirs(path)

            with open(path +'/' + filename+'.sav','wb') as f:
                # The SAV file extension is more commonly known as a generic extension that is used to save files and data
                pickle.dump(model,f)    # save the model to file

            self.logger_obj.log(self.file_obj, 'model file'+ filename + 'is saved'+ ' Exited save_model method of the File_Operation Class')

            return 'success'

        except Exception as e:
            self.logger_obj.log(self.file_obj, 'Exception occured in save model method of File_Operation class:{}'.format(e))
            self.logger_obj.log(self.file_obj, 'Model file'+ filename + 'could not be saved. Exited from save_model function of File operation class')
            raise Exception()

    def load_model(self, filename):
        self.logger_obj.log(self.file_obj, 'Entered into the load_model method of the File Operations Class')
        try:
            with open(self.model_dir + filename +'/'+ filename + '.sav', 'rb') as f:
                self.logger_obj.log(self.file_obj, 'Model file '+filename + 'Loaded. Exited from load_model function of File_operation class')
                return pickle.load(f)

        except Exception as e:
            self.logger_obj.log(self.file_obj, 'Exception occured in load_model function of the File_Operation class::{}'.format(e))
            self.logger_obj.log(self.file_obj, 'Model_name'+ filename + 'could not be saved. Exited from load_model function of File operation class')
            raise Exception

    def find_correct_model_file(self, cluster_number):
        self.logger_obj.log(self. file_obj, ' Entered into the find_correct_model_file function')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_dir
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name) #This method is used to retrieve the list of files and directories present in the specified directory.
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number))!=-1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_obj.log(self.file_obj,'Exited the find_correct_model of File_operations class')
            return self.model_name
        except Exception as e:
            self.logger_obj.log(self.file_obj,'Exception occured in find correct model file method of the File operations class')
            self.logger_obj.log(self.file_obj,'Exited the find correct method of File_Operations class')
            raise Exception()

