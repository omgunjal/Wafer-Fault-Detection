# importing the essentials

from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard
from predictFromModel import prediction
import json

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

# initializing the flask
app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route('/', methods = ['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route('/predict', methods =['POST'])
@cross_origin()
def predictRoutClient():
    try:
        if request.json is not None:
            path = request.json['filepath']

            pred_val = pred_validation(path)    # object initialization
            pred_val.prediction_validation()    # calling prediction validation method

            pred = prediction(path)     # obj initialization
            path, json_predictions = pred.predictionFromModel() # predicting for the dataset present in the database
            return Response("Prediction File created at:" + str(path) + 'and few of the predictions are: ' + str(json.loads(json_predictions)))

        elif request.form is not None:
            path = request.form['filepath']

            pred_val = pred_validation(path) # object initialization
            pred_val.prediction_validation() # calling the prediction validation method

            pred = prediction(path) # object initialization
            path, json_predictions = pred.predictionFromModel()

            return Response("Prediction File created at :" + str(path) + "and few of the predictions are:" + str(json.loads(json_predictions)))

        else :
            print('Nothing matched')

    except ValueError :
        return Response('Error Occurred: {}'.format(ValueError))

    except KeyError:
        return Response("Error occurred : {}".format(KeyError))

    except Exception as e:
        return Response("Error Occurred :{}".format(e))


@app.route('/train',methods = ['POST'])
@cross_origin()
def trainRoutClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']

            train_val_obj = train_validation(path) # object initialization
            train_val_obj.train_validation() # calling the training validation function

            train_Model_Obj = trainModel() # object initialization
            train_Model_Obj.trainingModel() # training the model for the files in the table
    except ValueError:
        return Response("Error occured :{}".format(ValueError))

    except KeyError:
        return Response("Error Occured: {}".format(KeyError))

    except Exception as e:
        return Response("Error Occured {}".format(e))

    return Response("Training successfull!")


# port something?
########################
if __name__ == "__main__":
    app.run(debug=True)










