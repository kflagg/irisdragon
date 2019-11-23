import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2 
import itertools
import os

def Extract_Data():
    data_uri = os.environ['DATABASE_URL']
    engine = create_engine(data_uri)
    dat = pd.read_sql('''SELECT Name, Petal_Length__c, Petal_Width__c, Sepal_Length__c, Sepal_Width__c, Species__c, petal2__c
                         FROM salesforce.Iris__c''', engine)
    engine.dispose()
    return dat

def Update_Salesforce_Records(Prediction_Output):
    try:
        data_uri = os.environ['DATABASE_URL']
        postgres_connection = psycopg2.connect(data_uri)
        cursor = postgres_connection.cursor()
        print('Connected to Database')

        SQL_Update_Query = """UPDATE salesforce.Iris__c 
                              SET petal2__c = %s 
                              WHERE Name = %s"""
        cursor.executemany(SQL_Update_Query, Prediction_Output)
        postgres_connection.commit()

        Updated_row_count = cursor.rowcount

        print(Updated_row_count,' rows updated')
        Status = "Success"
    except(Exception, psycopg2.Error) as error:
        print('Error in update operation: ', error)
        Status = "Failed"
    finally:
        ## Close Database connection
        if(postgres_connection):
            cursor.close()
            postgres_connection.close()
            print('Database Connection Closed')

    return Status

## Function for acquiring updates.
def Predict_petal2(dataset):
    SF_Id = dataset['name']
    SF_Id = SF_Id.values.tolist()

    ## Predict 
    Predicted_Output = (2*dataset['petal_length__c']).to_list()
    Predict_List = list(zip(Predicted_Output, SF_Id))

    return Predict_List

def Run_All():

    dat = Extract_Data()
    Count = dat.shape[0]

    if Count>0:
        print('\nPerforming Predictions')
        Predicted_output = Predict_petal2(dat)
        Update_Salesforce_Records(Predicted_output)
        print('\nPrediction complete')
    else:
        print('\nNumber of records less than 1, No Predictions/Updates Performed')

Run_All()
