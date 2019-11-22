import pandas as pd
import numpy as np
import pickle
from sqlalchemy import create_engine
from statsmodels.formula.api import glm
import psycopg2 
import itertools
import os

## Extract Titanic Data
def Extract_Data():
    data_uri = os.environ['DATABASE_URL']
    # data_uri = 'postgres://ujfkzspzjssvhx:ece2bb84158cf21405a686384ea4eff5344303580151c5180b40cbf9c78c7519@ec2-107-20-167-241.compute-1.amazonaws.com:5432/d9lhgv0dafi2fv'
    engine = create_engine(data_uri)
    dat = pd.read_sql('''SELECT sfid, heroku_id__c, survival_probability__c, survived__c,age__c,sex__c, pclass__c, fare__c
                         FROM salesforce.titanic_train_cleaned__c''', engine)
    engine.dispose()
    return dat
dat = Extract_Data()

def Update_Salesforce_Records(Prediction_Output):
    try:
        data_uri = os.environ['DATABASE_URL']
        #data_uri = 'postgres://vuvbqndrpsfoqn:64c1044b0c75aca863bf219187309088da53c1ee941f974e4fca2f09cf0e5381@ec2-174-129-222-15.compute-1.amazonaws.com:5432/dd3rjsf0sci85v'
        postgres_connection = psycopg2.connect(data_uri)
        cursor = postgres_connection.cursor()
        print('Connected to Database')
        
        ##Update data in Postgres with predictions
        
        SQL_Update_Query = """UPDATE salesforce.titanic_train_cleaned__c 
                              SET survival_probability__c = %s 
                              WHERE sfid = %s"""
        cursor.executemany(SQL_Update_Query,Prediction_Output)
        postgres_connection.commit()
        
        Updated_row_count = cursor.rowcount
        
        print(Updated_row_count,' rows updated')
        Status = "Success"
    except (Exception, psycopg2.Error) as error:
        print('Error while update operation: ', error)
        Status = "Failed"
    finally:
        ## Close Database connection
        if(postgres_connection):
            cursor.close()
            postgres_connection.close()
            print('Database Connection Closed')
            
    return Status

## Function for Predicting propensity
def Predict_Propensity(dataset):
    mod = pickle.load(open('iris2.sav','rb'))
    print('\nPredictions loaded successfully : ', mod)
           
    SF_Opportunity_Id = ['sfid']
    SF_Id = dataset[SF_Opportunity_Id]
    SF_Id = SF_Id.drop_duplicates().values.tolist()
    SF_Id = list(itertools.chain.from_iterable(SF_Id))
    dataset.drop('sfid',axis = 1,inplace = True)
         
     #Preprocess Records for predictions
    dat = Extract_Data()
    dat.info()
   
    ## Predict 
    Predicted_Output = mod.predict(dat).to_list()
    Predicted_Output = np.array([100]*len(Predicted_Output))*(np.array([1]*len(Predicted_Output)) - Predicted_Output)
    Prop_Predict_List = list(zip(Predicted_Output,SF_Id))
    
    return Prop_Predict_List

def Run_All():
   
    dat = Extract_Data()
    Count = dat.shape[0]
    
    if Count>0:
        print('\n Performing Predictions ')
        Predicted_output = Predict_Propensity(dat)
        Update_Salesforce_Records(Predicted_output)
        print('\n Prediction complete')
    else:
        print('\n Number of records less than 1, No Predictions/Updates Performed ')
        
Run_All()