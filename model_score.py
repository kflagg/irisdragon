import pandas as pd
import numpy as np
import pickle
from sqlalchemy import create_engine
import os

def Extract_Data():
    #data_uri = 'postgres://vuvbqndrpsfoqn:64c1044b0c75aca863bf219187309088da53c1ee941f974e4fca2f09cf0e5381@ec2-174-129-222-15.compute-1.amazonaws.com:5432/dd3rjsf0sci85v'
    data_uri = os.environ['DATABASE_URL']
    engine = create_engine(data_uri)
    dat = pd.read_sql('''SELECT Name, Petal_Length__c, Petal_Width__c, Sepal_Length__c, Sepal_Width__c, Species__c, petal2__c
                         FROM salesforce.Iris__c''', engine)
    engine.dispose()
    return dat

dat = Extract_Data()

dat['petal2__c'] = dat['petal_length__c'] * 2

pickle.dump(dat, open('iris2.sav', 'wb'))
