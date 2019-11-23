import pandas as pd
import numpy as np
import pickle
from sqlalchemy import create_engine
import os

def Extract_Data():
    data_uri = os.environ['DATABASE_URL']
    engine = create_engine(data_uri)
    dat = pd.read_sql('''SELECT Name, Petal_Length__c, Petal_Width__c, Sepal_Length__c, Sepal_Width__c, Species__c, petal2__c
                         FROM salesforce.Iris__c''', engine)
    engine.dispose()
    return dat

dat = Extract_Data()

dat['petal2__c'] = dat['petal_length__c'] * 2

pickle.dump(dat, open('iris2.sav', 'wb'))
