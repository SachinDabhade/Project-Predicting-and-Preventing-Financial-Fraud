from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, corr
import pickle
import numpy as np
import pandas as pd
from config import insurance_model, credit_card_model, transaction_model
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FinancialFraudApp").getOrCreate()

class Finanical_Fraud():

    def __init__(self, model, new_data):
        self.types = {'Transaction Fraud': transaction_model, 'Credit Card Fraud': credit_card_model, 'Insurance Fraud': insurance_model}
        self.model = model
        self.data = new_data

    def load_model(self):
        with open(self.types[self.model], 'rb') as file:
            self.loaded_model = pickle.load(file)

    def predict(self):
        self.predictions = self.loaded_model.predict(self.data)
        return self.predictions

    def main(self, data):
        self.load_model()
        return self.predict(data)

class Dashboard:

    def __init__(self, data):
        self.data = spark.read.csv(data, header=True, inferSchema=True)

    def raw_information(self):

        # Columns 
        columns = len(self.data.columns)
        print('columns\n', columns)

        # Total rows
        rows = self.data.count()
        print('Rows\n', rows)

        # NULL count
        null_counts = {col_name: self.data.filter(col(col_name).isNull()).count() for col_name in self.data.columns}
        print('Null Counts\n', null_counts)
    
        # NULL percentage
        null_percentages = {col_name: (count / rows) * 100 for col_name, count in null_counts.items()}
        print('Null PCT\n', null_percentages)

        # Count of Unique values
        unique_counts = {col_name: self.data.select(countDistinct(col(col_name))).collect()[0][0] for col_name in self.data.columns}
        print('Unique Counts\n', unique_counts)

        # Head
        head = self.data.limit(5)
        print('Head\n', head)

        # DataTypes
        dtypes = {field.name: str(field.dataType) for field in self.data.schema.fields}
        print('Data Type\n', dtypes)

        # Descriptive statistics
        descriptive_statistics = self.data.describe().toPandas()
        print('Discriptive Stats\n', descriptive_statistics)
                      

        # return columns, rows, null_counts, null_percentages, unique_values, unique_counts, head, dtypes, descriptive_statistics


from pprint import pprint
Sachin = Dashboard('data\insurance_claims.csv')
Sachin.raw_information()



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import pickle
import numpy as np
import pandas as pd
# import pyspark
from config import insurance_model, credit_card_model, transaction_model
import datetime

class Finanical_Fraud():

    def __init__(self, model):
        self.types = {'Transaction Fraud': transaction_model, 'Credit Card Fraud': credit_card_model, 'Insurance Fraud': insurance_model}
        self.model = model

    def load_model(self):
        with open(self.types[self.model], 'rb') as file:
            self.loaded_model = pickle.load(file)

    def predict(self, new_data):
        self.predictions = self.loaded_model.predict(new_data)
        return self.predictions

    def main(self, data):
        self.load_model()
        return self.predict(data)
    
class Dashboard:

    def __init__(self, data):
        self.data = pd.read_csv(data)

    def raw_information(self):
        CNT = self.data.count().sum()
        NULL = self.data.isnull().sum().sum()
        N_PCT = (NULL / CNT) * 100
        HEAD = self.data.head()
        CLM = self.data.columns
        DT = self.data.dtypes
        DCB = self.data.describe
        return CNT, NULL, N_PCT, HEAD, CLM, DT, DCB

def logrecord(text):
    now = datetime.datetime.now()
    with open('./static/dashboard/logs.txt', 'a') as file:
        file.write(f'{now} -> {text}\n')

def recent_activities():
    with open('./static/dashboard/logs.txt', 'r') as file:
        sentences = file.readlines()[::-1]
    recent_activities = []
    for i in range(0, min(len(sentences), 7)):
        data = sentences[i].split('->')
        do = datetime.datetime.now() - datetime.datetime.strptime(data[0][0:16], "%Y-%m-%d %H:%M")
        hours = do.total_seconds() / 3600
        sentence = data[1]
        recent_activities.append([round(hours, 1), sentence])
    return recent_activities

def main_FF(model):
    F = Finanical_Fraud(model=model)
    return F.main()

def main_D(data):
    D = Dashboard(data=data)
    return D.raw_information()

# def read_csv(data):
#     df = pd.read_csv(data)
#     return df