import pickle
import pandas as pd
from config import insurance_model, credit_card_model, transaction_model
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import xgboost as xgb

class Dashboard:

    # Initialize Spark session
    def __init__(self, data):
        self.spark = SparkSession.builder.appName("FinancialFraudApp").getOrCreate()
        self.data = self.spark.read.csv(data, header=True, inferSchema=True)
        self.data_csv = pd.read_csv(data)

    def raw_information(self):

        # Columns 
        columns = len(self.data.columns)
        # columns = len(self.data_csv.columns)
        print('columns\n', columns)

        # Total rows
        self.rows = self.data.count()
        # rows = self.data_csv.count()
        print('Rows\n', self.rows)

        # Total Records
        total = self.rows * columns
        print('Total Records: \n', total)

        # NULL count
        # null_counts = {col_name: self.data.filter(col(col_name).isNull()).count() for col_name in self.data.columns}.values().sum()
        null_counts = self.data_csv.isnull().sum()
        print('Null Counts\n', null_counts)
    
        # Count of Unique values
        # unique_counts = {col_name: self.data.select(countDistinct(col(col_name))).collect()[0][0] for col_name in self.data.columns}
        unique_counts = self.data_csv.nunique()
        print('Unique Counts\n', unique_counts)
        # unique_counts = self.data.select(col(col_name)).distinct().count()
        # print('Unique Counts\n', unique_counts)

        # DataTypes
        # dtypes = {field.name: str(field.dataType) for field in self.data.schema.fields}
        dtypes = self.data_csv.dtypes
        print('Data Type\n', dtypes)
                      
        return (total, self.rows, columns, null_counts[:10], unique_counts[:10], dtypes[:10])
    
    def stop_spark(self):
        self.spark.stop()


class Finanical_Fraud(Dashboard):

    def __init__(self, model, new_data):
        super().__init__(new_data)
        self.data_csv = pd.read_csv(new_data)
        self.types = {'transaction_file': transaction_model, 'Credit_card_file': credit_card_model, 'Insurance_file': insurance_model}
        self.model = model
        # record('Initialize Spark Succesfully')

    def load_model(self):
        # print(self.model)
        # print(self.data_csv)
        with open(self.types[self.model], 'rb') as file:
            self.loaded_model = pickle.load(file)
            # record("Model Loaded Succesfully")

    def predict(self):
        # print('TYPE\n', type(self.data_csv))
        self.predictions = self.loaded_model.predict(self.data_csv)
        print('Predictions\n', self.predictions)
        # record('Model Prediction Succesfully')
    
    def data_preprocessing(self):
        if self.model == 'Credit_card_file':
            target = 'Class'
            predictors = ['Time', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10',\
                'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19',\
                'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28',\
                'Amount']
            self.data = xgb.DMatrix(self.data_csv[predictors], self.data_csv[target].values)

    def about_results(self):
        unique = pd.Series(self.predictions).value_counts()
        Valid = unique[0]/self.rows
        Fraud = unique[1]/self.rows
        print('Valid and Fraud Transactions Details')
        print(unique[0], Valid, unique[1], Fraud)
        return (unique[0], Valid, unique[1], Fraud)

    

    def main_f(self):
        raw = self.raw_information()
        self.stop_spark()
        self.load_model()
        self.data_preprocessing()
        self.predict()
        result = self.about_results()
        return raw, result

def logrecord(text):
    now = datetime.datetime.now()
    with open('./static/dashboard/logs.txt', 'a') as file:
        file.write(f'{now} -> {text}\n')

def record(text):
    now = datetime.datetime.now()
    with open('./static/dashboard/records.txt', 'a') as file:
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

def main_FF(model, data):
    F = Finanical_Fraud(model=model, new_data=data)
    return F.main_f()

# def main_D(data):
#     D = Dashboard(data=data)
#     return D.main()

# main_FF(data=r'C:\Users\SACHIN\Desktop\Big-Data-Platform for Predicting and Preventing Financial Fraud\data\clean\Clean Insurance Data.csv', model='Insurance_file')
# main_FF(data=r'C:\Users\SACHIN\Desktop\Big-Data-Platform for Predicting and Preventing Financial Fraud\data\clean\Clean Credit Card Data.csv', model='Credit_card_file')
# main_FF(data=r'C:\Users\SACHIN\Desktop\Big-Data-Platform for Predicting and Preventing Financial Fraud\data\Cleaned Data.csv', model='transaction_file')