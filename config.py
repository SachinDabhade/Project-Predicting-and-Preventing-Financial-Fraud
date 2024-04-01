# Configering the paths
import os

# Base Path
BASE_PATH = os.getcwd()

# Home Directories
HOME_DATA = os.path.join(BASE_PATH,'data/sqldb/')
HOME_MODELS = os.path.join(BASE_PATH,'models/')
HOME_RESULTS = os.path.join(BASE_PATH,'results/')
UPLOAD_PATH = os.path.join(BASE_PATH,'static/upload/')
USER_DATA = 'sqlite:///users.db'

# MODELS
credit_card_model = os.path.join(BASE_PATH, HOME_MODELS, 'credit card fraud detection.pkl')
# insurance_model = os.path.join(BASE_PATH, HOME_MODELS, 'insurance fraud detection.pkl')
insurance_model = os.path.join(BASE_PATH, HOME_MODELS, 'Insurance Fraud Detection LDA.pkl')
transaction_model = os.path.join(BASE_PATH, HOME_MODELS, 'transaction fraud detection model.pkl')

# Parameters
Total_Transactions = 0
Valid_Transactions = 0
Fraud_Transactions = 0
Total_Users = 0
Valid_Users = 0
Fraud_Users = 0
Total_Amount = 0
Valid_Amount = 0
Fraud_Amount = 0