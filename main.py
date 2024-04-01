from flask import url_for
from flask import Flask 
from flask import render_template
from flask import request
from werkzeug.utils import secure_filename
from config import UPLOAD_PATH, Total_Transactions, Valid_Transactions, Fraud_Transactions, Total_Users, Valid_Users, Fraud_Users, Total_Amount, Valid_Amount, Fraud_Amount
from flask import request, redirect
from functions import logrecord, recent_activities, record
from functions import main_FF
import os
import xgboost as xgb


# Requirements
app = Flask(__name__)
app.config['UPLOAD_PATH'] = UPLOAD_PATH

# Loading Home Page
@app.route('/', methods=['POST', 'GET'])
def index():
    message = [Total_Transactions, Valid_Transactions, Fraud_Transactions, Total_Users, Valid_Users, Fraud_Users, Total_Amount, Valid_Amount, Fraud_Amount]    
    return render_template('index.html', message = message, recent_activities=recent_activities())

# Uploading File
@app.route('/upload', methods=['POST'])
def upload_file():
    upload_files_name = ['transaction_file', 'Insurance_file', 'Credit_card_file']
    for upload in upload_files_name:
        if upload in request.files:
            file = request.files[upload]
            if file.filename == '':
                logrecord(f'{upload} Uploading Error')
                return render_template('pages-error-404.html')
            if file:
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_PATH'], filename))
                logrecord(f'{upload} Uploaded Successfully')
                # raw = main_FF(data=r'C:\Users\SACHIN\Desktop\Big-Data-Platform for Predicting and Preventing Financial Fraud\data\clean\Clean Insurance Data.csv', model='Insurance_file')
                raw, result = main_FF(data=os.path.join(app.config['UPLOAD_PATH'], filename), model=upload)
                return render_template('dashboard.html', DataAnalysis=raw, FraudDetection=result)
                # return render_template('dashboard.html', DataAnalysis=['a', 'b', 'c', {1:1, 2:2}, {1:1, 2:2}, {1:1, 2:2}], FraudDetection=['a', 'b', 'c', 'd'])   

    logrecord('File Uploading Error')
    return render_template('pages-error-404.html')

@app.route('/profile', methods=['GET'])
def profile():
    return render_template('profile-file.html')

@app.route('/faq', methods=['GET'])
def faq():
    return render_template('pages-faq.html')

@app.route('/contact', methods=['GET'])
def contact():
    return render_template('pages-contact.html')

@app.route('/register', methods=['GET','POST'])
def register():
    pass

#     if request.method == 'POST':
#         name = request.form.get('name')
#         email = request.form.get('email')
#         username = request.form.get('username')
#         password = request.form.get('password')
#         term = request.form.get('terms')

#         # Print or process the extracted data
#         print("Name:", name)
#         print("Email:", email)
#         print("Username:", username)
#         print("Password:", password)
#         print("Terms Checked:", term)

#         # Perform validation and create the user
#         if not username or not password:
#             # return render_template('pages-error-404.html')
#             return "Sorry, enter valid username or password"

#         # existing_user = User.query.filter_by(username=username).first()
#         # if existing_user:
#         #     return "Username already exists. Please choose a different one."

#         # new_user = User(name=name, email=email, username=username, password=password)
#         # db.session.add(new_user)
#         # db.session.commit()  

#         return "Thanks for visiting"
#     return render_template('pages-register.html')

@app.route('/login', methods=['GET'])
def login():
    pass
    # return render_template('pages-login.html')

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('pages-error-404.html')

if __name__ == '__main__':
    app.run(debug=True)