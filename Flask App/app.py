from flask import Flask, render_template, request
import pandas as pd
import json
import boto3
import pickle
from athenaQuery import athena_query
from athenaQuery import athena_to_s3
from athenaQuery import s3_to_pandas
from athenaQuery import exectue
from decodeCategories import decode
import os
import pygal

app = Flask(__name__)

params = {
        'region': 'us-east-1',
        'database': 'airbnb-listings',
        'bucket': 'airbnb-final-project',
        'path': 'Training/Result',
        'query': ''
    }


session = boto3.Session(aws_access_key_id='AKIA4LWYDIHLX7JHKD5E',
                            aws_secret_access_key='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')

@app.route("/")
def home():
    sql = 'SELECT distinct neighbourhood_group_cleansed FROM "airbnb-listings"."airbnb_processesedtest"'
    params['query'] = sql
    df = exectue(session, params)
    if df.empty:
        return "Error Fetching records from Athena"
    neighbours = df['neighbourhood_group_cleansed'].tolist()

    sql = 'SELECT distinct property_type FROM "airbnb-listings"."airbnb_processesedtest"'
    params['query'] = sql
    df = exectue(session, params)
    property_types = df['property_type'].tolist()

    sql = 'SELECT distinct cancellation_policy FROM "airbnb-listings"."airbnb_processesedtest"'
    params['query'] = sql
    df = exectue(session, params)
    cancellation_policies = df['cancellation_policy'].tolist()

    sql = 'SELECT distinct bed_type FROM "airbnb-listings"."airbnb_processesedtest"'
    params['query'] = sql
    df = exectue(session, params)
    print(df.head())
    bed_types = df['bed_type'].tolist()

    return render_template(
        'index.html', neighbours=neighbours, property_types=property_types
        , cancellation_policies=cancellation_policies, bed_types=bed_types
        )

@app.route("/predict", methods=['POST'])
def predict():
    d=request.form.to_dict()
    d['neighbourhood_group_cleansed'] = decode(d['neighbourhood_group_cleansed'])
    d['property_type'] = decode(d['property_type'])
    d['cancellation_policy'] = decode(d['cancellation_policy'])
    d['bed_type'] = decode(d['bed_type'])
    df = pd.DataFrame([d])
    
    # client = boto3.client(
    #     's3', 
    #     aws_access_key_id='AKIA4LWYDIHLX7JHKD5E',
    #     aws_secret_access_key='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'
    # )

    # h2o.init()
    
    # if not os.listdir('MLmodel/'):
    #     client.download_file('airbnb-final-project', 'H2O Model/StackedEnsemble_BestOfFamily_AutoML_20191212_180451', 'MLmodel/auto_ml')
    
    # auto_ml = h2o.load_model('MLmodel/auto_ml')

    # h_df = auto_ml.predict(h2o.H2OFrame(df))

    rf = open("MLmodel\Finalized_Model_RF.pkl","rb")
    model = pickle.load(rf)

    prediction = model.predict(df)
    print("Prediction:", prediction)

    #Get the mean of price
    sql = f"""select avg(price) as Avg_price FROM "airbnb-listings"."airbnb_processesedtest"
                where neighbourhood_group_cleansed = '{request.form['neighbourhood_group_cleansed']}'
                and property_type = '{request.form['property_type']}'
                and cancellation_policy = '{request.form['cancellation_policy']}'
                and bed_type = '{request.form['bed_type']}'
            """
    
    params['query'] = sql
    print(sql)
    df = exectue(session, params)
    print("Avg_price", df['Avg_price'][0])

    # Create Chart
    line_chart = pygal.Bar()
    line_chart.title = 'Compare MEAN and PREDICTION'
    line_chart.add('Prediction', prediction[0])
    line_chart.add('Mean', df['Avg_price'][0])
    return line_chart.render_response()

if __name__ == "__main__":
    app.run(debug=True)