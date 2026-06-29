from datetime import datetime
from airflow import DAG
import uuid
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 6, 7,10,0)
}

def get_data():
    import requests

    res = requests.get('http://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name']= res['name']['last']
    data['gender']= res['gender']
    data['address'] = f"{str(res['location']['street']['number'])} {res['location']['street']['name']}, " \
                      f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['postal_code'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    #res = get_data()
    #res = format_data(res)
    #print(json.dumps(res,indent = 3))

    producer = KafkaProducer(bootstrap_servers= ['broker:29092'], max_block_ms = 5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60 : # it'll be working for 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue



with DAG('user_automation',
        default_args=default_args,
        schedule='@daily', 
        catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    ) 

