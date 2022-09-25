from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3, json, time,decimal,os
import pandas as pd
from airflow.models import Variable
import requests

def readconfig(**kwargs):
    s3 = boto3.resource('s3')
    config_path=Variable.get('app_config')
    bucketname=config_path.split("/")[2]
    filename=("/").join(config_path.split("/")[3:])
    content_object = s3.Object(bucketname, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content

def copydata(**kwargs):
    source_bucket=kwargs['dag_run'].conf['Bucket']
    destination_bucket=kwargs['ti'].xcom_pull(task_ids='readconfigfile')['actives_data']['raw_source'].split("/")[2]
    key=kwargs['dag_run'].conf['key']
    s3_resource = boto3.resource('s3')
    copy_source = {'Bucket': source_bucket,'Key': key}
    bucket = s3_resource.Bucket(destination_bucket)
    bucket.copy(copy_source, key)
            
def prevalidation(**kwargs):
    source_bucket=kwargs['dag_run'].conf['Bucket']
    destination_bucket=kwargs['ti'].xcom_pull(task_ids='readconfigfile')['actives_data']['raw_source'].split("/")[2]
    key=kwargs['dag_run'].conf['key']
    source_path=os.path.join("s3://"+source_bucket, key)
    destination_path=os.path.join("s3://"+destination_bucket, key)
    source_df=pd.read_parquet(source_path)
    dest_df=pd.read_parquet(destination_path)
    if source_df.equals(dest_df)==True:
        print("successfully copied data from landingzone to rawzone")
        return "Success"
    else:
        return "Failed"
        
def create_emr(**kwargs):
    prevalidation=kwargs['ti'].xcom_pull(task_ids='pre_validation')
    if prevalidation=="Success":
        prevalidation=kwargs['ti'].xcom_pull(task_ids='pre_validation')
        connection = boto3.client('emr',region_name='ap-southeast-1')
        cluster_id = connection.run_job_flow(Name='Nitesh_emr_airflow',LogUri='s3://nitesh-landingzone/logs/',ReleaseLabel='emr-6.2.1',
                Applications=[
                            { 'Name': 'hadoop' },
                            { 'Name': 'spark' },
                            { 'Name': 'hive' },
                            { 'Name': 'livy' }
                        ],
                Configurations=[
                            {
                            'Classification': Variable.get('Classification'),
                            'Properties':Variable.get('Properties',deserialize_json=True),
                            }
                        ],
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': "Master",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        },
                        {
                            'Name': "Slave",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        }
                    ],
                    
                    'Ec2KeyName': 'nitesh_emr',
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'EmrManagedMasterSecurityGroup': Variable.get('securitygp'),
                    'EmrManagedSlaveSecurityGroup': Variable.get('securitygp'),
                },
                
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                AutoTerminationPolicy = {"IdleTimeout": 1800},
                )
        return cluster_id['JobFlowId']
    else:
        raise ValueError("Pre-validation failed")

def clusterrunning(**kwargs):
    client = boto3.client("emr",region_name='ap-southeast-1')
    waiter = client.get_waiter("cluster_running")
    waiter.wait(
        ClusterId=kwargs['ti'].xcom_pull(task_ids='EMR_cluster'),
        WaiterConfig={
            "Delay": 60,
            "MaxAttempts": 100
        }
    )
def getdetails(**kwargs):
    client = boto3.client("emr",region_name='ap-southeast-1')
    x=client.describe_cluster(ClusterId=kwargs['ti'].xcom_pull(task_ids='EMR_cluster'))
    return x['Cluster']['MasterPublicDnsName']
    
def livysubmit(**kwargs):
    host = kwargs['ti'].xcom_pull(task_ids='EMR_details')
    filename=Variable.get('sparkfile')
    arguments=kwargs['dag_run'].conf['filename']
    classname=Variable.get('className')
    data = {'file': filename,'args':[arguments], 'className': classname}
    headers = {'Content-Type': 'application/json'}
    try:
        r = requests.post('http://' + host + ':8998/batches', data=json.dumps(data), headers=headers)
    except (r.exceptions.ConnectionError, ValueError):
        return livysubmit()
    else:
        return r.headers

def livy_job_status(**kwargs):
    status = ''
    host = kwargs['ti'].xcom_pull(task_ids='EMR_details')
    livy_headers=kwargs['ti'].xcom_pull(task_ids='Emr_livy')
    url = 'http://' + host + ':8998'
    session_url = host + livy_headers['location'].split('/statements', 1)[0]
    
    while status != 'success':
        statement_url = url + livy_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        status = statement_response.json()['state']
        
        if status=='dead':
            raise Exception("Session Dead")

    print(statement_response.json())
    final_status = statement_response.json()['state']
    print(final_status)
    return final_status

def terminate_cluster(**kwargs):
    job_status=kwargs['ti'].xcom_pull(task_ids='livy_job_status')
    ClusterId=kwargs['ti'].xcom_pull(task_ids='EMR_cluster')
    client = boto3.client("emr",region_name='ap-southeast-1')
    
    if job_status=="success":
        response = client.terminate_job_flows(JobFlowIds=[ClusterId])
        print(response)
    else:
        print("Couldn't terminate cluster")
        

def post_validation(**kwargs):
    json_data=kwargs['ti'].xcom_pull(task_ids='readconfigfile')
    #Data availability check
    filename=kwargs['dag_run'].conf['filename']
    
    if filename=="Actives":
        active_s_path=json_data['actives_data']['raw_source']
        active_d_path=json_data['actives_data']['final_destination']
        active_s_df=pd.read_parquet(active_s_path)
        active_d_df=pd.read_parquet(active_d_path)
        #Data availability check
        if active_d_df[active_d_df.columns[0]].count() !=0:
            #Count check
            for columns in active_s_df.columns:
                if active_d_df[columns].count()==active_s_df[columns].count():
                    print(f"count for column {columns} is valid")
                else:
                    print(f"count mismatch for column {columns}")
            # Datatype validation
            d={'user_latitude':float,'user_longitude':float,'location_source':str}
            for transform_col, datatype in d.items():
                if isinstance(active_d_df[transform_col][0], datatype)==True:
                    print(f"Datatype is valid for column {transform_col}")
                else:
                    print(f"Datatype is invalid for column {transform_col}")
        else:
            raise ValueError("No Data is coming in stagingzone")

    
    if filename=="Viewership":
        view_s_path=json_data['viewership_data']['raw_source']
        view_d_path=json_data['viewership_data']['final_destination']
        view_s_df=pd.read_parquet(view_s_path)
        view_d_df=pd.read_parquet(view_d_path)
        #Data availability check
        if view_d_df[view_d_df.columns[0]].count() !=0:
            #Count check
            for columns in view_s_df.columns:
                if view_d_df[columns].count()==view_s_df[columns].count():
                    print(f"count for column {columns} is valid")
                else:
                    print(f"count mismatch for column {columns}")
            # Datatype validation
            d={'user_lat':float,'user_long':float,'location_source':str}
            for transform_col, datatype in d.items():
                if isinstance(view_d_df[transform_col][0], datatype)==True:
                    print(f"Datatype is valid for column {transform_col}")
                else:
                    print(f"Datatype is invalid for column {transform_col}")
        else:
            raise ValueError("No Data is coming in stagingzone")

dag = DAG(dag_id="Airflow_dags_9",
          start_date=datetime(2022, 8, 31),
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=60),
          catchup=False)

readconfig = PythonOperator(task_id="readconfigfile", python_callable=readconfig, dag=dag)

copyfile = PythonOperator(task_id="s3_copy", python_callable=copydata, dag=dag)

pre_validation = PythonOperator(task_id="pre_validation", python_callable=prevalidation, dag=dag)

cluster_creation = PythonOperator(task_id="EMR_cluster", python_callable=create_emr, dag=dag)

cluster_run_status = PythonOperator(task_id="EMR_status", python_callable=clusterrunning, dag=dag)

cluster_properties = PythonOperator(task_id="EMR_details", python_callable=getdetails, dag=dag)

livysubmit = PythonOperator(task_id="Emr_livy", python_callable=livysubmit, dag=dag)

livystatus = PythonOperator(task_id="livy_job_status", python_callable=livy_job_status, dag=dag)

cluster_termination = PythonOperator(task_id="terminate_cluster", python_callable=terminate_cluster, dag=dag)

post_validation=PythonOperator(task_id="post_validation", python_callable=post_validation, dag=dag)

readconfig>>copyfile>>pre_validation>>cluster_creation>>cluster_run_status>>cluster_properties>>livysubmit>>livystatus>>cluster_termination>>post_validation
