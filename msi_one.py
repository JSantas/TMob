#test
from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from providers.sapiq.hooks.sapiq import SapIQHook

from airflow.utils.dates import days_ago
from airflow.models import Variable

from msi.msi_reports import reports
from msi.msi_reports import connections

# msi requirements

# database modules
import sqlalchemy as sa
import psycopg2
from sqlalchemy.sql.operators import isnot
import sqlanydb
from sqlalchemy.dialects.sybase import \
    CHAR, VARCHAR, TIME, NCHAR, NVARCHAR,\
    TEXT, DATE, DATETIME, FLOAT, NUMERIC,\
    BIGINT, INT, INTEGER, SMALLINT, BINARY,\
    VARBINARY, UNITEXT, UNICHAR, UNIVARCHAR,\
    IMAGE, BIT, MONEY, SMALLMONEY, TINYINT

# etl with pandas
import pandas as pd

# to transform
import json
import re

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['lopatao@fuib.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

default_loglevel = 'info'

extractFilePath = '/extract'
deleteInterval = '1 year'
loadFromId = None


def get_dwh_connection_id(Environment):
    connection_id = 'dwh'
    connection_suffix = connections[connection_id]['environments'][
        environmentName]['suffix']
    if bool(connection_suffix):
        connection_id = connection_id + f"_{connection_suffix}"
    return connection_id


def get_report_connection_id(Environment, OperatorName):
    connection_id = reports[OperatorName]['ConnectionId']
    connection_suffix = connections[connection_id]['environments'][
        environmentName]['suffix']
    if bool(connection_suffix):
        connection_id = connection_id + f"_{connection_suffix}"
    return connection_id


def get_source_last_ReportId(Environment, OperatorName, TableName):

    #create  a PostgresHook
    db_hook = PostgresHook(get_report_connection_id(Environment, OperatorName))

    sql = f"""
    SELECT COALESCE(MAX(s.id), 0) LastReportId
    FROM public.{TableName} s
    ;
  """
    LastReportId = int(db_hook.get_first(sql)[0])
    return LastReportId


def get_target_last_ReportId(Environment, OperatorName, ReportName):

    #create  a SapIQHook
    db_hook = SapIQHook(get_dwh_connection_id(Environment))

    lastReportIdStmnt = f"""
    SELECT ISNULL(MAX(r.OriginalId), 0) LastReportId
    FROM MSI.tb_msi_Request r
    WHERE r.OperatorName = '{OperatorName}'
    AND r.ReportName = '{ReportName}'
    ;
  """
    LastReportId = int(db_hook.get_first(lastReportIdStmnt)[0])
    return LastReportId


def _extract_and_transform(Environment, operatorName, reportName, tableName,
                           requestName, extractFilePath, **kwargs):

    #create  a PostgresHook
    db_hook = PostgresHook(get_report_connection_id(Environment, operatorName))

    count_requests = 0
    count_parameters = 0
    count_values = 0
    maxId = 0
    maxIdParameter = 0
    maxIdResponse = 0
    ti = kwargs["ti"]
    environmentPrefix = kwargs["environmentPrefix"]
    loadFromId = int(
        ti.xcom_pull(
            task_ids=
            f'{environmentPrefix}get_target_id_{operatorName}_{reportName}'))
    # Preparing a statement before hand can help protect against injections.
    date_cast_Kyivstar = """CAST(s.created_date AS timestamp)"""
    date_cast_Vodafone = """CAST(s.created_date AT TIME ZONE 'Europe/Kiev' AS timestamp)"""
    environmentPrefix = kwargs["environmentPrefix"]
    local_filepath = f'{extractFilePath}/{environmentPrefix}{operatorName}_{reportName}'
    stmt = sa.text(f"""
  SELECT
        s.id
      ,  { date_cast_Kyivstar if operatorName == 'Kyivstar' else date_cast_Vodafone } created_date
      , s.phone_number
      , s.caller_id
      , s.caller_type
      , s.login
      , s.system_code
      , s.request
      , s.response
      , s.error_message
  FROM
      public.{tableName} s
  WHERE
      s.id  > {loadFromId}
  """)

    df: pd.DataFrame = db_hook.get_pandas_df(sql=stmt.text)
    df = df.rename(
        columns={
            "id": "OriginalId",
            "created_date": "CreatedDate",
            "system_code": "SystemCode",
            "caller_id": "CallerId",
            "caller_type": "CallerType",
            "login": "RequestLogin",
            "phone_number": "SubscriberId",
            "error_message": "ErrorMessage",
            "request": "Request",
            "response": "Response"
        })
    # perform transformations on df here

    # add RequestId surrogate key
    #df['unique_id'] = pd.factorize(df['id'])[0]
    df['Id'] = df.index + maxId + 1

    # OperatorName as parameter value
    df['OperatorName'] = operatorName

    # ReportName as parameter value
    df['ReportName'] = reportName

    # CreatedDate remove timezone
    #df['CreatedDate'] = pd.to_datetime(df.CreatedDate).dt.tz_localize(None)

    # SystemCode replace null values to Unknown
    df["SystemCode"].fillna("Unknown", inplace=True)

    # IsError if ErrorMessage not null
    # df['IsError'] = df.ErrorMessage.notnull()
    df['IsError'] = df.ErrorMessage.apply(lambda x: '0' if x is None else '1')

    # Convert Request and Response to json
    df['Response'] = df.Response.apply(lambda x: json.loads(x)
                                       if type(x) is str else x)
    df['Request'] = df.Request.apply(lambda x: json.loads(x)
                                     if type(x) is str else x)

    # UniqueRequestId response::json#>>'{cid}'
    df['UniqueRequestId'] = df.Response.apply(
        lambda x: None if x is None else x.get(requestName))

    # UniqueRequestId from ErrorMessage
    def findRequestId(element, message):
        regex = r"^.*BadRequest: (\{.+\}).*$"
        m = re.search(regex, message, re.MULTILINE | re.DOTALL)
        rv = None
        if m:
            rv = json.loads(m.group(1))
            try:
                keys = element.split('.')
                for key in keys:
                    rv = rv[key]
            except KeyError:
                rv = None
        return rv

    df["UniqueRequestId"] = df[
        (df["UniqueRequestId"].isnull()) & (df["IsError"])].ErrorMessage.apply(
            lambda x: None if x is None else findRequestId(requestName, x))

    # get key path and value from dictionary to list
    def getValues(id: int, source, target: list):
        def get_path_value(obj, path: str):
            for k, v in obj.items() if isinstance(obj, dict) else enumerate(
                    obj, 1):
                if isinstance(v, (dict, list)):
                    yield from get_path_value(
                        v, f"{path}/{k}" if bool(path) else str(k))
                else:
                    yield f"{path}/{k}" if bool(path) else str(k), str(v)

        for path, v in get_path_value(source, path=''):
            target.append([id, path, v])

    RequestParameter = []
    ResponseValue = []
    for index, row in df.iterrows():
        if row.Request is not None:
            getValues(id=row.Id, source=row.Request, target=RequestParameter)
        if row.Response is not None:
            getValues(id=row.Id, source=row.Response, target=ResponseValue)

    count_requests = len(df.index)
    count_parameters = len(RequestParameter)
    count_values = len(ResponseValue)

    # Request
    df[[
        'Id', 'OperatorName', 'ReportName', 'OriginalId', 'SubscriberId',
        'CreatedDate', 'SystemCode', 'CallerId', 'CallerType', 'RequestLogin',
        'UniqueRequestId', 'IsError', 'ErrorMessage', 'Request', 'Response'
    ]].to_csv(local_filepath + "_Request.csv",
              sep=',',
              header=True,
              index=False,
              quoting=0)

    # RequestParameter
    dfp = pd.DataFrame(RequestParameter,
                       columns=['RequestId', 'Name', 'Value'])
    dfp['Id'] = dfp.index + maxIdParameter + 1
    dfp[['Id', 'RequestId', 'Name',
         'Value']].to_csv(local_filepath + "_RequestParameter.csv",
                          sep=',',
                          header=True,
                          index=False,
                          quoting=0)

    # ResponseValue
    dfr = pd.DataFrame(ResponseValue, columns=['RequestId', 'Name', 'Value'])
    dfr['Id'] = dfr.index + maxIdResponse + 1
    dfr[['Id', 'RequestId', 'Name',
         'Value']].to_csv(local_filepath + "_ResponseValue.csv",
                          sep=',',
                          header=True,
                          index=False,
                          quoting=0)

    logging.info(f'Requests count: {count_requests}')
    logging.info(f'Parameters count: {count_requests}')
    logging.info(f'Response values count: {count_values}')

    return f'''Data for operator {operatorName} report {reportName} extracted\n
  Requests count: {count_requests}\n
  Parameters count: {count_parameters}\n
  Values count: {count_values}
  '''


class DWHLoad(PythonOperator):
    template_ext = ('.sql', )


def _load(Environment, operatorName, reportName, extractFilePath, **kwargs):

    query_prepare = kwargs['templates_dict']['query_prepare']
    query_load_Request = kwargs['templates_dict']['query_load_Request']
    query_load_RequestParameter = kwargs['templates_dict'][
        'query_load_RequestParameter']
    query_load_ResponseValue = kwargs['templates_dict'][
        'query_load_ResponseValue']
    query_update = kwargs['templates_dict']['query_update']

    #create  a SapIQHook
    db_hook = SapIQHook(get_dwh_connection_id(Environment))

    iqEngine = db_hook.get_sqlalchemy_engine()
    dwhDb = iqEngine.connect().execution_options(autocommit=True,
                                                 case_sensitive=True)

    dwhDb.execute(query_prepare)
    dwhDb.execute(query_load_Request)
    dwhDb.execute(query_load_RequestParameter)
    dwhDb.execute(query_load_ResponseValue)
    dwhDb.execute(query_update)

    return 'Data is loaded to DWH'


with DAG(
        'etl_msi_to_dwh',
        default_args=default_args,
        description='Load requests to mobile operators to DHW',
        schedule_interval=timedelta(minutes=30),
        start_date=datetime(2021, 11, 1, 1, 15, 0),
        catchup=False,
        #template_searchpath="{{ var.value.get('msi_sql_path', '/opt/airflow/dags/msi/sql') }}",
        template_searchpath="/opt/airflow/dags/msi/sql",
        tags=['MSI', 'narsildb', 'hadhafangdb'],
) as dag:

    previousTaskGroupEnd = None

    run_environments = [
        x.strip()
        for x in Variable.get('msi_run_environments', 'stage').split(",")
    ]

    #    for environmentName in run_environments:
    #      for operatorName in reports:
    #        for reportName in reports[operatorName]['Reports']:

    operatorName = 'Vodafone'
    #operatorName = 'Kyivstar'
    reportName = 'Pilot'
    #reportName = 'Checksim'
    environmentName = run_environments[0]

    connection_id = reports[operatorName]['ConnectionId']
    connection_suffix = connections[connection_id]['environments'][
        environmentName]['suffix']
    environmentSuffix = "_" + connection_suffix if connection_suffix else ''
    environmentPrefix = connection_suffix + "_" if connection_suffix else ''

    tableName = reports[operatorName]['Reports'][reportName]['Table Name']
    requestName = reports[operatorName]['RequestName']

    logging.info(f'Loading {operatorName}, {reportName}, {tableName}')

    start = DummyOperator(
        task_id=f"{environmentPrefix}start_{operatorName}_{reportName}")

    if previousTaskGroupEnd:
        previousTaskGroupEnd >> start

    end = DummyOperator(
        task_id=f"{environmentPrefix}end_{operatorName}_{reportName}",
        trigger_rule="none_failed",
    )

    previousTaskGroupEnd = end

    get_source_id = PythonOperator(
        task_id=f'{environmentPrefix}get_source_id_{operatorName}_{reportName}',
        python_callable=get_source_last_ReportId,
        op_kwargs={
            'Environment': environmentName,
            'OperatorName': operatorName,
            'TableName': tableName
        })

    get_target_id = PythonOperator(
        task_id=f'{environmentPrefix}get_target_id_{operatorName}_{reportName}',
        python_callable=get_target_last_ReportId,
        op_kwargs={
            'Environment': environmentName,
            'OperatorName': operatorName,
            'ReportName': reportName,
        })

    nothing_to_load = DummyOperator(
        task_id=
        f"{environmentPrefix}nothing_to_load_{operatorName}_{reportName}")

    def _check_last_id(**context):
        environmentPrefix = context["environmentPrefix"]
        operatorName = context["operatorName"]
        reportName = context["reportName"]
        ti = context["ti"]
        source_id = int(
            ti.xcom_pull(
                task_ids=
                f'{environmentPrefix}get_source_id_{operatorName}_{reportName}'
            ))
        target_id = int(
            ti.xcom_pull(
                task_ids=
                f'{environmentPrefix}get_target_id_{operatorName}_{reportName}'
            ))
        if source_id > target_id:
            return f"{environmentPrefix}extract_and_transform_{operatorName}_{reportName}"
        else:
            return f"{environmentPrefix}nothing_to_load_{operatorName}_{reportName}"

    check_last_id = BranchPythonOperator(
        task_id=f"{environmentPrefix}check_last_id_{operatorName}_{reportName}",
        python_callable=_check_last_id,
        op_kwargs={
            "environmentPrefix": environmentPrefix,
            "operatorName": operatorName,
            "reportName": reportName,
        },
    )

    extract_and_transform = PythonOperator(
        task_id=
        f"{environmentPrefix}extract_and_transform_{operatorName}_{reportName}",
        python_callable=_extract_and_transform,
        op_args=[
            environmentName, operatorName, reportName, tableName, requestName,
            extractFilePath
        ],
        op_kwargs={
            "environmentPrefix": environmentPrefix,
        },
    )

    load = DWHLoad(
        task_id=f"{environmentPrefix}load_{operatorName}_{reportName}",
        python_callable=_load,
        templates_dict={
            'query_prepare': 'msi_prepare.sql',
            'query_load_Request': 'msi_load_Request.sql',
            'query_load_RequestParameter': 'msi_load_RequestParameter.sql',
            'query_load_ResponseValue': 'msi_load_ResponseValue.sql',
            'query_update': 'msi_update.sql',
        },
        params={
            'Environment': environmentName,
            'operatorName': operatorName,
            'reportName': reportName,
            'extractFilePath': extractFilePath,
            "environmentPrefix": environmentPrefix,
        },
        op_kwargs={
            "Environment": environmentName,
            "operatorName": operatorName,
            "reportName": reportName,
            "extractFilePath": extractFilePath,
            "environmentPrefix": environmentPrefix,
        },
    )

    start >> [get_source_id, get_target_id] >> check_last_id
    check_last_id >> [extract_and_transform, nothing_to_load]
    extract_and_transform >> load >> end
    nothing_to_load >> end
