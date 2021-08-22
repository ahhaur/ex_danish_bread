import os
import sys
import time
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from src.edb.dbhelper import DbConnector
from src.edb.githelper import RepoReader

dbconfig = {'user':'root', 'pass':'S!mpl3P4ssw0rd', 'host': 'mariadb', 'db': 'edb'}
tbl_b = 'branch_table'
tbl_c = 'commit_table'
start_date = datetime.strptime("2021-07-20", "%Y-%m-%d")
myconn = DbConnector(dbconfig)
gitobj = RepoReader(org='apache', repo='airflow', start_date=start_date)

def get_branch(ds, **kwargs):
    col = ['name', 'sha']
    branches = gitobj.get_branch()
    for branch in branches:
        if myconn.check_record(tbl_b, 'branch_id', f"name='{branch['name']}'") == 0:
            print("Record not exists, insert it.")
            myconn.insert(tbl_b, col, (branch['name'], branch['sha']) )
        else:
            print("Record exists, skip it.")
    print('done')

def get_commit(ds, **kwargs):
    branches = myconn.get_result(f"select * from {tbl_b}")
    for b in branches:
        tpl = {
            'branch_id': b['branch_id'],
            'commit_sha' : '',
            'author_id' : '',
            'author_login' : '',
            'author_name' : '',
            'committer_id' : '',
            'committer_login' : '',
            'committer_name' : '',
            'commit_date' : '',
            'commit_datetime' : None,
            'message' : ''
        }
        last_record = myconn.get_record(f"select * from {tbl_c} where branch_id = '{b['branch_id']}' order by commit_datetime asc limit 1")
        if last_record is None:
            print('no last record.')
            commit_sha = b['sha']
            skip_first = False
        else:
            print('found last record.')
            commit_sha = last_record['commit_sha']
            skip_first = True

        commits = gitobj.get_branch_commit_conn(b['name'], commit_sha, conn=myconn, tpl=tpl, skip_first=skip_first)

        print(f"Done for branch #{b['branch_id']}...")

args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    'Data_Pipeline',
    default_args=args,
    description='Run this',
    schedule_interval="0 0 1 1 *",
    start_date=days_ago(1),
    tags=['Git API', 'Run this'],
) as dag:

    start = DummyOperator(task_id='start')

    task1 = PythonOperator(
        task_id='get_branch_info',
        python_callable=get_branch
    )
    
    task2 = PythonOperator(
        task_id='get_commit_info',
        python_callable=get_commit
    )

    start >> task1 >> task2