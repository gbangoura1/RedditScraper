from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import praw
import pandas as pd
from fpdf import FPDF
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'reddit_to_snowflake',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def scrape_reddit(**kwargs):
    reddit = praw.Reddit(client_id='7um3tb-0f_S2AmPOlFcuRQ',
                         client_secret='-MEktB6efsjtg0x099bq-pgUAt3kQQ',
                         user_agent='airflow_scrape')
    posts = reddit.subreddit('WashU').hot(limit=10)
    data_posts = []
    data_comments = []
    for p in posts:
        p.comments.replace_more(limit=0)
        data_posts.append({
            'post_id': p.id,
            'score': p.score,
            'num_comments': p.num_comments,
            'author': p.author.name if p.author else None,
            'flair': p.link_flair_text,
            'title': p.title,
            'body': p.selftext
        })
        for c in p.comments:
            data_comments.append({
                'comment_id': c.id,
                'post_id': p.id,
                'comment_text': c.body,
                'comment_author': c.author.name if c.author else None
            })
    pd.DataFrame(data_posts).to_csv('/tmp/raw_posts.csv', index=False)
    pd.DataFrame(data_comments).to_csv('/tmp/raw_comments.csv', index=False)

scrape_task = PythonOperator(
    task_id='scrape_reddit',
    python_callable=scrape_reddit,
    dag=dag
)

def load_to_snowflake(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='SnowFlakeConn')
    engine = hook.get_sqlalchemy_engine()

    posts = pd.read_csv('/tmp/raw_posts.csv')
    comments = pd.read_csv('/tmp/raw_comments.csv')

    posts.to_sql('RAW_POSTS', engine, if_exists='replace', index=False)
    comments.to_sql('RAW_COMMENTS', engine, if_exists='replace', index=False)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

def transform_tables(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='SnowFlakeConn')
    engine = hook.get_sqlalchemy_engine()
    conn = engine.connect()

    sql_stmts = [
        "CREATE OR REPLACE TABLE POSTS AS SELECT post_id, score, num_comments, author, flair, title, body FROM RAW_POSTS;",
        "CREATE OR REPLACE TABLE COMMENTS AS SELECT comment_id, post_id, comment_text, comment_author FROM RAW_COMMENTS;"
    ]

    for stmt in sql_stmts:
        conn.execute(stmt)

    conn.close()

transform_task = PythonOperator(
    task_id='transform_tables',
    python_callable=transform_tables,
    dag=dag
)

# Set task dependencies
scrape_task >> load_task >> transform_task
