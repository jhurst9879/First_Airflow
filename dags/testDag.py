try:
    from airflow import DAG
    from datetime import timedelta,datetime
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from airflow.models import Variable
    #from scripts.scraper import getMakeYrPrice
    from scripts.scraper import getModel
    from scripts.scraper import getDescription
    from scripts.scraper import getLink
    from scripts.output_df import final_df
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from scripts.scraper import getMakeYrPrice

    print('all dags work')
except Exception as e:
    print("Error {}".format(e))



with DAG(
    dag_id='csv_dag',
    schedule_interval='@daily',
    default_args={'owner':'airflow',
                  'retries':1,
                  'retry_delay':timedelta(minutes=1),
                  'start_date': datetime(2020,1,1)
                  },
    catchup=False

) as f:

    start_task = BashOperator(
        task_id='start',
        bash_command='date'
    )

    yr_mk_price_dag = PythonOperator(
        task_id='yr_mk_price_dag',
        python_callable=getMakeYrPrice
    )


    model_dag = PythonOperator(
        task_id='model_dag',
        python_callable=getModel
    )

    description_dag = PythonOperator(
        task_id='description_dag',
        python_callable=getDescription
    )

    link_dag = PythonOperator(
        task_id='link_dag',
        python_callable=getLink
    )
    final_dag = PythonOperator(
        task_id='final_dag',
        python_callable=final_df
    )



    start_task   >> [yr_mk_price_dag, model_dag, description_dag, link_dag] >> final_dag