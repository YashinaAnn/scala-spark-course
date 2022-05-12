from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    dag_id="marketing_analytics_dag",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=None
) as dag:

    java_home = Variable.get("JAVA_HOME")
    app_home = Variable.get("APP_HOME")

    build_projection = SparkSubmitOperator(
        task_id='build_projection',
        conn_id='spark_local',
        java_class='marketing_analyzer.projection.PurchaseProjectionDsBuilder',
        application=f"{app_home}/target/scala-2.12/marketing_analyzer-assembly-0.1.jar",
        conf={"spark.driver.host": "localhost"},
        env_vars={"JAVA_HOME": java_home},
        dag=dag
    )

    top_campaigns = SparkSubmitOperator(
        task_id='top_campaigns',
        conn_id='spark_local',
        java_class='marketing_analyzer.metrics.topCampaigns.TopCampaignsSqlCalculator',
        application=f"{app_home}/target/scala-2.12/marketing_analyzer-assembly-0.1.jar",
        conf={"spark.driver.host": "localhost"},
        env_vars={"JAVA_HOME": java_home},
        dag=dag
    )

    top_channels = SparkSubmitOperator(
        task_id='top_channels',
        conn_id='spark_local',
        java_class='marketing_analyzer.metrics.topChannels.TopChannelsSqlCalculator',
        application=f"{app_home}/target/scala-2.12/marketing_analyzer-assembly-0.1.jar",
        conf={"spark.driver.host": "localhost"},
        env_vars={"JAVA_HOME": java_home},
        dag=dag
    )

    build_projection >> [top_campaigns, top_channels]