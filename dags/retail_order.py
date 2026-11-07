from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 
import smtplib
from email.message import EmailMessage
from google.cloud import secretmanager 
import json 

default_args={
    "owner":"matias", 
    "depends_on_past":False, 
    "email_on_failure":False, 
    "retries":1, 
    "retry_delay": timedelta(minutes=5)
}

def get_secret(): 
    client=secretmanager.SecretManagerServiceClient()
    project_id='data-engineer-509817'
    secret_id='airflow-dev'
    version_id=1
    name=f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response=client.access_secret_version(request={"name":name})
    detail_secret_json=json.loads(response.payload.data.decode('utf-8'))
    return detail_secret_json




def send_success_email(**kwargs):
    secret=get_secret()
    
    # Configura tus datos de correo o usa el sistema de notificaciones de Airflow
    msg = EmailMessage()
    msg['Subject']="Alerta: Carga a BigQuery Exitosa"
    msg.set_content("Hola Matías,\n\nEl archivo de órdenes se ha cargado correctamente en la tabla de BigQuery (retail.orders).")
    
    msg['From'] = secret["email"]
    msg['To'] = secret["email"]  # O el destinatario de la alerta

    # Ejemplo usando servidor SMTP (Gmail, Outlook, etc.)
    # Nota: Si usas Gmail, recuerda usar una Contraseña de Aplicación (App Password)
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = secret["email"]
    smtp_password = secret["password"]

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print("Correo de alerta enviado exitosamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")
        raise e


with DAG(
    dag_id="gcs_to_bigquery",
    default_args=default_args, 
    description="Insertar archivo de cloud storage a bigquery", 
    schedule=None, 
    start_date=datetime(2026,11,7), 
    catchup=False, 
    tags=["csv", "gcs"] 
) as dag: 

    load_gcs_to_bq=GCSToBigQueryOperator(
        task_id="transfer_gcs_to_bigquery", 
        bucket="retail_order", 
        source_objects=["orders.csv" ], 
        destination_project_dataset_table="data-engineer-509817.retail.orders",
        gcp_conn_id="google_cloud_default", 
        schema_fields=[
            {"name": "order_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "order_customer_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
            
        ],
        autodetect=True, 
        source_format="CSV", 
        skip_leading_rows=1, 
        write_disposition="WRITE_TRUNCATE" 
    )

    transform_order_detail=BashOperator(
        task_id="transform_order_detail", 
        bash_command="cd /opt/airflow/dbt/retail && dbt run --profiles-dir ."
    )

    send_email_task = PythonOperator(
        task_id='send_alert_email',
        python_callable=send_success_email,
    )

    load_gcs_to_bq >>  transform_order_detail >>send_email_task