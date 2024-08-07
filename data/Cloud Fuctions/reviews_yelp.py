import pandas as pd
from google.cloud import bigquery, storage

#Esta funcion asemeja a la función main 
def transform(event, context):

    # Obtenemos la ruta
    bucket_name = event["bucket"]
    file_name = event["name"]
    
    # Inicializa el cliente de GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Descarga el archivo
    temp_file_path = f'/tmp/{file_name}'
    blob.download_to_filename(temp_file_path)
    print(f"File downloaded to: {temp_file_path}")

    # Carga el contenido del archivo .json en un DataFrame de pandas

    df = etl_process(bucket_name, file_name)

    load_byquery(df, bucket_name, file_name)

def etl_process(bucket_name, file_name):
    # Leemos el archivo json
    df = pd.read_json(f"gs://{bucket_name}/{file_name}", lines=True)


    # Eliminamos los nulos
    df = df.dropna()
    
    # Eliminamos los duplicados
    df = df.drop_duplicates()
    
    
    # Transformamos la columna tipo de dato
    df['stars'] = df['stars'].astype(float)
    df['useful'] = df['useful'].astype(int)
    df['funny'] = df['funny'].astype(int)
    df['cool'] = df['cool'].astype(int)
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d %H:%M:%S')
    
    
    df.reset_index(drop=True, inplace=True)
    
    return df


def load_byquery(df, bucket, name):

    # Cliente de bigquery
    client = bigquery.Client()

    # Crear el dataset
    dataset_id = 'braided-grammar-430922-b4.datanexus'
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'southamerica-west1'
    dataset = client.create_dataset(dataset, exists_ok=True) # exists_ok=True; verifica si existe la base de datos   
    
    
    # Ruta de la tabla
    table_id = f'{dataset_id}.reviews_yelp'

    # Define el esquema de la tabla
    schema = [
    bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stars", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("useful", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("funny", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("cool", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
]
    
    # Configuracion del trabajo
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Carga los datos en la tabla de BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() 

    #Remover duplicados de BigQuery
    remove_duplicates(client, dataset_id, 'reviews_yelp')

def remove_duplicates(client, dataset_id, table_name):
    # Nombre de la tabla temporal
    temp_table_id = f'{dataset_id}.temp_{table_name}'

    # Consulta SQL para crear una tabla temporal con registros únicos
    query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT DISTINCT * FROM `{dataset_id}.{table_name}`
    """

    # Ejecuta la consulta para crear la tabla temporal
    query_job = client.query(query)
    query_job.result()
    print(f"Temporary table {temp_table_id} created with unique records.")

    # Sobrescribir la tabla original con los datos únicos
    query = f"""
    CREATE OR REPLACE TABLE `{dataset_id}.{table_name}` AS
    SELECT * FROM `{temp_table_id}`
    """

    # Ejecuta la consulta para sobrescribir la tabla original
    query_job = client.query(query)
    query_job.result()
    print(f"Table {dataset_id}.{table_name} overwritten with unique records.")
    
    # eliminar la tabla temporal
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Temporary table {temp_table_id} deleted.")
