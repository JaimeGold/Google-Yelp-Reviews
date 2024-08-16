import pandas as pd
from google.cloud import bigquery, storage

#Esta funcion asemeja a la función main 
def transform(event, context):
    """
    Obtenemos la ruta del bucket y el nombre del archivo de entrada, y lo descargamos al directorio temporal. Luego, cargamos el contenido del archivo .pkl en un DataFrame de pandas. Luego, lo procesamos y lo cargamos en BigQuery.
    
    Args:
        event (dict): Contiene la ruta del bucket y el nombre del archivo de entrada.
        context (google.cloud.functions.Context): Objeto de contexto de Google Cloud Functions.
    """
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
    # Leemos el parquet
    df = pd.read_json(f"gs://{bucket_name}/{file_name}", lines=True)
    
        
    #Reemplazamos nulos (Nan, Null) por vacios
    df['text'].fillna('', inplace=True)
    df['pics'] = df['pics'].apply(lambda x: x if isinstance(x, list) else [])
    df['resp'].fillna({}, inplace=True)
    
    #Eliminamos duplicados
    columns_to_convert = ['pics', 'resp']
    df[columns_to_convert] = df[columns_to_convert].map(str)
    df = df.drop_duplicates()

    #Validacion tipos de datos


    # Convertir la columna 'name' a string, eliminando valores numéricos
    df = df[~df['name'].str.isnumeric()]
    df["name"] = df["name"].astype(str)
    df["name"] = df["name"].apply(lambda x: x.lower())

    # Convertir la columna 'rating' a int, eliminando valores inválidos
    df = df[pd.to_numeric(df['rating'], errors='coerce').notna()]
    df['rating'] = df['rating'].astype(float)
   
    df['time'] = df['time'].astype(int)

    df.reset_index(drop=True, inplace=True)

    return df



def load_byquery(df, bucket, name):
    """
    Cargamos el DataFrame en BigQuery creando el dataset si no existe.
    
    Args:
        df (DataFrame): DataFrame a cargar.
        bucket (str): Nombre del bucket donde se encuentra el archivo .json.
        name (str): Nombre del archivo .json. 
    """
    # Cliente de bigquery
    client = bigquery.Client()

    # Crear el dataset
    dataset_id = 'braided-grammar-430922-b4.datanexus'
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'southamerica-west1'
    dataset = client.create_dataset(dataset, exists_ok=True) # exists_ok=True; verifica si existe la base de datos   
    
    
    # Ruta de la tabla
    table_id = f'{dataset_id}.reviews_google'

    # Define el esquema de la tabla
    schema = [
    bigquery.SchemaField("user_id", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("time", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("rating", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pics", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("resp", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gmap_id", "STRING", mode="REQUIRED"),
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
    remove_duplicates(client, dataset_id, 'reviews_google')

def remove_duplicates(client, dataset_id, table_name):
    """
    Elimina registros duplicados de una tabla de BigQuery con una consulta SQL.
    
    Args:
        client (bigquery.Client): Objeto de cliente de BigQuery.
        dataset_id (str): ID del dataset.
        table_name (str): Nombre de la tabla.
    """
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

    