import requests
import pandas as pd
import json
import time
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# << ---------------------------------------------------- >>
# << 1. CÓDIGO NUEVO: CONFIGURACIÓN PARA GOOGLE CLOUD BIGQUERY >>
# << ---------------------------------------------------- >>

# Placeholder para la ruta del archivo de credenciales de Google Cloud
# Asegúrate de que este archivo está en un lugar seguro y accesible para tu script.
# Puedes obtenerlo de la Consola de Google Cloud -> IAM & Admin -> Service Accounts.
# Reemplaza 'path/to/your/service_account.json' con la ruta real.
credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

if not credentials_path:
    raise ValueError("La variable de entorno 'GOOGLE_APPLICATION_CREDENTIALS' no está configurada.")


# Placeholder para los datos de tu tabla en BigQuery
project_id = 'peak-key-470610-t6'
dataset_id = 'pisos'  
table_id = 'pisos_particular'    

# 2. AUTENTICACIÓN: Carga las credenciales del archivo.
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=project_id)

# << ---------------------------------------------------- >>
# << 3. LÓGICA EXISTENTE PARA LA EXTRACCIÓN DE DATOS >>
# << ---------------------------------------------------- >>

def get_idealista_data(location_id, location_name_for_api):
    # ... (El código de esta función es el mismo, no es necesario modificarlo) ...
    rapidapi_key = os.environ.get('RAPIDAPI_KEY')

    if not rapidapi_key:
        raise ValueError("La variable de entorno 'RAPIDAPI_KEY' no está configurada.")

    url = "https://idealista7.p.rapidapi.com/listhomes"
    headers = {
        "x-rapidapi-key": rapidapi_key,  # <<------- Reemplazado por la variable de entorno
        "x-rapidapi-host": "idealista7.p.rapidapi.com"
    }
    all_properties = []
    num_page = 1
    total_pages = 1
    print(f"\n--- Starting data retrieval for locationId: {location_id} (API Location Name: {location_name_for_api}) ---")
    while num_page <= total_pages:
        querystring = {
            "order": "relevance",
            "operation": "sale",
            "locationId": location_id,
            "locationName": 'Madrid',
            "numPage": str(num_page),
            "maxItems": "40",
            "location": "es",
            "locale": "es",
            "sinceDate":"W"
        }
        print(f"Fetching page {num_page} for {location_id}...")
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            resultado = response.json()
            if num_page == 1:
                total_pages = resultado.get('totalPages', 1)
                print(f"Total pages for {location_id}: {total_pages}")
            element_list = resultado.get('elementList', [])
            all_properties.extend(element_list)
            num_page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {location_id} on page {num_page}: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for {location_id} on page {num_page}: {e}")
            break
    return all_properties

def extract_property_details(property_list):
    # ... (El código de esta función es el mismo) ...
    extracted_data = []
    for prop in property_list:
        details = {
            'propertyCode': prop.get('propertyCode'),
            'price': prop.get('price'),
            'propertyType': prop.get('propertyType'),
            'operation': prop.get('operation'),
            'size': prop.get('size') / 10 if prop.get('size') is not None else None,
            'rooms': prop.get('rooms'),
            'bathrooms': prop.get('bathrooms'),
            'address': prop.get('address'),
            'province': prop.get('province'),
            'municipality': prop.get('municipality'),
            'locationId': prop.get('locationId'),
            'latitude': prop.get('latitude'),
            'longitude': prop.get('longitude'),
            'url': prop.get('url'),
            'description': prop.get('description'),
            'status': prop.get('status'),
            'phoneNumberForMobileDialing': prop.get('contactInfo', {}).get('phone1', {}).get('phoneNumberForMobileDialing'),
            'contactName': prop.get('contactInfo', {}).get('contactName'),
            'userType': prop.get('contactInfo', {}).get('userType'),
            'hasParkingSpace': prop.get('parkingSpace', {}).get('hasParkingSpace'),
            'priceByArea': prop.get('priceByArea'),
            'hasSwimmingPool': prop.get('features', {}).get('hasSwimmingPool'),
            'hasTerrace': prop.get('features', {}).get('hasTerrace'),
            'hasAirConditioning': prop.get('features', {}).get('hasAirConditioning'),
            'hasBoxRoom': prop.get('features', {}).get('hasBoxRoom'),
            'hasGarden': prop.get('features', {}).get('hasGarden')
        }
        extracted_data.append(details)
    return extracted_data


# << ---------------------------------------------------- >>
# << 4. CÓDIGO NUEVO: FUNCIÓN PARA CARGAR EN BIGQUERY >>
# << ---------------------------------------------------- >>

def load_to_bigquery(dataframe, client, project_id, dataset_id, table_id):
    """
    Loads a Pandas DataFrame into a BigQuery table, handling duplicates.
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # 1. Obtener los propertyCode existentes en la tabla
    print("Fetching existing propertyCodes from BigQuery...")
    query = f"""
        SELECT propertyCode
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    try:
        existing_codes_df = client.query(query).to_dataframe()
        existing_codes = set(existing_codes_df['propertyCode'].tolist())
    except Exception as e:
        print(f"Warning: Could not fetch existing propertyCodes. Loading all data. Error: {e}")
        existing_codes = set()

    # 2. Filtrar el DataFrame para eliminar duplicados
    # Usamos la columna 'propertyCode' como llave primaria
    new_data = dataframe[~dataframe['propertyCode'].isin(existing_codes)].copy()
    
    if new_data.empty:
        print("No new data to load. All properties are already in the table.")
        return
    
    # 3. Cargar los datos nuevos
    print(f"Loading {len(new_data)} new rows into BigQuery table...")
    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_dataframe(new_data, table_ref, job_config=job_config)
    job.result() # Wait for the job to complete

    print(f"Successfully loaded {len(new_data)} rows into {project_id}:{dataset_id}.{table_id}")


# << ---------------------------------------------------- >>
# << 5. CÓDIGO PRINCIPAL (AJUSTADO PARA BIGQUERY) >>
# << ---------------------------------------------------- >>

if __name__ == "__main__":
    location_data = [
        {"id": "0-EU-ES-28-07-001-079-01-002", "name": "Lavapiés-Embajadores, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-01-006", "name": "Sol, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-02-002", "name": "Acacias, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-01-003", "name": "Huertas-Cortes, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-09-002", "name": "Argüelles, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-07-002", "name": "Arapiles, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-07-001", "name": "Gaztambide, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-07-006", "name": "Vallehermoso, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-04-003", "name": "Fuente del Berro, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-04-004", "name": "Guindalera, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-18-003", "name": "Ensanche de Vallecas - La Gavia, Madrid"},
        {"id": "0-EU-ES-28-07-001-079-06", "name": "Tetuán, Madrid"},
        {"id": "0-EU-ES-45-05-003-161", "name": "Seseña, Toledo"}
    ]

    all_extracted_properties = []

    for loc in location_data:
        loc_id = loc["id"]
        location_origin_name = loc["name"]
        api_location_name_param = loc["name"]
        start_time_loc_id = time.time()
        properties_for_location = get_idealista_data(loc_id, api_location_name_param)
        extracted_details = extract_property_details(properties_for_location)
        for prop_detail in extracted_details:
            prop_detail['origin_location_name'] = location_origin_name
        all_extracted_properties.extend(extracted_details)
        end_time_loc_id = time.time()
        time_taken = end_time_loc_id - start_time_loc_id
        print(f"Finished processing {loc_id} ({location_origin_name}). Time taken: {time_taken:.2f} seconds.")

    # Create a single DataFrame
    final_df = pd.DataFrame(all_extracted_properties)
    final_df_private = final_df[final_df['userType'] == 'private'].copy()
    
    # Convertir la columna 'propertyCode' a tipo entero para evitar errores de tipo de dato en BigQuery
    final_df_private['propertyCode'] = pd.to_numeric(final_df_private['propertyCode'], errors='coerce').astype('Int64')

    # Add new columns as requested
    final_df_private['category'] = "raw"
    final_df_private['fetch_timestamp'] = pd.to_datetime(pd.Timestamp.now(tz='UTC'))

    print("\n--- Final DataFrame (Private UserType) Head ---")
    print(final_df_private.head())
    print(f"\nTotal number of properties collected (Private UserType): {len(final_df_private)}")

    # << ---------------------------------------------------- >>
    # << 6. CÓDIGO NUEVO: CARGAR DATOS EN BIGQUERY >>
    # << ---------------------------------------------------- >>
    try:
        load_to_bigquery(final_df_private, client, project_id, dataset_id, table_id)
    except Exception as e:
        print(f"An error occurred during BigQuery load: {e}")
