import os
import requests
import functions_framework
from pprint import pprint
from google.cloud import bigquery

# Configuración (Idealmente desde variables de entorno)
# CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID", "TU_CLIENT_ID")
# CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET", "TU_CLIENT_SECRET")
# REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN", "TU_REFRESH_TOKEN")

CLIENT_ID = "203413"
CLIENT_SECRET = "20ec8ef05cdcea56147629cf87890b6665bc236c"
REFRESH_TOKEN = "4e34861332b2ea60e7e3e0f3cbf12b4c42dc7639"

TABLE_ID = "strava-api-dashboard.traceflow_dataset.activities"

def get_new_access_token():
    """Usa el refresh_token para obtener un access_token válido."""
    auth_url = "https://www.strava.com/oauth/token"
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN,
        'grant_type': 'refresh_token',
        'f': 'json'
    }
    print("Solicitando nuevo access token...")
    res = requests.post(auth_url, data=payload)
    res.raise_for_status()
    return res.json().get('access_token')

@functions_framework.http
def get_strava_activities(request):
    """
    Función que recorre todas las páginas de la API de Strava
    para extraer el historial completo de actividades.
    """
    print("Iniciando renovación de token...")
    # --- 1. Obtener Token Dinámico ---
    try:
        current_access_token = get_new_access_token()
    except Exception as e:
        return ({"error": "Falló la renovación del token", "details": str(e)}, 500)

    client = bigquery.Client()
    all_activities = []
    page = 1
    per_page = 200  # Máximo permitido por Strava para ser eficiente

    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {'Authorization': f'Bearer {current_access_token}'}

    print("Iniciando extracción masiva de actividades...")

    # --- 2. Extracción de datos ---
    while True:
        params = {
            'page': page,
            'per_page': per_page
        }

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Error en página {page}: {response.status_code}")
                return ({"error": response.text, "last_page": page}, response.status_code)

            data = response.json()

            # Si la API devuelve una lista vacía, terminamos
            if not data:
                break

            all_activities.extend(data)
            print(f"Página {page} procesada. Total acumulado: {len(all_activities)}")

            # Si recibimos menos de 200, es que ya no hay más actividades
            if len(data) < per_page:
                break

            page += 1

        except Exception as e:
            return ({"error": str(e)}, 500)

    actividades_limpias = []
    for act in all_activities:
        fila = {
            # Identificadores
            "id": act.get("id"),
            "athlete_id": act.get("athlete", {}).get("id"),
            "name": act.get("name"),

            # Métricas (Floats e Integers)
            "distance": float(act.get("distance", 0)),
            "moving_time": int(act.get("moving_time", 0)),
            "elapsed_time": int(act.get("elapsed_time", 0)),
            "average_speed": float(act.get("average_speed", 0)),
            "max_speed": float(act.get("max_speed", 0)),
            "total_elevation_gain": float(act.get("total_elevation_gain", 0)),

            # Fechas (Strava devuelve strings ISO 8601, BigQuery los acepta bien)
            "start_date": act.get("start_date"),
            "start_date_local": act.get("start_date_local"),
            "utc_offset": act.get("utc_offset"), # Desfase en segundos

            # Ubicación y Dispositivo
            "location_city": act.get("location_city"),
            "location_state": act.get("location_state"),
            "location_country": act.get("location_country"),
            "device_name": act.get("device_name"),

            # Clasificación
            "type": act.get("type"),
            "sport_type": act.get("sport_type")
        }
        actividades_limpias.append(fila)

    # 3. Inserción en BigQuery

    if actividades_limpias:
        try:
            # Opción recomendada: TRUNCATE mediante una query DDL
            print("Iniciando truncado de tabla...")
            truncate_query = f"TRUNCATE TABLE `{TABLE_ID}`"
            query_job = client.query(truncate_query)
            query_job.result()  # Esperar a que termine de borrar
            print(f"Tabla {TABLE_ID} truncada con éxito.")

            # Inserción de los nuevos datos
            print("Iniciando inserción en Bigquery...")
            errors = client.insert_rows_json(TABLE_ID, actividades_limpias)

            if not errors:
                return ({"status": "éxito", "filas_insertadas": len(actividades_limpias)}, 200)
            else:
                return ({"status": "error_insercion", "errores_bq": errors}, 500)

        except Exception as e:
            return ({"status": "error_bq", "detalle": str(e)}, 500)

    return ({"status": "sin datos", "mensaje": "No se encontraron actividades"}, 200)