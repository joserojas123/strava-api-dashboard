import requests
import functions_framework
from pprint import pprint
from google.cloud import bigquery

# Configuración manual para la prueba
ACCESS_TOKEN = "ca672781dc902b2cf9df231f24f464b51359ab8e"
TABLE_ID = "strava-api-dashboard.traceflow_dataset.activities"

@functions_framework.http
def get_strava_activities(request):
    """
    Función que recorre todas las páginas de la API de Strava
    para extraer el historial completo de actividades.
    """
    client = bigquery.Client()
    all_activities = []
    page = 1
    per_page = 200  # Máximo permitido por Strava para ser eficiente

    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

    print("Iniciando extracción masiva de actividades...")

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
            truncate_query = f"TRUNCATE TABLE `{TABLE_ID}`"
            query_job = client.query(truncate_query)
            query_job.result()  # Esperar a que termine de borrar
            print(f"Tabla {TABLE_ID} truncada con éxito.")

            # Inserción de los nuevos datos
            errors = client.insert_rows_json(TABLE_ID, actividades_limpias)

            if not errors:
                return ({"status": "éxito", "filas_insertadas": len(actividades_limpias)}, 200)
            else:
                return ({"status": "error_insercion", "errores_bq": errors}, 500)

        except Exception as e:
            return ({"status": "error_bq", "detalle": str(e)}, 500)

    return ({"status": "sin datos", "mensaje": "No se encontraron actividades"}, 200)