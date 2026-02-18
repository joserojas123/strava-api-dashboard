import requests
import functions_framework
from pprint import pprint
from google.cloud import bigquery

# Configuración manual para la prueba
ACCESS_TOKEN = "db7c8b4090d2d886a1df839b917ed6f0c36783b1"
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
            "athlete_id": act.get("athlete", {}).get("id"),
            "average_speed": act.get("average_speed"),
            "distance": act.get("distance"),
            "elapsed_time": act.get("elapsed_time"),
            "location_city": act.get("location_city"),
            "location_country": act.get("location_country"),
            "location_state": act.get("location_state"),
            "max_speed": act.get("max_speed"),
            "moving_time": act.get("moving_time"),
            "name": act.get("name"),
            "sport_type": act.get("sport_type"),
            "start_date_local": act.get("start_date_local")
        }
        actividades_limpias.append(fila)

    # 3. Inserción en BigQuery
    if actividades_limpias:
        errors = client.insert_rows_json(TABLE_ID, actividades_limpias)
        if errors == []:
            return ({"status": "éxito", "filas_insertadas": len(actividades_limpias)}, 200)
        else:
            return ({"status": "error", "errores_bq": errors}, 500)

    return ({"status": "sin datos", "mensage": "No se encontraron actividades nuevas"}, 200)