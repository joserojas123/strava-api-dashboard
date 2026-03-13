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

# Función que obtiene un nuevo token de acceso usando el refresh token
def get_new_access_token():
    """
    Obtiene un nuevo access_token de la API de Strava usando el refresh_token.
    
    Esta función realiza una solicitud POST a los servidores de Strava con las
    credenciales de la aplicación (CLIENT_ID y CLIENT_SECRET) junto con el
    REFRESH_TOKEN para intercambiarlo por un access_token válido.
    
    Los access tokens de Strava tienen una validez limitada (aproximadamente 6 horas),
    por lo que es necesario renovarlos regularmente usando el refresh_token que no expira.
    
    Returns:
        str: El access_token válido para usar en las peticiones autenticadas a la API de Strava
        
    Raises:
        RequestException: Si la solicitud HTTP falla o la API devuelve un error
        KeyError: Si la respuesta no contiene el campo 'access_token'
    """
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

# Función que obtiene las actividades de Strava y las inserta en BigQuery
@functions_framework.http
def get_strava_activities(request):
    """
    Función principal que extrae todas las actividades del atleta desde la API de Strava,
    las transforma y las carga en BigQuery de manera completa.
    
    Proceso:
    1. Obtiene un nuevo access_token dinámico usando el refresh_token
    2. Realiza solicitudes paginadas a la API de Strava para extraer todas las actividades
    3. Transforma y limpia los datos recibidos en el formato esperado por BigQuery
    4. Trunca la tabla existente en BigQuery para evitar duplicados
    5. Inserta los datos frescos en la tabla
    
    Parameters:
        request: Objeto HTTP request de Flask/Google Cloud Functions (no se usa en esta implementación)
    
    Returns:
        tuple: Una tupla (response_dict, status_code) donde:
            - response_dict es un diccionario con el resultado (éxito, error, cantidad de filas)
            - status_code es el código HTTP (200 para éxito, 500 para errores)
            
    Ejemplo de respuesta exitosa:
        {"status": "éxito", "filas_insertadas": 245}, 200
        
    Ejemplo de respuesta con error:
        {"error": "Falló la renovación del token", "details": "..."}, 500
    """
    print("Iniciando renovación de token...")
    # --- 1. Obtener Token Dinámico ---
    # Se obtiene un nuevo access_token de Strava. Si falla, la función retorna
    # un error 500 sin intentar continuar. El token es necesario para autenticarse
    # en todas las solicitudes posteriores a la API.
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

    # --- 2. Extracción de datos paginada ---
    # Strava limita las respuestas a 200 actividades por página. Iteramos sobre
    # todas las páginas hasta obtener una respuesta vacía o una página con menos
    # de 200 resultados. Esto indica que hemos llegado al final del historial.
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

    # --- 2.5. Transformación y limpieza de datos ---
    # Extraemos solo los campos necesarios de cada actividad y convertimos
    # los tipos de datos al formato esperado por BigQuery. Se utiliza .get()
    # con valores por defecto para evitar KeyErrors si falta algún campo.
    actividades_limpias = []
    for act in all_activities:
        fila = {
            # Identificadores únicos
            # 'id': ID único de la actividad en Strava
            # 'athlete_id': ID del atleta propietario de la actividad
            # 'name': Nombre/descripción de la actividad (ej: "Morning Run")
            "id": act.get("id"),
            "athlete_id": act.get("athlete", {}).get("id"),
            "name": act.get("name"),

            # Métricas de rendimiento (convertidas a tipos numéricos)
            # 'distance': Distancia en metros
            # 'moving_time': Tiempo en movimiento en segundos
            # 'elapsed_time': Tiempo total incluyendo paradas en segundos
            # 'average_speed': Velocidad promedio en m/s
            # 'max_speed': Velocidad máxima alcanzada en m/s
            # 'total_elevation_gain': Ganancia de elevación acumulada en metros
            "distance": float(act.get("distance", 0)),
            "moving_time": int(act.get("moving_time", 0)),
            "elapsed_time": int(act.get("elapsed_time", 0)),
            "average_speed": float(act.get("average_speed", 0)),
            "max_speed": float(act.get("max_speed", 0)),
            "total_elevation_gain": float(act.get("total_elevation_gain", 0)),

            # Timestamps en formato ISO 8601
            # 'start_date': Fecha y hora UTC de inicio
            # 'start_date_local': Fecha y hora en zona horaria local del atleta
            # 'utc_offset': Desfase en segundos respecto a UTC (ej: -18000 para EST)
            "start_date": act.get("start_date"),
            "start_date_local": act.get("start_date_local"),
            "utc_offset": act.get("utc_offset"),

            # Información de ubicación
            # Estos campos pueden ser null si la actividad no incluye datos de ubicación
            "location_city": act.get("location_city"),
            "location_state": act.get("location_state"),
            "location_country": act.get("location_country"),
            "device_name": act.get("device_name"),

            # Clasificación de la actividad
            # 'type': Tipo general (Run, Ride, Swim, etc.)
            # 'sport_type': Subtipo más específico (Trail Run, Mountain Bike, etc.)
            "type": act.get("type"),
            "sport_type": act.get("sport_type")
        }
        actividades_limpias.append(fila)

    # --- 3. Inserción en BigQuery ---
    # Antes de insertar los nuevos datos, truncamos (borramos) el contenido anterior
    # de la tabla. Esto asegura que la tabla siempre contiene el estado más reciente
    # del historial de actividades sin duplicados.

    if actividades_limpias:
        try:
            # Truncado mediante una query DDL (Data Definition Language)
            # TRUNCATE es más eficiente que DELETE para borrar todos los datos.
            print("Iniciando truncado de tabla...")
            truncate_query = f"TRUNCATE TABLE `{TABLE_ID}`"
            query_job = client.query(truncate_query)
            query_job.result()  # Esperar a que termine la operación de borrado
            print(f"Tabla {TABLE_ID} truncada con éxito.")

            # Inserción de los nuevos datos limpios y transformados
            # insert_rows_json es más eficiente que append_rows para múltiples registros
            print("Iniciando inserción en Bigquery...")
            errors = client.insert_rows_json(TABLE_ID, actividades_limpias)

            if not errors:
                return ({"status": "éxito", "filas_insertadas": len(actividades_limpias)}, 200)
            else:
                return ({"status": "error_insercion", "errores_bq": errors}, 500)

        except Exception as e:
            return ({"status": "error_bq", "detalle": str(e)}, 500)

    return ({"status": "sin datos", "mensaje": "No se encontraron actividades"}, 200)