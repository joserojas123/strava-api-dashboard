import requests
import functions_framework
from pprint import pprint

# Configuración manual para la prueba
ACCESS_TOKEN = "098d653f26e6d56c83e5c0552b53110deaa3d20c"


@functions_framework.http
def get_strava_activities(request):
    # Endpoint de la API de Strava para actividades del atleta
    url = "https://www.strava.com/api/v3/athlete/activities"

    # Cabecera de autorización obligatoria
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    # Parámetros opcionales (ej: traer 5 resultados)
    params = {
        'per_page': 5
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        # Esto se verá en los logs de Google Cloud
        pprint(data)
        return ({"status": "exito", "data": data}, 200)
    else:
        return ({"error": response.text}, response.status_code)

