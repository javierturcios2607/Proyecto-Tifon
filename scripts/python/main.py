import functions_framework
from google.cloud import bigtable
import json
import datetime

# --- CONFIGURACIÓN ---
PROJECT_ID = "riverajavier-dev"
INSTANCE_ID = "tifon-bigtable"
TABLE_ID = "user-events"

# --- ¡OPTIMIZACIÓN "MALDITO INGENIERO"! ---
# Instanciamos los clientes FUERA de la función principal.
# Esto permite que la Cloud Function reutilice la conexión en ejecuciones "cálidas",
# reduciendo drásticamente la latencia al evitar crear una nueva conexión en cada llamada.
client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table = instance.table(TABLE_ID)

@functions_framework.http
def get_user_profile(request):
    """
    Función HTTP que recibe un user_id y devuelve sus eventos más recientes desde Bigtable.
    Para probar: /?user_id=user_123
    """
    # Extraer el user_id de los parámetros de la URL
    user_id = request.args.get("user_id")

    if not user_id:
        return ("Por favor, proporciona un 'user_id' como parámetro en la URL (ej: ?user_id=user_123).", 400)

    print(f"🔍 Buscando eventos para el usuario: {user_id}")

    # --- LECTURA RÁPIDA POR PREFIJO ---
    # Gracias a nuestro diseño de row key, esto es increíblemente rápido.
    prefix = f"{user_id}#"

    # Leemos solo los 5 eventos más recientes que coincidan con el prefijo.
    rows = table.read_rows(start_key=prefix.encode('utf-8'), limit=5)

    events = []
    for row in rows:
        event_data = {"row_key": row.row_key.decode('utf-8')}
        # Iteramos sobre las celdas de la fila
        for cf, cols in row.cells.items():
            for col, cells in cols.items():
                event_data[col.decode('utf-8')] = cells[0].value.decode('utf-8')
        events.append(event_data)

    if not events:
        return (f"No se encontraron eventos para el usuario '{user_id}'.", 404)

    # Preparamos la respuesta JSON
    response_data = {
        "user_id": user_id,
        "event_count": len(events),
        "recent_events": events
    }

    # Devolvemos la respuesta como JSON
    headers = {"Content-Type": "application/json"}
    return (json.dumps(response_data, indent=2), 200, headers)