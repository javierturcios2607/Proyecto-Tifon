provider "google" {
    project = var.gcp_proyecto_id
    region = var.gcp_region
  
}

# --- creamos el embudo de datos cloud pub/sub
resource "google_pubsub_topic" "ad_events" {
    name = "ad-events-topic"
    project = var.gcp_proyecto_id
    #depends_on = [google_project_service.pubsub_api] ----- dependeria de esto , pero ya tengo habilitada la api de pubsub en mi proyecto
  
}
# --- NUEVO BLOQUE: La Suscripción para Dataflow ---
resource "google_pubsub_subscription" "dataflow_subscription" {
  name    = "ad-events-dataflow-subscription"
  topic   = google_pubsub_topic.ad_events.name # Lo conectamos al tópico
  project = var.gcp_proyecto_id

  # Configuración de Retención de Mensajes
  # Le decimos a Pub/Sub que guarde los mensajes no leídos hasta 7 días.
  message_retention_duration = "604800s" 

  # Le damos 5 minutos a nuestro consumidor para procesar un mensaje
  # antes de que Pub/Sub lo vuelva a entregar.
  ack_deadline_seconds = 300

  # Nos aseguramos de que se cree DESPUÉS de que el tópico exista.
  depends_on = [google_pubsub_topic.ad_events]
}


# --- . La Bestia de Baja Latencia (Bigtable) ---
resource "google_bigtable_instance" "tifon_instance" {
  name         = "tifon-bigtable"
  display_name = "Tifon Bigtable Instance"
  project      = var.gcp_proyecto_id
  
  # --- BLOQUE DE CLUSTER AÑADIDO ---
  # Aquí definimos dónde y cómo se almacenarán nuestros datos.
  cluster {
    cluster_id   = "tifon-cluster-us-central1"
    zone         = "${var.gcp_region}-a" # Usamos una zona dentro de nuestra región
    num_nodes    = 1                      # 1 nodo es ideal para desarrollo (equivale a "DEVELOPMENT")
    storage_type = "SSD"                  # Usamos almacenamiento SSD para baja latencia
  }
  
  # Eliminamos la línea 'instance_type' que está deprecada.
  
 # depends_on   = [google_project_service.bigtable_api]
}
resource "google_bigtable_table" "user_events_table" {
  name          = "user-events"
  instance_name = google_bigtable_instance.tifon_instance.name
  project       = var.gcp_proyecto_id

  column_family {
    family = "event_data"
  }
  column_family {
    family = "profile_agg"
  }
}
# --- El Almacén Analítico (BigQuery) ---

# Primero, creamos el nuevo dataset dedicado para el Proyecto Tifón
resource "google_bigquery_dataset" "tifon_dataset" {
  dataset_id = "tifon_analytics_dataset"
  project    = var.gcp_proyecto_id
  location   = var.gcp_region # Es buena práctica especificar la ubicación
  
  description = "Dataset para el almacenamiento de datos analíticos del Proyecto Tifón."
  
  #depends_on = [google_project_service.bigquery_api]
}

# Ahora, creamos la tabla DENTRO del nuevo dataset
resource "google_bigquery_table" "ad_events_historical" {
  # Apuntamos al nuevo dataset que acabamos de definir
  dataset_id = google_bigquery_dataset.tifon_dataset.dataset_id 
  table_id   = "ad_events_historical"
  project    = var.gcp_proyecto_id
  
  schema = <<EOF
[
  {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
  {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "revenue", "type": "FLOAT", "mode": "NULLABLE"}
]
EOF

  # Nos aseguramos de que la tabla se cree DESPUÉS de que el dataset exista
  depends_on = [google_bigquery_dataset.tifon_dataset]
}


