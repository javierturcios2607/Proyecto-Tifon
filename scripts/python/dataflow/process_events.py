import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud.bigtable import row
# --- IMPORT CORREGIDO ---
# Importamos la herramienta de escritura de Bigtable desde su ubicación específica.
from apache_beam.io.gcp.bigtableio import WriteToBigTable
import datetime
import json
import logging
import time

# --- CONFIGURACIÓN DEL PIPELINE ---
PROJECT_ID = "riverajavier-dev"
REGION = "us-central1"
PUBSUB_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/ad-events-dataflow-subscription"
BIGQUERY_TABLE = f"{PROJECT_ID}:tifon_analytics_dataset.ad_events_historical"
BIGTABLE_INSTANCE = "tifon-bigtable"
BIGTABLE_TABLE = "user-events"
TEMP_GCS_BUCKET = f'gs://data-lake-fenix-riverajavier/temp'

# --- TRANSFORMACIONES (DoFn) ---

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            yield json.loads(element.decode('utf-8'))
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar JSON: {e} - Mensaje corrupto: {element}")

class FormatForBigQuery(beam.DoFn):
    def process(self, element):
        yield {
            "user_id": element.get("user_id"),
            "event_type": element.get("event_type"),
            "product_id": element.get("product_id"),
            "event_timestamp": element.get("event_timestamp"),
            "revenue": element.get("revenue")
        }

class FormatForBigtable(beam.DoFn):
    MAX_TIMESTAMP_MS = int(datetime.datetime(3000, 1, 1).timestamp() * 1000)

    def process(self, element):
        try:
            user_id = element.get("user_id")
            event_timestamp_ms = int(element.get("event_timestamp") * 1000)
            inverted_timestamp = self.MAX_TIMESTAMP_MS - event_timestamp_ms
            row_key = f"{user_id}#{inverted_timestamp}".encode('utf-8')
            direct_row = row.DirectRow(row_key=row_key)
            current_time = datetime.datetime.utcnow()
            
            direct_row.set_cell('event_data', 'event_type'.encode('utf-8'), str(element.get('event_type', '')).encode('utf-8'), timestamp=current_time)
            direct_row.set_cell('event_data', 'product_id'.encode('utf-8'), str(element.get('product_id', '')).encode('utf-8'), timestamp=current_time)
            direct_row.set_cell('event_data', 'revenue'.encode('utf-8'), str(element.get('revenue', 0.0)).encode('utf-8'), timestamp=current_time)
            
            yield direct_row
        except Exception as e:
            logging.error(f"Error al formatear para Bigtable: {e} - Elemento: {element}")

# --- EL PIPELINE PRINCIPAL ---

def run(pipeline_options):
    with beam.Pipeline(options=pipeline_options) as p:
        logging.info("🚀 Iniciando el pipeline de doble escritura 'Tifón'...")

        messages = p | "Leer de Pub/Sub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        events = messages | "Parsear Evento JSON" >> beam.ParDo(ParseEvent())
        
        # RAMA 1: BigQuery (Cold Path)
        (events | "Formatear para BigQuery" >> beam.ParDo(FormatForBigQuery())
                | "Escribir en BigQuery" >> beam.io.WriteToBigQuery(
                    table=BIGQUERY_TABLE,
                    schema='user_id:STRING,event_type:STRING,product_id:STRING,event_timestamp:TIMESTAMP,revenue:FLOAT',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
        
        # RAMA 2: Bigtable (Hot Path)
        (events | "Formatear para Bigtable" >> beam.ParDo(FormatForBigtable())
                # --- LLAMADA CORREGIDA ---
                # Usamos la clase 'WriteToBigTable' que importamos directamente.
                | "Escribir en Bigtable" >> WriteToBigTable(
                    project_id=PROJECT_ID,
                    instance_id=BIGTABLE_INSTANCE,
                    table_id=BIGTABLE_TABLE))

        logging.info("✅ Pipeline de doble camino definido. Desplegando en Dataflow...")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    job_name = f'tifon-pipeline-dual-path-{int(time.time())}'
    options = PipelineOptions(
        streaming=True, runner='DataflowRunner', project=PROJECT_ID, region=REGION,
        temp_location=TEMP_GCS_BUCKET, job_name=job_name)
    run(options)