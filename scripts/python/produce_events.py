import json
import time
import random
import uuid
from concurrent import futures
from google.cloud import pubsub_v1

#--- configuracion

PROJECT_ID = "riverajavier-dev"
TOPIC_NAME = "ad-events-topic"

 #--- FINAL DE LA CONFIGURACION


 # Construyo la ruta completa del tópico
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"

#Configuración del cliente de Pub/Sub para enviar en lotes
# Enviará mensajes cuando se acumulen 100 mensajes, o pase 1 segundo.
batch_settings = pubsub_v1.types.BatchSettings( #https://docs.cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.types.BatchSettings
    max_messages=100,
    max_bytes=1024 * 10, # 10 KB
    max_latency=1, # 1 segundo
)
publisher = pubsub_v1.PublisherClient(batch_settings)
publish_futures = []

def get_callback(publish_future, data):#https://docs.cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.publisher.futures.Future#methods
    """ Callback para manejar el resultado de la publicación de un lote. """
    def callback(result): #https://docs.cloud.google.com/pubsub/docs/publish-receive-messages-client-library#publish_messages
        # result() puede lanzar una excepción si la publicación falló.
        try:
            # El resultado es el ID del mensaje publicado.
            print(f" Mensaje {result} publicado con éxito.")
            publish_future.set_result(result)
        except Exception as e:
            print(f" Falló la publicación del mensaje con datos {data}: {e}")
            publish_future.set_exception(e)
    return callback
#https://github.com/googleapis/python-pubsub repositorio de documentation

def generate_ad_event():
    """ Genera un evento de anuncio ficticio. """
    event_types = ["impression", "click", "conversion"]
    product_ids = ["PROD-A", "PROD-B", "PROD-C", "PROD-D", "PROD-E"]
    user_id = f"user_{random.randint(100, 999)}"
    event_type = random.choices(event_types, weights=[0.8, 0.15, 0.05], k=1)[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "product_id": random.choice(product_ids),
        "event_timestamp": time.time(),
        "revenue": round(random.uniform(0.01, 1.50), 4) if event_type == 'click' else 0.0
    }
    return event

if __name__ == "__main__":
    print(f" Desatando el viento de eventos hacia el tópico: {TOPIC_NAME}")
    print("Presiona Ctrl+C para detener.")

    try:
        num_events = 0
        while True:
            event_data = generate_ad_event()
            data = json.dumps(event_data).encode("utf-8")

            # El método publish() es asíncrono y devuelve un "future".
            publish_future = publisher.publish(topic_path, data)

            # Adjuntamos nuestro callback para saber qué pasó.
            publish_future.add_done_callback(get_callback(publish_future, data))
            publish_futures.append(publish_future)

            num_events += 1
            print(f"  -> Evento #{num_events} encolado para {event_data['user_id']} ({event_data['event_type']})")

            # Pequeña pausa para no saturar la CPU
            time.sleep(0.05) 

    except KeyboardInterrupt:
        print("\n Deteniendo el productor. Esperando a que se publiquen los lotes finales...")

    finally:
        # Asegura que todos los mensajes encolados se envíen antes de salir.
        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        print(" Productor detenido. Todos los mensajes han sido enviados.")