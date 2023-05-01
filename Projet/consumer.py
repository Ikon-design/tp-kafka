from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error

def main():
    # Initialiser le client MinIO

    minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)

    # Vérifier si la bucket existe, sinon le créer

    if not minioClient.bucket_exists('consumer'):
      minioClient.make_bucket('consumer')
    else:
       print('Bucket minIo existe déja')
    
    # Initialiser le consommateur Kafka

    consumer = KafkaConsumer('capteur',
                            bootstrap_servers=['127.0.0.1:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Boucle infinie pour lire les données Kafka
    for obj in consumer:
       data = obj.value
    # Enregistrer les données dans la bucket MinIO
    try:
        # Définir le nom de l'objet
        objName = f"{data['timestamp']}.json"

        # Encodage des données en JSON
        dataJson = json.dump(data).encode("utf-8")

        # Enregistrement des données dans le bucket MinIO
        minioClient.put_object('consumer', objName, dataJson, content_type="application/json")

    except S3Error as e:
        print("Error:", e)


if __name__ == "__main__":
  main()