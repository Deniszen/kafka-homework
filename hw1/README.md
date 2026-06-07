1. Скачал докер образ кафки docker pull apache/kafka:4.3.0
2. Запустил контейнер кафки docker run -p 9092:9092 apache/kafka:4.3.0
3. Зашел в контейнер кафки docker exec -it 4e7e88e54b6d /bin/bash 
4. Перешел в директорию со скриптами cd /opt/kafka/bin
5. Создал топик ./kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
6. Записал сообщения в топик ./kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
7. Прочел сообщения из топика ./kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092