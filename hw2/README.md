1. Выполнил команду в контейнере ./opt/kafka/bin/kafka-storage.sh random-uuid для генерации UUID, 
сгенерированный UUID = ZfG4Li0VQcmnZK727aoNcQ
2. Выполнил ./opt/kafka/bin/kafka-storage.sh format --config /opt/kafka/config/server.properties --cluster-id ZfG4Li0VQcmnZK727aoNcQ
3. Запустил бркоер ./opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties брокер запустился
4. Оставновил брокер
5. Добавил конфиги для трех клиентов и запустил брокер выполнил ./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/bob.properties --list ошибок нет
6. Добавил топик  ./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/alice.properties --create --topic test-topic --partitions 1 --replication-factor 1
7. Добавил ACL для пользователей проверил командой ./opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 --command-config /tmp/alice.properties --list
8. Проверка для пользователя alice:
- список топиков - ./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/alice.properties --list
- запись в топик - ./opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --command-config /tmp/alice.properties --topic test-topic
- чтение из топика - ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config /tmp/alice.properties --topic test-topic --from-beginning
9. Проверка для пользователя bob:
- список топиков - ./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/bob.properties --list
- запись в топик - ./opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --command-config /tmp/bob.properties --topic test-topic
- чтение из топика - ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config /tmp/bob.properties --topic test-topic --from-beginning
10. Проверка для пользователя a2:
- список топиков - ./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/a2.properties --list
- запись в топик - ./opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --command-config /tmp/a2.properties --topic test-topic
- чтение из топика - ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config /tmp/a2.properties --topic test-topic --from-beginning