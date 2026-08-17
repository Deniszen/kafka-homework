# Домашнее задание №4 подсчёт количества событий с одинаковым key в рамках сессии 5 минут

Приложение читает топик `events`, группирует по ключу и считает события 5 минут

Топология: `events` → `groupByKey` → `SessionWindows(5 минут)` → `count` → session store `events-session-store`.

## 1. Запустить Kafka

```bash
docker compose -f hw4/kafka/docker-compose.yml up -d
```

## 2. Создать топик events

Топик создаётся автоматически при старте приложения

## 3. Запустить приложение

Каждое обновление сессии пишется в лог:

```
key=user1 session=[2026-08-17T15:41:27.947Z .. 2026-08-17T15:41:27.958Z] count=3
```

## 4. Отправить сообщения console producer


```bash
docker exec -it kafka1-otuskafka kafka-console-producer --bootstrap-server localhost:9191 --topic events --property parse.key=true --property key.separator=:
```

Далее вводить строки вида:

```
user1:login
user1:click
user2:login
user1:logout
```

## 5. Проверить результат

```bash
curl "http://localhost:8080/count?key=user1"
```
