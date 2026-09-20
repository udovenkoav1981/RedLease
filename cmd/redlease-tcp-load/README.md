# redlease-tcp-load

Минимальный генератор нагрузки для одного raw TCP-соединения. Он напрямую
отправляет непрерывные пары `Acquire`/`Release`, не используя пакеты `client`
и `client1of1`.

Сначала запустите `redlease-server`, затем:

```bash
go run ./cmd/redlease-tcp-load \
  -target 127.0.0.1:50051 \
  -duration 10s \
  -warmup 1s \
  -keys 1024
```

Клиент сам ждёт завершения restart quarantine. Результат содержит число
завершённых пар `Acquire`/`Release` в секунду и число принятых response messages
в секунду.
