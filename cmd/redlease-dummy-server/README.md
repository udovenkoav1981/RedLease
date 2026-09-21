# redlease-dummy-server

Stateless TCP peer для оценки производительности клиента. Принимает протокольные
FlatBuffers requests и возвращает корректные responses с тем же `request_id`.
Для `Acquire` и `Renew` возвращает `OK` и
`min(requested_ttl_ms, max_ttl_ms)`; для `Release` — `OK`; для `GetTTL` —
`max_ttl_ms`. Map, heap, shard queue и контроля владения здесь нет.

```bash
go run ./cmd/redlease-dummy-server -listen 127.0.0.1:50052 -max-ttl-ms 5000
```

В другой консоли:

```bash
REDLEASE_BENCH_TARGET=127.0.0.1:50052 go test ./client1of1 \
  -run '^$' -bench '^BenchmarkClient1Of1AcquireRelease/workers=256$' \
  -benchtime=5s -count=1
```

Чтобы изолированно исследовать сервер, запустите настоящий `redlease-server`
вместо этой заглушки и отправляйте нагрузку через `redlease-dummy-client`.

**Этот peer не обеспечивает mutual exclusion и никогда не должен использоваться
с реальной бизнес-логикой.**
