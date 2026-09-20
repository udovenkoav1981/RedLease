# Benchmark `client1of1`

`BenchmarkClient1Of1AcquireRelease` измеряет публичный API `client1of1` через
настоящее TCP-соединение с отдельно запущенным RedLease server. Benchmark
не запускает встроенный server.

Сначала запустите server в отдельном терминале:

```bash
go run ./cmd/redlease-server -listen=127.0.0.1:50051
```

Затем из корня репозитория запустите benchmark:

```bash
REDLEASE_BENCH_TARGET=127.0.0.1:50051 \
go test ./client1of1 \
  -run '^$' \
  -bench '^BenchmarkClient1Of1AcquireRelease$' \
  -benchmem \
  -benchtime=3s \
  -count=3
```

Если `REDLEASE_BENCH_TARGET` не задан, используется `127.0.0.1:50051`.
Benchmark сам ждёт готовности connection и выхода server из restart quarantine.
Один `client1of1.Client` и одно TCP-соединение совместно используются 1, 2, 4,
8, 16, 32, 64, 128 и 256 параллельными workers.

В результате выводятся:

- `acquire-release-pairs/s` — суммарная пропускная способность;
- `avg-latency-ms` — средняя видимая вызывающему коду задержка пары;
- `p50-latency-ms`, `p95-latency-ms`, `p99-latency-ms` — приблизительные
  перцентили задержки;
- `B/op`, `allocs/op` — клиентские и FlatBuffers/TCP-аллокации на пару.

Одна измеряемая пара — синхронный `Acquire` и вызов асинхронного `Release`.
Поэтому latency включает ожидание ответа `Acquire` и постановку `Release` в
send queue, но не ожидание ответа `Release`: публичный API его не ожидает.
Следующий `Acquire` того же worker использует тот же key и следует за предыдущим
`Release` в одном соединении и одном server shard.

Стандартный `ns/op` при нескольких workers равен измеренному времени,
делённому на общее число пар. Это обратная суммарная пропускная способность, а
не latency отдельного вызова; для задержки используйте отдельные latency-метрики.
