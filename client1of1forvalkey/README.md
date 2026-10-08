# `client1of1forvalkey`

`client1of1forvalkey` предоставляет тот же пользовательский API, что и
`client1of1`, но хранит lease в одном standalone Valkey server.

```go
client, err := client1of1forvalkey.New(client1of1forvalkey.Config{
    ClientID: 1,
    Target:   "127.0.0.1:6379",
    Logger:   logger,
})
if err != nil {
    return err
}
defer client.Close()

if err := client.WaitReady(ctx); err != nil {
    return err
}

lease, err := client.Acquire(ctx, resourceKey, 1_000)
if err != nil {
    return err
}
defer lease.Release()
```

Ключи Valkey имеют бинарный вид `redlease:` + big-endian `uint64`. Значением
является уникальный 16-байтный ownership token. `Renew` и `Release` выполняются
Lua-скриптами с проверкой token, поэтому запоздавшая операция не может изменить
или удалить lease нового владельца.

`Release()` асинхронный и best-effort, как в `client1of1`. Операции одного
`Lease` нельзя вызывать конкурентно.

Все Acquire, Renew и Release попадают в один ordered `go-redis`
`AsyncAutoPipeline` с `FullDuplex`: отдельные writer и reader работают через
одно TCP-соединение, не ожидая завершения всей пачки. Поэтому Release и
следующий Acquire одного key сохраняют порядок, хотя `Release()` не ждёт
ответа. Окно full-duplex потока ограничено 4096 командами и при заполнении
применяет backpressure.

Lua-скрипты загружаются через `SCRIPT LOAD`, а рабочий поток использует
`EVALSHA`. Если Acquire или Renew получает `NOSCRIPT`, скрипты загружаются
снова, но неоднозначная lease-операция автоматически не повторяется. Release
остаётся best-effort, и его ответ не ожидается. Сетевые retry в `go-redis`
отключены по той же причине. Используемый API `AsyncAutoPipeline`
экспериментальный, поэтому версия `go-redis` зафиксирована в `go.mod`.

Valkey должен использовать `maxmemory-policy noeviction`: вытеснение активного
ключа позволило бы другому клиенту захватить тот же ресурс до истечения lease.

Этот пакет сохраняет пользовательский API, но обычный Valkey не реализует
server-side restart quarantine RedLease. Если после restart Valkey потеряет
lease-ключи, он сможет немедленно принять новые Acquire. Прикладная
инфраструктура должна либо гарантировать сохранность данных, либо не допускать
сервер к обслуживанию дольше максимального TTL после старта.

Интеграционный тест запускается против внешнего Valkey:

```bash
REDLEASE_VALKEY_TEST_TARGET=127.0.0.1:6379 \
go test ./client1of1forvalkey -run TestValkeyEndToEnd -count=1
```

Benchmark публичного API использует один client, одно TCP-соединение и
отдельный key на каждого worker. Как и benchmark `client1of1`, он вызывает
`Release()` сразу после `Acquire()` без искусственного удержания lease:

```bash
go test ./client1of1forvalkey \
  -run '^$' \
  -bench '^BenchmarkClient1Of1AcquireRelease/workers=2048$' \
  -benchtime=15s \
  -count=1 \
  -benchmem
```

Адрес по умолчанию — `127.0.0.1:6379`; другой адрес задаётся через
`REDLEASE_VALKEY_BENCH_TARGET`.
