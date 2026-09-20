# RedLease — a 1-RTT quorum-based distributed lease protocol inspired by Redlock

RedLease предоставляет распределённые краткоживущие блокировки ресурсов между
узлами кластера с приоритетом на минимальную latency (1 RTT).

Сервис является распределённым и отказоустойчивым к выходу из строя узлов в
пределах выбранной конфигурации quorum: `1/1`, `2/3` или `3/5`. Quorum собирается
на клиенте без Raft/Paxos, а все серверные данные хранятся только в RAM.
Wire protocol использует size-prefixed FlatBuffers messages поверх постоянных
TCP-соединений.

Client и server поддерживаются только на Linux; для отсчёта lease используется
монотонный suspend-aware clock `CLOCK_BOOTTIME`.

## Установка

Для использования универсального client library:

```bash
go get github.com/udovenkoav1981/RedLease/client@latest
```

Для специализированного клиента с фиксированной топологией `1/1`:

```bash
go get github.com/udovenkoav1981/RedLease/client1of1@latest
```

Для встраивания server и его Prometheus collector:

```bash
go get github.com/udovenkoav1981/RedLease/server@latest
go get github.com/udovenkoav1981/RedLease/server/prometheus@latest
```

Для установки тестового server launcher:

```bash
go install github.com/udovenkoav1981/RedLease/cmd/redlease-server@latest
```

## Быстрый запуск сервера

Готовый launcher предназначен для локального тестирования. Он запускает один
lock-server без TLS и аутентификации:

```bash
redlease-server \
  -listen 127.0.0.1:50051 \
  -metrics-listen 127.0.0.1:9090 \
  -configured-max-ttl-ms 5000 \
  -max-keys 10000
```

Prometheus exporter после этого доступен на
`http://127.0.0.1:9090/metrics`:

Если `-metrics-listen` не задан, HTTP endpoint не запускается.

Все параметры доступны через `redlease-server -h`. Для quorum `2/3` или `3/5`
нужно запустить соответственно три или пять независимых процессов на разных
адресах и передать все адреса клиенту.

После обычного старта сервер находится в restart quarantine примерно 5,1
секунды и в это время отклоняет `Acquire`, `Renew` и `Release`. Встроенное
приложение может пропустить встроенный таймер через `SkipRestartQuarantine`, но
тогда именно оно обязано выдержать `server.RestartQuarantineDuration` после
возможной потери прежнего состояния RAM.

Launcher создаёт отдельный Prometheus registry для каждого процесса. Для
production-применения server можно встроить в приложение, которое отвечает за
сетевую изоляцию, аутентификацию, lifecycle и способ публикации метрик.

## Встроенный server и Prometheus exporter

Пакет `server/prometheus` предоставляет collector, но не регистрирует его в
глобальном registry и не запускает HTTP-сервер. Ключевые фрагменты встраивания выглядят так.

Создание server и запуск TCP accept loop:

```go
leaseServer, err := redleaseserver.New(redleaseserver.Config{
	MaxTTL:  5000,
	MaxKeys: 10000,
	Logger:  logger,
})

listener, err := net.Listen("tcp", "127.0.0.1:50051")
go leaseServer.Serve(listener) // ошибку Serve нужно обработать в приложении
```

Регистрация collector в отдельном registry и публикация `/metrics`:

```go
collector := redleaseprometheus.NewCollector(leaseServer)
registry := prometheus.NewRegistry()
registry.Register(collector)


metricsMux := http.NewServeMux()
metricsMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
metricsServer := &http.Server{
	Addr:              "127.0.0.1:9090",
	Handler:           metricsMux,
	ReadHeaderTimeout: 5 * time.Second,
}
go metricsServer.ListenAndServe() // обработку ошибки нужно добавить в приложении
```

Владелец embedded server также должен обслуживать `leaseServer.Fatal()` и при
остановке закрывать `leaseServer` и HTTP server. `leaseServer.Close()` также
закрывает переданный ему listener и активные TCP-соединения.

## Запуск клиента

Client является библиотекой и запускается внутри прикладного процесса.

Package `client` поддерживает все конфигурации `1/1`, `2/3` и `3/5`. Package
`client1of1` — отдельная упрощённая реализация только для одного lock-server;
в её `Config` вместо `Quorum` и списка `Servers` задаётся один `Target`.

Client должен быть долгоживущим объектом приложения. `Logger` задаётся явно.
Специализированный client `1/1` настраивается так:

```go
client, err := client1of1.New(client1of1.Config{
	ClientID:        1,
	Target:          "127.0.0.1:50051",
	Logger:          logger,
	ResponseTimeout: 1000,
})

client.WaitReady(startupCtx)

lease, err := client.Acquire(ctx, 42, 3000)
defer lease.Release()

// Проверка непосредственно перед запуском защищённой операции.
if lease.RemainingTTLms() > 0 {
	protectedOperation(ctx)
}
```

`key` имеет тип `uint64`; стабильное сопоставление прикладных ресурсов с
числовыми ключами выполняет вызывающее приложение.

Для `Quorum2Of3` и `Quorum3Of5` используется универсальный package `client` со
списком из трёх или пяти `Servers`.

`WaitReady` проверяет наличие достаточного числа подключённых TCP-соединений, но не
завершение server quarantine. `Acquire` выполняет одну попытку и при неудаче
сам запускает cleanup частично приобретённых locks; retry и randomized backoff
остаются ответственностью вызывающего приложения. Для долгой защищённой
операции вызывайте `Lease.Renew` до истечения текущего quorum и контролируйте
`Lease.RemainingTTLms()` в обоих клиентах.

`Lease.Release()` немедленно делает локальный lease невалидным и выполняет
сетевое освобождение асинхронно. `client1of1` однократно пытается поставить
`Release` в send queue и не повторяет его после обрыва соединения; не дошедший до
server lease ограничен TTL. Не создавайте и не закрывайте `Client` для каждой
операции: закрытие клиента останавливает его фоновые соединения, а у универсального
клиента также healing и retry. Закрывайте `Client` один раз при остановке
приложения.

## Документация проекта

- [Requirements](Requirements.md) — требования к системе.
- [Architecture](Architecture.md) — протокол и архитектурные решения.
- [Нагрузочный тест](cmd/redlease-load/README.md) — матрица коротких lease для `client1of1` и общего `client` на отдельно запущенных серверах.
- [Benchmark client1of1](client1of1/README.md) — throughput и latency одного клиента через внешний TCP server.
- [MIT License](LICENSE).
