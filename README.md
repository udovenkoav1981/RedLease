# RedLease — a 1-RTT quorum-based distributed lease protocol inspired by Redlock

RedLease предоставляет распределённые краткоживущие блокировки ресурсов между
узлами кластера с приоритетом на минимальную latency (1 RTT).

Сервис является распределённым и отказоустойчивым к выходу из строя узлов в
пределах выбранной конфигурации quorum: `1/1`, `2/3` или `3/5`. Quorum собирается
на клиенте без Raft/Paxos, а все серверные данные хранятся только в RAM.

Client и server поддерживаются только на Linux; для отсчёта lease используется
монотонный suspend-aware clock `CLOCK_BOOTTIME`.

## Установка

Для использования client library:

```bash
go get github.com/udovenkoav1981/RedLease/client@latest
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
TLS, аутентификацию, lifecycle и способ публикации метрик.

## Встроенный server и Prometheus exporter

Пакет `server/prometheus` предоставляет collector, но не регистрирует его в
глобальном registry и не запускает HTTP-сервер. Ключевые фрагменты встраивания выглядят так.

Создание server и регистрация gRPC service:

```go
leaseServer, err := redleaseserver.New(redleaseserver.Config{
	MaxTTL:  5000,
	MaxKeys: 10000,
	Logger:  logger,
})

grpcServer := grpc.NewServer() // add transport credentials in production
leaseServer.Register(grpcServer)
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
остановке закрывать `leaseServer`, gRPC server и HTTP server.

## Запуск клиента

Client является библиотекой и запускается внутри прикладного процесса.

`client.Client` должен быть долгоживущим объектом приложения: он поддерживает
по одному reconnecting stream к каждому server. `Logger` и transport credentials
задаются явно. Для локального plaintext server конфигурация `1/1` выглядит так:

```go
client, err := redleaseclient.New(redleaseclient.Config{
	ClientID: 1, // unique among simultaneously running client processes
	Quorum:   redleaseclient.Quorum1Of1,
	Servers: []redleaseclient.ServerConfig{
		{
			Target: "127.0.0.1:50051",
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		},
	},
	Logger:          logger,
	ResponseTimeout: 1000, // milliseconds per server response
})

client.WaitReady(startupCtx)

lease, err := client.Acquire(ctx, []byte("resource/42"), 3000)
defer lease.Release()

// Проверка непосредственно перед запуском защищённой операции.
if lease.Valid() {
	protectedOperation(ctx)
}
```

При `Quorum2Of3` список `Servers` должен содержать ровно три адреса, а при
`Quorum3Of5` — ровно пять. В production вместо `insecure.NewCredentials()`
нужно передать подходящие TLS credentials.

`WaitReady` проверяет наличие достаточного числа подключённых streams, но не
завершение server quarantine. `Acquire` выполняет одну попытку и при неудаче
сам запускает cleanup частично приобретённых locks; retry и randomized backoff
остаются ответственностью вызывающего приложения. Для долгой защищённой
операции вызывайте `Lease.Renew` до истечения текущего quorum и контролируйте
`Lease.Valid()` или `Lease.RemainingTTL()`.

`Lease.Release()` немедленно делает локальный lease невалидным и выполняет
сетевое освобождение асинхронно. Не создавайте и не закрывайте `Client` для
каждой операции: закрытие клиента останавливает его фоновые streams и retry.
Закрывайте `Client` один раз при остановке приложения.

## Документация проекта

- [Requirements](Requirements.md) — требования к системе.
- [Architecture](Architecture.md) — протокол и архитектурные решения.
- [MIT License](LICENSE).
