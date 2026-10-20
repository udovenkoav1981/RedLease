# RedLease — a 1-RTT quorum-based distributed lease protocol inspired by Redlock

RedLease предоставляет распределённые краткоживущие блокировки ресурсов между
узлами кластера с приоритетом на минимальную latency (1 RTT).

Сервис является распределённым и отказоустойчивым к выходу из строя узлов в
пределах выбранной конфигурации quorum: `1/1`, `2/3` или `3/5`. Quorum собирается
на клиенте без Raft/Paxos, а все серверные данные хранятся только в RAM.
Wire protocol использует size-prefixed FlatBuffers messages поверх постоянных
TCP-соединений.

Client и server поддерживаются только на Linux; для отсчёта lease используется
monotonic-компонента `time.Now()` (`CLOCK_MONOTONIC` на Linux). Она защищает
validity от перевода wall clock и NTP step. Linux system suspend не
поддерживается; при live migration гипервизор должен сохранить monotonic clock
и учесть время паузы VM.

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

Для Linux `x86_64` доступна установка из RPM-репозитория:

```bash
sudo curl --fail --silent --show-error \
  https://udovenkoav1981.github.io/RedLease/redlease.repo \
  --output /etc/yum.repos.d/redlease.repo
sudo dnf install redlease-server
```

Для Debian и Ubuntu `amd64` доступен APT-репозиторий:

```bash
sudo curl --fail --silent --show-error \
  https://udovenkoav1981.github.io/RedLease/redlease.list \
  --output /etc/apt/sources.list.d/redlease.list
sudo apt update
sudo apt install redlease-server
```

Репозитории пока не подписаны и публикуются в GitHub Pages при создании
релизного тега. Оба пакета устанавливают статические `redlease-server` и
`redlease-load`, создают системного пользователя `redlease` и устанавливают
`redlease-server.service`, но не запускают его автоматически.
Параметры запуска задаются в `/etc/sysconfig/redlease-server` для RPM и в
`/etc/default/redlease-server` для DEB. По умолчанию RedLease слушает
`0.0.0.0:17379`, а Prometheus exporter — `0.0.0.0:17380`. Ограничение доступа
к обоим портам выполняется firewall или сетевой политикой. Установленный
systemd unit по умолчанию также включает регистрацию через etcd на
`http://127.0.0.1:2379` с префиксом `default`. Перед запуском замените
`-etcd-advertise 127.0.0.1:17379` на конкретный IP:port, доступный клиентам:

```bash
sudo systemctl enable --now redlease-server
```

## Быстрый запуск сервера

Готовый launcher предназначен для локального тестирования. Он запускает один
lock-server без TLS и аутентификации:

```bash
redlease-server \
  -listen 127.0.0.1:17379 \
  -metrics-listen 127.0.0.1:17380 \
  -configured-max-ttl-ms 5000 \
  -max-keys 10000
```

Prometheus exporter после этого доступен на
`http://127.0.0.1:17380/metrics`:

Если `-metrics-listen` не задан, HTTP endpoint не запускается.

Все параметры доступны через `redlease-server -h`. Для quorum `2/3` или `3/5`
нужно запустить соответственно три или пять независимых процессов на разных
адресах и передать все адреса клиенту.

После обычного старта сервер находится в restart quarantine примерно 5,1
секунды и в это время отклоняет `Acquire`, `Renew` и `Release`. Отключить
restart quarantine через API server library нельзя.

Launcher создаёт отдельный Prometheus registry для каждого процесса. Для
production-применения server можно встроить в приложение, которое отвечает за
сетевую изоляцию, аутентификацию, lifecycle и способ публикации метрик.

### Регистрация server через etcd

Без etcd-флагов launcher работает как раньше. Режим регистрации включается
явно:

```bash
redlease-server \
  -listen 0.0.0.0:17379 \
  -etcd-endpoint http://127.0.0.1:2379 \
  -etcd-prefix production \
  -etcd-advertise 192.0.2.10:17379
```

Launcher последовательно пытается занять один из пяти ключей
`production-redlease-server-1` … `production-redlease-server-5`. Ключ создаётся
атомарно только при его отсутствии, содержит переданный IP:port и привязан к
etcd lease с TTL 3 секунды. Lease обновляется раз в секунду. Если все позиции
заняты, процесс следит за удалением всех пяти ключей и повторяет попытку.

RedLease TCP server создаётся только после получения позиции, затем выдерживает
полный restart quarantine и переходит в `ACTIVE`. Потеря etcd lease закрывает
TCP listener, все соединения и текущий экземпляр server вместе с его RAM-only
leases. Процесс не завершается и запускает новый цикл захвата позиции. До нового
захвата TCP endpoint недоступен; после захвата создаётся новый server и полный
restart quarantine отсчитывается заново. Prometheus endpoint продолжает
работать независимо от наличия текущего server. `-etcd-advertise` можно не
указывать, если `-listen` уже содержит конкретный IP:port; для wildcard-address
его следует задавать явно.

## Встроенный server и Prometheus exporter

Пакет `server/prometheus` предоставляет collector, но не регистрирует его в
глобальном registry и не запускает HTTP-сервер. Ключевые фрагменты встраивания выглядят так.

Создание server и запуск TCP accept loop:

```go
listener, err := net.Listen("tcp", "127.0.0.1:17379")
leaseServer, err := redleaseserver.New(listener, redleaseserver.Config{
	MaxTTL:  5000,
	MaxKeys: 10000,
	Logger:  logger,
})
```

Регистрация collector в отдельном registry и публикация `/metrics`:

```go
collector := redleaseprometheus.NewCollector(leaseServer)
registry := prometheus.NewRegistry()
registry.Register(collector)


metricsMux := http.NewServeMux()
metricsMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
metricsServer := &http.Server{
	Addr:              "127.0.0.1:17380",
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
	Target:          "127.0.0.1:17379",
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

Оба client package собирают histogram полной latency вызова `Acquire`, отдельно
для успешных и неуспешных результатов. Snapshot доступен напрямую через
`client.MetricsSnapshot()`. Один общий Prometheus collector работает с обеими
реализациями клиента:

```go
collector, err := clientmetricsprometheus.NewCollector(client)
registry.MustRegister(collector)
```

Collector экспортирует
`redlease_client_acquire_duration_seconds{result="acquired|not_acquired"}` и,
как server collector, самостоятельно не регистрируется и не запускает HTTP
endpoint. Утилита `redlease-load` может опубликовать агрегированную histogram
всех клиентов текущей точки матрицы через флаг
`-metrics-listen=127.0.0.1:17390`.

`Lease.Release()` немедленно делает локальный lease невалидным и выполняет
сетевое освобождение асинхронно. `client1of1` однократно пытается поставить
`Release` в send queue. Оставшийся в очереди запрос будет отправлен после
reconnect, уже извлечённый writer автоматически не повторяется; не дошедший до
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
