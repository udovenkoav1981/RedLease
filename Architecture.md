# RedLease — архитектура

Этот документ описывает принятую архитектуру RedLease и то, **как** она
обеспечивает свойства из [`Requirements.md`](Requirements.md).

Архитектурные решения из этого документа не изменяются без отдельного
согласования.

## 1. Обзор

RedLease — специализированный quorum-based distributed lease service,
оптимизированный для Acquire и Renew примерно за один network RTT.

Ключевые решения:

```text
Servers                    5 independent lock-servers
Quorum                     3 of 5
Storage                    RAM only
Disk persistence           none
Protocol maximum TTL       5 s
Per-server configuredMaxTTL <= Protocol maximum TTL
Typical Renew interval     1 s
Safety margin              100 ms (fixed)
Lease time source          local wall clock (`time.Now().Round(0)`)
Wire TTL representation    uint64 milliseconds
Leader                     none
Server-to-server hot path  none
Client transport           5 persistent ordered gRPC streams
Initial ownership          >= 3/5
Steady-state target        5/5
Restart protection         quarantine > protocolMaxTTL + safetyMargin
Forced lease overwrite     forbidden
Global fencing token       none
```

- Архитектура не использует leader-based replicated log. 
- Серверы не обмениваются сообщениями друг с другом, и ни чего друг о друге не знают
- Membership статический. Все клиенты используют одну и ту же конфигурацию из пяти серверов.
- клиент независимо и параллельно обращается ко всем lock-server.

## 2. Топология общения клиент-сервер

В штатном Acquire или Renew клиент делает параллельный fan-out на все серверы:

```text
             -> S1
             -> S2
client       -> S3
             -> S4
             -> S5
```

Критический путь заканчивается после третьего успешного ответа, поэтому latency
примерно равна RTT до третьего по скорости сервера.

## 3. Состояние lock-server

Для каждого ключа server хранит только локальное состояние в RAM:

```go
type Lease struct {
    LeaseID  LeaseID
    Deadline time.Time
}
```

Операции над одним ключом выполняются локально атомарно. 

Истёкшие записи могут удаляться лениво при обращении и/или фоновым механизмом.
Точная memory-management strategy будет определена при реализации.

## 4. Lease identity

Lease идентифицируется составным значением:

```text
leaseID = { 
    clientID,  : uint32
    bootID,    : uint32
    leaseSeq   : uint64
}
```

`clientID` — уникальный ID клиентской ноды, заданный статической конфигурацией.
Повторяющиеся `clientID` являются ошибкой конфигурации.

`bootID` генерируется криптографическим RNG при каждом запуске клиентского
процесса и разделяет разные инкарнации одной клиентской ноды.

`leaseSeq` — локальный атомарный счётчик, начинающийся с единицы после старта
процесса. Каждый Acquire использует новое значение. Поэтому один процесс может
одновременно владеть множеством разных ключей, не переиспользуя идентификаторы.

## 5. Операции протокола

Клиентские операции:

```text
Acquire(key, leaseID, ttl)
Renew(key, leaseID, ttl)
Release(key, leaseID)
GetTTL()
```

Все значения TTL в wire protocol передаются как `uint64` миллисекунд. Поэтому
отрицательный `requestedTTL` не представим и отдельного ответа `INVALID_TTL`
нет. Нулевой `requestedTTL` допустим: новый lease с таким TTL не даёт клиенту
положительной validity, а Renew не сокращает уже существующий deadline.

### 5.1. Acquire

Клиент создаёт новый `leaseID`, фиксирует локальное время начала операции и
одновременно отправляет `Acquire(key, leaseID, requestedTTL)` всем пяти
серверам.

Каждый server локально атомарно выполняет:

```text
effectiveTTL = min(requestedTTL, configuredMaxTTL)
now = localNow

if key отсутствует or deadline <= now:
    установить leaseID
    deadline = now + effectiveTTL
    return OK(ttl = deadline - now)
if current.leaseID == requested.leaseID:
    deadline не изменять
    return ALREADY_OWNED(ttl = deadline - now)
return BUSY
```

`OK` и `ALREADY_OWNED` подтверждают наличие запрошенного `leaseID` на сервере и
учитываются клиентом как успешная реплика. `ALREADY_OWNED` делает повторный
Acquire идемпотентным, но не продлевает server deadline: продление выполняется
только через Renew. Поле `ttl` успешного ответа содержит оставшееся время до
server deadline на момент формирования ответа.

Клиент устанавливает владение после трёх успешных реплик, если рассчитанный для
них `quorumValidUntil` ещё не достигнут, и сразу возвращает успех приложению.
Оставшиеся запросы не блокируют этот результат и продолжают обрабатываться для
background healing.

### 5.2. Cleanup после failed Acquire

Результат меньше 3/5 означает, что клиент не получил lease. Даже если lease
частично установлен на одном или двух серверах, клиент немедленно отправляет
Release с тем же `leaseID` всем пяти серверам.

Повторная попытка Acquire использует новый `leaseID` и randomized backoff.

### 5.3. Background healing и client-side Attach

После commit threshold 3/5 клиент продолжает в фоне доводить активный lease до
целевых 5/5 в целях повышения отказоустойчивости свого quorum и снижения вероятности 
повторного конфликта конкурирующих клиентов:

```text
3/5 -> 4/5 -> 5/5
```

`Attach` — только название логической операции внутри клиента. Она не является
операцией wire protocol, не отправляется в stream и ничего не добавляет в
server API. Lock-server не знает о понятии `Attach`.

В рамках этой логической операции клиент продолжает обрабатывать ответы
исходного Acquire и отправляет обычный
`Acquire(key, leaseID, requestedTTL)` серверам, на которых lease отсутствует.
Healing продолжается в течение жизни lease, в том числе после reconnect или
restart сервера.

После Release или истечения локальной validity (если Renew не удалось) клиент прекращает healing.

Конкретная retry/backoff policy background healing остаётся параметром
реализации.

### 5.4. Renew

Примерно раз в секунду клиент фиксирует локальное время начала Renew и
параллельно отправляет `Renew(key, leaseID, requestedTTL)` всем пяти серверам.

Server локально атомарно выполняет:

```text
now = localNow

if current.leaseID != requested.leaseID:
    return STALE
if deadline <= now:
    return STALE

effectiveTTL = min(requestedTTL, configuredMaxTTL)
deadline = max(deadline, now + effectiveTTL)
return OK(ttl = deadline - now)
```

Истёкший lease не воскрешается. После трёх `OK` Renew успешен и клиент обновляет
`validUntil`. Если quorum не собран, прежний успешно подтверждённый quorum и
`validUntil` остаются действительными до этого `validUntil`; потеря соединений
не отзывает lease досрочно. Неуспешный Renew не продлевает `validUntil`, а клиент
может повторять Renew в оставшемся окне validity.

### 5.5. Release

Server удаляет запись только при совпадении идентификатора:

```text
if current.leaseID == requested.leaseID:
    delete lease
```

Это предотвращает удаление нового lease старым владельцем.

Обычный Release может быть асинхронным best-effort.

### 5.6. GetTTL

`GetTTL()` возвращает `configuredMaxTTL`, загруженный из конфигурации
lock-server при старте. Операция не изменяет значение и не принимает новое.
`GetTTL()` доступен как в `QUARANTINE`, так и в `ACTIVE`.

## 6. Модель времени и validity

Каждый lock-server загружает из конфигурации `configuredMaxTTL` — максимальный
срок, который он может предоставить одному lease. Server проверяет при старте:

```text
0 < configuredMaxTTL <= protocolMaxTTL
protocolMaxTTL = 5 s
```

Некорректное значение не позволяет server process запуститься. В течение жизни
процесса `configuredMaxTTL` неизменен, но может отличаться у разных серверов и
может быть изменён при следующем restart отдельного server process.

Клиент передаёт желаемый `requestedTTL` в Acquire и Renew. Server применяет:

```text
effectiveTTL = min(requestedTTL, configuredMaxTTL)
```

Тем самым никакой Acquire или Renew не может установить deadline дальше, чем на
`configuredMaxTTL` от текущего server-local времени.

Rolling-изменение `configuredMaxTTL` выполняется изменением конфигурации и
restart отдельных серверов. Пока обновление продолжается, серверы могут иметь
разные лимиты; клиент учитывает фактический `ttl` каждого успешного ответа.
Quarantine рассчитывается по неизменному `protocolMaxTTL`, а не по новому
`configuredMaxTTL`, поэтому безопасны как увеличение, так и уменьшение лимита.

Absolute timestamp и deadline между машинами не сериализуются.

Каждый server устанавливает локальный deadline:

```go
effectiveTTL := min(requestedTTL, configuredMaxTTL)
now := time.Now().Round(0)
deadline := now.Add(effectiveTTL)
```

Успешные ответы Acquire и Renew содержат `ttl` — оставшуюся локальную validity
соответствующей серверной реплики на момент формирования ответа. Это позволяет
одному клиентскому quorum включать серверы с разными `configuredMaxTTL`.

Клиент измеряет продолжительность Acquire и Renew локально:

```go
operationStart := time.Now().Round(0)
elapsed := time.Now().Round(0).Sub(operationStart)
```

Вызов `Round(0)` удаляет monotonic-компоненту из `time.Time`. Server deadlines,
клиентские `operationStart`, `candidateValidUntil` и `validUntil` используют
локальное wall-clock время. Благодаря этому время, проведённое OS или VM в
suspend, учитывается после resume: server считает lease, чей deadline прошёл за
время паузы, истёкшим, а клиент не начинает новую защищённую операцию по уже
истёкшему `validUntil`.

Значения локального времени никогда не сравниваются между машинами и absolute
timestamp по протоколу не передаётся. 

Для каждого успешного ответа `i` клиент консервативно вычисляет:

```text
candidateValidUntil[i] = operationStart + response[i].ttl - safetyMargin
```

Для подтверждения операции клиент выбирает quorum `Q` из трёх успешных ответов.
Локальная validity этого quorum равна минимальной validity его реплик:

```text
quorumValidUntil = min(candidateValidUntil[i]), i in Q
```

`safetyMargin` — фиксированная константа протокола, равная 100 ms.

Acquire считается успешным только если `quorumValidUntil` ещё не достигнут к
моменту принятия решения. Для успешного Renew клиент обновляет свой срок как:

```text
validUntil = max(previousValidUntil, quorumValidUntil)
```

Использование `operationStart` вычитает из доступной клиенту validity время
получения quorum. Failed Renew и клиентский background healing не двигают
`validUntil` вперёд. Ответы, пришедшие после выбора quorum и возврата результата,
могут обновить сведения о репликах, но сами по себе не меняют `validUntil`.

`time.Now().Round(0)` не может остановить уже начатую бизнес-операцию при
suspend клиентской VM. После resume такая операция может продолжиться уже после
истечения lease; это входит в явно принятое ограничение отсутствия global
fencing token. Клиент проверяет `validUntil` перед запуском новых защищённых
операций.

## 7. Persistent ordered gRPC streams

Каждая клиентская нода поддерживает по одному независимому persistent ordered
gRPC stream к каждому lock-server:

```text
client
  |---- stream ---> S1
  |---- stream ---> S2
  |---- stream ---> S3
  |---- stream ---> S4
  `---- stream ---> S5
```

Один stream multiplexes операции для множества ключей:

```text
Acquire
Renew
Release
Acquire
...
```

Streams независимы: slow или disconnected server не создаёт head-of-line
blocking для остальных четырёх путей.

Persistent streams:

- убирают connection setup из hot path;
- сохраняют порядок доставки запросов для пары client/server;
- уменьшают RPC overhead;
- позволяют обслуживать тысячи активных leases через пять соединений.

Каждый запрос содержит `requestID`, уникальный в пределах одного stream. Server
возвращает тот же `requestID` в ответе. Это correlation identifier, а не номер
операции: он не задаёт порядок применения, не сохраняется server после reconnect
и не обеспечивает дедупликацию.

Server принимает запросы одного stream по порядку и направляет их в ordered
очереди по `key`. Операции одного ключа применяются в порядке получения, а
разные ключи могут обрабатываться параллельно. На практике это может быть
реализовано фиксированным количеством worker queues с выбором очереди по
`hash(key)`. Количество workers является параметром реализации.

Завершённые операции поступают в общий response channel и отправляются одним
writer в gRPC stream. Поэтому ответы для разных ключей могут возвращаться не в
порядке запросов. Client сопоставляет их через `requestID` и не ждёт предыдущий
ответ перед отправкой следующего запроса.

Для каждого ожидаемого ответа client хранит отдельный deadline. После timeout
запрос перестаёт участвовать в quorum, а возможный поздний ответ игнорируется.
Разрыв stream завершает все его незавершённые запросы transport error и
запускает reconnect; client никогда не ждёт отдельный пропущенный ответ
бесконечно. `requestID` не устраняет неопределённость результата при разрыве
stream: повторная отправка опирается на идемпотентность операций по `leaseID`.

Точная retry sequencing после reconnect будет определена при реализации
client transport.

## 8. Restart quarantine

После crash/restart server теряет все RAM-only leases. Чтобы пустой server не
создал новый конфликтующий quorum, каждый процесс начинает работу в состоянии
`QUARANTINE`:

```text
server start
    |
    v
QUARANTINE
    |
    | GetTTL  -> configuredMaxTTL
    | Acquire -> NOT_READY
    | Renew   -> NOT_READY
    | Release -> NOT_READY
    |
    | wait > protocolMaxTTL + safetyMargin
    v
ACTIVE
```

Переход в `ACTIVE` выполняется по обычному локальному timer только после:

```text
REJOIN_DELAY > protocolMaxTTL + safetyMargin
```

Это server-side invariant. В `QUARANTINE` server отвечает на read-only
`GetTTL`, но никогда не возвращает `OK` на lease-операции. Поэтому только что
перезапущенный узел нельзя использовать в успешном lease quorum даже при
клиенте, не знающем о restart.

После перехода в `ACTIVE` server может снова получить реплики активных leases
через обычные Acquire, отправленные client-side background healing.

## 9. Полный restart кластера

При одновременном падении пяти серверов всё lock state теряется. После запуска
каждый server независимо проходит обязательный quarantine.

Даже если процессы или операционные системы перезапустились быстрее пяти
секунд, новый quorum не станет доступен раньше, чем истекут все leases,
существовавшие до отказа:

```text
all RAM state lost
        |
all servers enter QUARANTINE
        |
wait > protocolMaxTTL + safetyMargin
        |
servers become ACTIVE
```

Таким образом безопасность не зависит от длительности внешней процедуры
restart.

## 10. Архитектурные инварианты

```text
< 3 successful replicas -> client does not own lease
>=3 successful replicas -> client owns lease
3/5                     -> commit threshold
5/5                     -> desired steady state
```
