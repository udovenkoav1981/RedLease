# RedLease — a 1-RTT quorum-based distributed lease protocol inspired by Redlock

RedLease предоставляет распределённые краткоживущие блокировки ресурсов между
узлами кластера с приоритетом на минимальную latency (1 RTT). 

Сервис является распределенным и отказоустойчивым к выходу из строя 2 узлов.

Все данные хранятся только в RAM.

Client и server поддерживаются только на Linux; для отсчёта lease используется
монотонный suspend-aware clock `CLOCK_BOOTTIME`.

Не используется Raft/Paxos. Quorum собирается на клиенте.
