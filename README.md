# spring-reliable-task-queue-java

进程内任务队列与可靠投递示例：用本地 JSON 文件模拟队列后端，保证任务在**重复提交、失败重试、服务重启**三类场景下状态清晰、行为可预期。

## 任务状态流转

```
                 提交(去重通过)
                      │
                      ▼
        ┌───────── PENDING ◄──────────────┐
        │             │                   │
        │        claimNextDue             │ 到达重试时间
        │             ▼                   │
        │          RUNNING                │
        │          │     │                │
        │   成功   │     │ 失败           │
        │          ▼     ▼                │
        │     SUCCEEDED  FAILED ──────────┘ (attempts < maxAttempts)
        │     （终态）      │
        │                  │ attempts >= maxAttempts
        │                  ▼
        │                DEAD ──人工再投递(redeliver)──► PENDING（attempts 归零）
        │               （终态，可查询）
        │
        └── 重启恢复：进程退出时处于 RUNNING 的任务，启动时重置为 PENDING 重新投递
```

- **PENDING**：已入队待投递；**RUNNING**：已被消费者领取；**FAILED**：本次失败、等待退避到期重投；**SUCCEEDED / DEAD**：终态。
- 每次状态变更都会原子落盘（临时文件 + rename），崩溃不会留下写了一半的存储文件。

## 去重规则（幂等提交）

- 提交时以 `businessKey` 做幂等键：只要同键任务处于未终结状态（PENDING / RUNNING / FAILED），重复提交返回 `DUPLICATE` 并携带已存在任务，**不会再次入队、不会重复执行**。
- 同键任务到达终态（SUCCEEDED / DEAD）后，允许再次提交为一条新任务。
- REST 层：新任务返回 `201 + ACCEPTED`，重复提交返回 `200 + DUPLICATE`，两种结果可明确区分。

## 重试与死信规则

- 任务执行抛异常即失败，按指数退避重投：`等待 = backoffBaseMs * 2^(attempts-1)`，封顶 `maxBackoffMs`。
- 第 `maxAttempts` 次仍失败则进入死信（DEAD），可通过 `GET /api/tasks/dead-letters` 查询、`POST /api/tasks/dead-letters/{id}/redeliver` 再投递（attempts 归零，重新按 maxAttempts 重试）。
- 任务被领取后即置 RUNNING 并落盘，重试中 / 已成功的任务不会被其他消费者重复领取、重复计数。
- 多消费者通过“在处理业务键集合”做保序栅栏：同一 `businessKey` 的任务同一时间只被一个消费者处理，多个同键任务按创建先后执行。

## 重启恢复

- 启动时加载存储文件，把中断的 RUNNING 任务重置为 PENDING 重新投递（未完成任务不丢）。
- SUCCEEDED / DEAD 终态任务保持原状，不会被重新执行。

## 配置项（`application.properties`，前缀 `taskqueue.`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `taskqueue.storage-path` | `./data/task-queue.json` | 本地存储文件路径 |
| `taskqueue.consumer-count` | `4` | 并发消费者线程数 |
| `taskqueue.max-attempts` | `3` | 最大尝试次数（含首次） |
| `taskqueue.backoff-base-ms` | `1000` | 指数退避基数（毫秒） |
| `taskqueue.max-backoff-ms` | `30000` | 退避上限（毫秒） |
| `taskqueue.poll-interval-ms` | `100` | 消费者空闲轮询间隔（毫秒） |

## REST 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks` | 提交任务，body：`{"businessKey":"k","payload":"p"}`；新任务 201/ACCEPTED，重复 200/DUPLICATE |
| GET | `/api/tasks/{id}` | 查询单个任务状态 |
| GET | `/api/tasks` | 列出全部任务 |
| GET | `/api/tasks/dead-letters` | 死信列表 |
| POST | `/api/tasks/dead-letters/{id}/redeliver` | 死信再投递 |

业务处理逻辑通过声明 `TaskHandler` Bean 注入（默认实现仅打印日志）：

```java
@Bean
TaskHandler taskHandler() {
    return task -> doBusiness(task.getBusinessKey(), task.getPayload());
}
```

## 本地验证

```bash
# 跑全部测试（日志中带 [用例]/[结果]/[通过] 标记，可看到每个场景覆盖与结果）
mvn test

# 启动应用后手工验证
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"order-1","payload":"hello"}'   # 201 ACCEPTED
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"order-1","payload":"hello"}'   # 200 DUPLICATE
curl localhost:8080/api/tasks                           # 查看状态流转
```

## 测试覆盖

| 测试类 | 覆盖场景 |
| --- | --- |
| `DuplicateSubmitTest` | 重复提交去重、并发重复提交只入队一次、终态后同键可再提交 |
| `RetryAndDeadLetterTest` | 重试耗尽进死信、死信查询与再投递、重试中不重复投递、成功任务不重复执行 |
| `ConcurrentConsumptionTest` | 多消费者下每个任务恰好执行一次、同业务键保序且不并发、存储层保序栅栏 |
| `RestartRecoveryTest` | 存储层状态恢复（RUNNING→PENDING、终态不变）、端到端重启后未完成任务重跑且已完成任务不重跑 |
