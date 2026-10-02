# spring-reliable-task-queue-java

基于 Spring Boot 的进程内可靠任务队列：用本地 JSON 文件模拟队列后端，提供**幂等去重、失败重试、死信、并发消费、按业务键保序、重启恢复**能力。

## 任务状态流转

```
                 提交(幂等键=业务键+负载)
                       │
                       ▼
        ┌────────── PENDING ◄──────────────┐
        │              │ 调度器领取(claimDue) │
        │              ▼                    │
        │           RUNNING                 │
        │          ┌────┴────┐              │
        │     成功 ▼         ▼ 失败          │
        │      SUCCEEDED   attempts+1       │
        │                  ┌─┴─┐            │
        │          未耗尽 ▼     ▼ 耗尽       │
        │           FAILED    DEAD ──再投递──┘
        │  (退避到期后回到待投递)  (死信，终态)
        └──────── 重启恢复：遗留 RUNNING 回收为 PENDING
```

- `PENDING`：已入队待投递；`RUNNING`：已被领取正在执行；`FAILED`：本次失败、等待退避重试；
- `SUCCEEDED` / `DEAD`：终态，不再被调度；死信可通过接口再投递回到 `PENDING`。
- 每次状态变更都原子落盘（写临时文件 + 原子替换），并带 `version` 乐观锁，领取与完成不会互相覆盖。

## 去重规则

- **幂等键 = 业务键 + 负载**。相同幂等键的重复提交（如客户端超时重试）返回 `outcome=DUPLICATE`，
  并携带已存在任务的 `taskId`、当前状态与去重原因，**不会再次执行**。
- 相同业务键、不同负载视为不同任务，正常入队；消费时同一业务键的任务严格按提交顺序串行执行。

## 重试与死信规则

- 处理器抛异常即视为失败，`attempts+1`；未耗尽时按**指数退避**等待重试：
  `延迟 = min(backoff-base-ms * 2^(n-1), backoff-max-ms)`（n 为第几次执行）。
- 达到 `max-attempts`（含首次）后进入死信 `DEAD`，保留 `lastError` 便于定位失败环节；
  死信不再被投递，可通过 REST 接口查询与再投递（再投递后 `attempts` 清零重新计）。
- 任务只有在被领取（`RUNNING`）后才会执行，领取是原子的，重试中/已成功/死信的任务都不会被重复投递、重复计数。

## 并发与保序

- 调度器按 `poll-interval-ms` 轮询领取到期任务，按**业务键哈希分带**到 `consumer-threads` 个单线程执行器：
  同一业务键永远落到同一条带 → 严格 FIFO 串行；不同键分布到不同条带 → 并行消费。

## 重启恢复

- 所有状态持久化在 `queue.storage-dir/tasks.json`，重启时自动加载。
- 已 `SUCCEEDED` 的任务不会被重新执行；`PENDING`/`FAILED` 任务继续按原规则调度；
- 崩溃时遗留的 `RUNNING` 任务在启动时被回收为 `PENDING` 重新投递（日志会打印回收数量）。
  注意：若崩溃发生在“业务已执行成功但状态尚未落盘”的极小窗口内，该任务会被重放一次，
  业务处理器应自身保证幂等。

## 配置项（application.properties）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `queue.storage-dir` | `./data/queue` | 本地存储目录（`tasks.json`） |
| `queue.consumer-threads` | `4` | 消费者线程数（= 保序分带数） |
| `queue.max-attempts` | `3` | 最大执行次数（含首次），耗尽进死信 |
| `queue.backoff-base-ms` | `1000` | 退避基数（毫秒） |
| `queue.backoff-max-ms` | `30000` | 退避上限（毫秒） |
| `queue.poll-interval-ms` | `200` | 调度轮询间隔（毫秒） |
| `queue.poll-batch-size` | `50` | 单次轮询最多领取的任务数 |

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/tasks` | 提交任务，body: `{"businessKey":"k","payload":"p"}`；返回 `outcome=ACCEPTED/DUPLICATE` |
| `GET` | `/api/tasks/{businessKey}` | 按业务键查询任务状态 |
| `GET` | `/api/tasks` | 列出全部任务 |
| `GET` | `/api/tasks/dead-letters` | 列出死信 |
| `POST` | `/api/tasks/dead-letters/{taskId}/redeliver` | 死信再投递 |

## 本地验证

```bash
# 跑全部测试（日志中 [CASE]/[PASS] 标明每个用例的覆盖点与结果）
mvn test

# 启动应用后手工验证
mvn spring-boot:run
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"order-1","payload":"created"}'
# 重复提交 → outcome=DUPLICATE，不会重复执行
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"order-1","payload":"created"}'
curl localhost:8080/api/tasks/order-1        # 查看状态流转
curl localhost:8080/api/tasks/dead-letters   # 查看死信
# 杀掉进程再重启，观察未完成任务被恢复、已完成任务不重复执行
```

测试覆盖的场景（`src/test/java/.../queue/`）：

| 测试类 | 覆盖场景 |
|---|---|
| `DuplicateSubmissionTest` | 重复提交去重：同幂等键只执行一次、返回可区分原因；同键不同负载各自入队 |
| `RetryAndDeadLetterTest` | 重试耗尽进死信、退避重试不重复计数、死信查询与再投递 |
| `ConcurrentConsumptionTest` | 8 键 × 20 任务并发消费：同键严格保序、跨键并行、每任务恰好一次 |
| `RestartRecoveryTest` | 重启后已完成不重跑、PENDING 不丢、崩溃残留 RUNNING 被回收重投 |
