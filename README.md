# spring-reliable-task-queue-java

基于 Spring Boot 的进程内可靠任务队列：用本地 JSON 文件模拟队列后端，提供**幂等去重、优先级调度、定时投递、失败重试、死信、取消与重新提交、并发消费、按业务键保序、重启恢复**能力。

## 任务状态流转

```
                 提交(幂等键=业务键+负载；可带 priority / notBefore)
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
        │              │        │
        │              └────┬────┘
        │                   ▼ 取消(cancel)
        │                CANCELLED（终态；可重新提交同一业务键）
        └──────── 重启恢复：遗留 RUNNING 回收为 PENDING
```

- `PENDING`：已入队待投递（定时任务在 `notBefore` 之前停留在此状态）；`RUNNING`：已被领取正在执行；`FAILED`：本次失败、等待退避重试；
- `SUCCEEDED` / `DEAD` / `CANCELLED`：均为**终态**，不再被调度；死信可再投递回到 `PENDING`，也可取消变为 `CANCELLED`。
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

## 优先级调度

- 提交时可带 `priority`（整数，越大越优先；缺省 `0`）。
- 积压较多时，领取次序为 **`priority` 降序 → 创建时间升序（同档 FIFO）**，高优先级任务先被消费。
- **优先级不能越过同键顺序**：一个任务只有在“同一业务键下比它更早创建的任务全部进入终态
  （`SUCCEEDED`/`DEAD`/`CANCELLED`）”后才有资格被领取。后提交的高优先级任务不会插到同键更早的任务前面。
- 不传 `priority`（或全为 `0`）时，领取次序退化为纯创建时间 FIFO，与旧行为完全一致。

## 定时投递

- 提交时可带 `notBefore`（ISO-8601，如 `2026-10-03T12:00:00Z`）：任务**最早**可开始执行的时间点。
- 到点之前任务停留 `PENDING`：既不会被消费，也**不算失败、不占重试次数**（`attempts` 保持 0）。
- 该时间点持久化在 `tasks.json`，**应用重启后不丢**，未到点重启也不会提前执行。
- **退避与定时点互不混淆**：重试退避只从任务“真正开始执行（被领取）之后”按 `Instant.now()+backoff` 计算，
  原始 `notBefore` 字段保留；两者分别存于 `nextAttemptAt`（当前等待点）与 `notBefore`（原始定时点）。
- **同键约束**：同一业务键下，前一个任务还没到点或还没做完（未终结），后面的任务不会被提前挑走，
  即使后一个任务自己已经到点。
- `notBefore` 传过去的时间等价于立即投递。

## 取消与重新提交

- 可以取消尚未终结的任务：`PENDING`（排队中，含等待定时点）、`FAILED`（重试等待中）、`DEAD`（死信）都可取消，
  取消后变为终态 `CANCELLED`，记录 `cancelledAt` 与可选 `reason`，**之后永远不会再执行**（含不再重试）。
- `RUNNING`（正在执行）不允许取消，返回 `outcome=RUNNING`；`SUCCEEDED` 返回 `ALREADY_FINISHED`；
  `CANCELLED` 再次取消返回 `ALREADY_CANCELLED`；任务不存在返回 `NOT_FOUND`。调用方可据 `outcome` 明确区分这五种结果。
- 取消会**释放该任务的幂等键**（业务键+负载），因此取消后可以用**完全相同的业务键和负载**重新提交，
  得到一个新的 `taskId` 并正常执行一次；旧记录保留为 `CANCELLED` 可查。
- 重新提交后的同键顺序可预期：被取消的任务是终态、不再占位；同键顺序始终按“当前最早创建的未终结任务优先”，
  新提交的任务排在同键仍在处理中的任务之后，不会越级。

## 兼容性说明

- **旧行为不变**：不带 `priority`/`notBefore` 的提交走原有 `POST /api/tasks`（两字段均可选），
  缺省 `priority=0`、无定时点，调度次序与旧版本一致；去重、退避、死信、分带保序、重启恢复规则均未改变。
- **旧数据可直接加载**：旧 `tasks.json` 没有 `priority/notBefore/cancelledAt/cancelReason` 字段，
  Jackson 反序列化时取 Java 字段默认值（`priority=0`、其余为 `null`），效果等价于“普通优先级、立即投递、未取消”。
- **旧配置不受影响**：没有新增任何必须配置项，`application.properties` 保持原样即可工作。
- 新增 `CANCELLED` 终态与 `/cancel`、`/id/{taskId}` 接口均为增量能力，不影响既有接口的返回结构。

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
| `POST` | `/api/tasks` | 提交任务，body: `{"businessKey":"k","payload":"p","priority":10,"notBefore":"2026-10-03T12:00:00Z"}`；`priority`/`notBefore` 均可省略；返回 `outcome=ACCEPTED/DUPLICATE` |
| `GET` | `/api/tasks/{businessKey}` | 按业务键查询最新一条任务状态 |
| `GET` | `/api/tasks/id/{taskId}` | 按任务 id 查询（取消/重提后用于区分同一业务键的不同任务实例） |
| `GET` | `/api/tasks` | 列出全部任务（含 CANCELLED 记录） |
| `GET` | `/api/tasks/dead-letters` | 列出死信 |
| `POST` | `/api/tasks/dead-letters/{taskId}/redeliver` | 死信再投递 |
| `POST` | `/api/tasks/{taskId}/cancel` | 取消任务，body 可空或 `{"reason":"用户撤单"}`；返回 `outcome=CANCELLED/ALREADY_CANCELLED/RUNNING/ALREADY_FINISHED/NOT_FOUND` |

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

# 高优先级：积压时 priority 大的先消费（同键内仍保序）
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"urgent-1","payload":"p","priority":10}'

# 定时投递：到 2026-10-03T12:00:00Z 之前保持 PENDING，到点才执行；重启后该时间点不丢
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"future-1","payload":"p","notBefore":"2026-10-03T12:00:00Z"}'

# 取消（排队中/重试中/死信均可），再用同一业务键重新提交
TASK_ID=$(curl -s localhost:8080/api/tasks/future-1 | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
curl -X POST localhost:8080/api/tasks/$TASK_ID/cancel -H 'Content-Type: application/json' \
     -d '{"reason":"用户撤单"}'
curl -X POST localhost:8080/api/tasks/$TASK_ID/cancel   # 再次取消 → ALREADY_CANCELLED

curl localhost:8080/api/tasks/order-1        # 查看状态流转
curl localhost:8080/api/tasks/dead-letters   # 查看死信
# 杀掉进程再重启，观察未完成任务被恢复、已完成任务不重复执行、定时点/退避点不丢
```

测试覆盖的场景（`src/test/java/.../queue/`）：

| 测试类 | 覆盖场景 |
|---|---|
| `DuplicateSubmissionTest` | 重复提交去重：同幂等键只执行一次、返回可区分原因；同键不同负载各自入队 |
| `RetryAndDeadLetterTest` | 重试耗尽进死信、退避重试不重复计数、死信查询与再投递 |
| `ConcurrentConsumptionTest` | 8 键 × 20 任务并发消费：同键严格保序、跨键并行、每任务恰好一次 |
| `RestartRecoveryTest` | 重启后已完成不重跑、PENDING 不丢、崩溃残留 RUNNING 被回收重投 |
| `PrioritySchedulingTest` | 跨键高优先级先领取、缺省优先级保持 FIFO、同键高优先级不能越级、终态前序释放后续 |
| `PrioritySchedulingEndToEndTest` | 真实轮询下端到端验证：条带占满时积压中的高优先级先完成 |
| `DelayedDeliveryTest` | 到点前 PENDING/attempts=0、到点才执行、同键延迟队首阻塞后续、退避从真正执行后算起 |
| `CancelAndResubmitTest` | 排队中/重试中/死信可取消、五类取消结果可区分、取消后同键重新提交且顺序可预期 |
| `RestartRecoverySchedulingTest` | 重启后定时点/优先级/attempts 保留不提前执行、退避点与 attempts 保留不提前重试 |
