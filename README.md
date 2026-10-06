# spring-reliable-task-queue-java

基于 Spring Boot 的进程内可靠任务队列：用本地 JSON 文件模拟队列后端，提供**幂等去重、优先级调度、定时投递、失败重试、死信、取消与重新提交、并发消费、按业务键保序、重启恢复**能力。

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
        │
        │  PENDING/FAILED/DEAD ──取消──► CANCELLED（终态，可凭同业务键重新提交）
        └──────── 重启恢复：遗留 RUNNING 回收为 PENDING
```

- `PENDING`：已入队待投递（设置了 `notBefore` 时，到点前不可领取）；`RUNNING`：已被领取正在执行；`FAILED`：本次失败、等待退避重试；
- `SUCCEEDED` / `DEAD` / `CANCELLED`：终态，不再被调度；死信可通过接口再投递回到 `PENDING`；取消后可凭同一业务键重新提交新任务。
- 每次状态变更都原子落盘（写临时文件 + 原子替换），并带 `version` 乐观锁，领取与完成不会互相覆盖。

## 去重规则

- **幂等键 = 业务键 + 负载**。相同幂等键的重复提交（如客户端超时重试）返回 `outcome=DUPLICATE`，
  并携带已存在任务的 `taskId`、当前状态与去重原因，**不会再次执行**。
- 相同业务键、不同负载视为不同任务，正常入队；消费时同一业务键的任务严格按提交顺序串行执行。

## 优先级调度

- 提交时可带 `priority`（整数，**越大越优先**，缺省/不传为 `0`）。
- 调度器领取任务时，在所有“已到点可执行”的任务中按 **优先级降序**挑选；同优先级按创建（提交）时间升序。
  因此积压较多时高优先级任务会先被消费。
- **同一业务键内部永远严格先来后到**：后提交的任务即使优先级更高，也不能越过同键还没结束的前辈
  （前辈排队未到点、正在执行、退避重试中都会挡住后辈）。优先级只影响“不同键 / 同键队头之间”的先后。
  该保证对**旧版本留下的数据同样成立**：启动时旧任务会按提交先后补入序号链（见“兼容范围”），
  所以“旧库里未跑完的任务 + 升级后同键高优先级新任务”也严格按提交先后执行，不会出现一会儿按顺序、
  一会儿按优先级的情况。
- 前辈的阻塞边界是其**生命周期状态**：`PENDING`（含定时未到点）、`RUNNING`、`FAILED`（退避等待重试）
  都算未终结，后继同键任务必须等待；只有前辈进入 `SUCCEEDED` / `CANCELLED` / `DEAD` 终态后才放行
  （`DEAD` 被再投递后会重新成为活动队头，再次挡住后继直到它再次终结）。
- 不传优先级时全部任务优先级相同，行为与旧版本完全一致（按提交时间 FIFO）。

## 定时投递（notBefore）

- 提交时可带 `notBefore`（ISO-8601 时间点，如 `2026-10-03T12:00:00Z`），表示**最早可以开始执行的时间点**。
- 到点之前任务保持 `PENDING`：既不会被消费，也**不计失败/不消耗重试次数**；到点之后才允许领取。
  传过去的时间点等价于立即可执行。
- `notBefore` 随任务持久化，**应用重启后不丢**，仍按原时间点放行。
- **退避与定时互不混淆**：首次可领取时间 = `notBefore`（无则立即）；一旦真正开始执行后失败，
  只把“下次重试时间”置为 `失败时刻 + 退避`，不再叠加首次的延迟时间。
- 同一业务键下，前辈还没到点（或还没做完）时，后面的任务即使已到点也不能被提前挑走，顺序仍可预期。

## 取消与重新提交

- 可以取消**尚未终结**的任务：排队中（`PENDING`）、退避重试中（`FAILED`）、死信（`DEAD`）都可取消，
  取消后进入终态 `CANCELLED`，绝不会再被执行。
- 取消结果可在任务记录中查到（状态 `CANCELLED`），重复取消同一任务返回可区分的结果：

  | 情况 | 返回 outcome |
  |---|---|
  | 本次真正取消成功 | `CANCELLED` |
  | 任务此前已取消（重复取消） | `ALREADY_CANCELLED` |
  | 任务已成功（终态） | `ALREADY_SUCCEEDED` |
  | 任务正在执行中（进程内队列不支持中断） | `NOT_CANCELLABLE`（状态仍为 RUNNING） |
  | 任务不存在 | `NOT_FOUND`（HTTP 404） |

- 取消会**释放该任务的幂等键**：取消后可以用同一业务键（即使负载也相同）重新提交，新任务会被当作全新任务正常执行。
- 重新提交后的同键顺序：已取消的任务是终态、不再占据队头；新任务接在该键现存活动任务之后，
  与未取消任务之间仍严格按提交先后执行。

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
  这对旧版本数据一视同仁：旧任务崩溃残留被回收后仍是同键队头，先于其后提交的（哪怕高优先级）新任务执行，
  旧任务与新任务的相对先后在任意次重启后都不变。
- 旧数据的同键序号补齐也发生在加载阶段并立即落盘：**用旧数据第一次启动即完成、中途崩溃也不会回退**，
  之后每次重启看到的都是补齐后的记录，顺序语义不会在重启之间漂移。
- 定时点 `notBefore` 与退避重试时间 `nextAttemptAt` 都随记录持久化：重启后延迟任务仍按原时间点放行，
  退避中的任务仍按原退避点重试，不会因重启而提前或重置。
  注意：若崩溃发生在“业务已执行成功但状态尚未落盘”的极小窗口内，该任务会被重放一次，
  业务处理器应自身保证幂等。

## 配置项（application.properties）

新增能力不引入新配置项，沿用原有配置；`priority` / `notBefore` 均为提交参数（缺省即旧行为）。

| 配置 | 默认值 | 说明 |
|---|---|---|
| `queue.storage-dir` | `./data/queue` | 本地存储目录（`tasks.json`） |
| `queue.consumer-threads` | `4` | 消费者线程数（= 保序分带数） |
| `queue.max-attempts` | `3` | 最大执行次数（含首次），耗尽进死信 |
| `queue.backoff-base-ms` | `1000` | 退避基数（毫秒） |
| `queue.backoff-max-ms` | `30000` | 退避上限（毫秒） |
| `queue.poll-interval-ms` | `200` | 调度轮询间隔（毫秒） |
| `queue.poll-batch-size` | `50` | 单次轮询最多领取的任务数 |

## 兼容范围（旧行为 / 旧配置 / 旧数据）

- **旧提交方式不变**：`POST /api/tasks` 的 body 仍只需要 `businessKey`、`payload`；
  `priority`、`notBefore` 均可缺省，此时分别取 `0` 与“不延迟”，表现与上一版本完全一致。
- **旧配置不变**：没有新增任何 `queue.*` 配置，旧 `application.properties` 直接可用。
- **旧数据自动兼容（无需手工迁移/清空）**：旧版本落盘的 `tasks.json` 缺少 `priority` / `notBefore` / `seq` 字段，
  启动加载时自动补默认值（优先级 `0`、不定时），并按业务键依据**创建时间 + 文件中的原始先后**为旧记录
  重新分配同键提交序号 `seq=1..n`（升级后新提交的任务序号从 `n+1` 续编）。补齐结果在首次加载时立即落盘，
  重启幂等、不会重复迁移。
  因此**同业务键顺序在新旧数据之间始终一致**：旧任务（无论排队中、执行中崩溃残留、还是退避重试中）
  未终结前，同键后提交的任务即使带更高优先级也不能插队；旧任务进入终态（成功/取消/死信）后才放行后继。
  跨业务键的优先级调度不受影响：不同键之间仍按“高优先级先消费、同级按提交时间”挑选。
- 既有语义保持不变：幂等去重规则（业务键+负载）、退避/死信规则、按键哈希分带并发与重启恢复均未改变。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/tasks` | 提交任务，body: `{"businessKey":"k","payload":"p","priority":10,"notBefore":"2026-10-03T12:00:00Z"}`；后两个字段可选；返回 `outcome=ACCEPTED/DUPLICATE` |
| `GET` | `/api/tasks/{businessKey}` | 按业务键查询最新任务状态 |
| `GET` | `/api/tasks/id/{taskId}` | 按任务 id 查询（取消结果等可在此查到） |
| `POST` | `/api/tasks/{taskId}/cancel` | 取消任务，返回 `outcome=CANCELLED/ALREADY_CANCELLED/ALREADY_SUCCEEDED/NOT_CANCELLABLE`；不存在返回 404 |
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

# 高优先级任务（积压多时先被消费，但不打乱同键顺序）
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"urgent-1","payload":"p","priority":10}'
# 定时投递：到点前不会执行（时间用 ISO-8601）
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"later-1","payload":"p","notBefore":"2026-10-03T12:00:00Z"}'
# 取消（先按业务键查出任务，取 id）
curl -X POST localhost:8080/api/tasks/<taskId>/cancel
curl localhost:8080/api/tasks/id/<taskId>   # 查看取消结果 CANCELLED
# 取消后用同一业务键可重新提交
curl localhost:8080/api/tasks/order-1        # 查看状态流转
curl localhost:8080/api/tasks/dead-letters   # 查看死信
# 杀掉进程再重启，观察未完成任务（含未到点的延迟任务、退避中的任务）被恢复、已完成任务不重复执行
```

### 手工验证“旧数据 + 同键高优先级新任务”

```bash
# 1) 用旧版本（13c79c6）启动一次并提交任务，留下不含 seq/priority/notBefore 的 tasks.json
# 2) 直接用新版本代码启动（无需迁移、无需清空 data/queue/）：
mvn spring-boot:run
#    启动日志可见：[store] 从 .../tasks.json 恢复 N 条任务记录（旧数据序号补齐 M 条）
# 3) 对尚未跑完的旧任务同一业务键提交一个高优先级新任务：
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
     -d '{"businessKey":"<旧任务的业务键>","payload":"new-after-upgrade","priority":100}'
# 4) 观察执行日志：旧任务先执行、成功/取消/死信后新任务才执行；
#    期间另提一个其他业务键的高优任务，可看到跨键高优先级仍先被消费。
# 5) 杀掉进程再重启（可在旧任务执行中杀）：恢复后同键相对顺序不变。
```

测试覆盖的场景（`src/test/java/.../queue/`）：

| 测试类 | 覆盖场景 |
|---|---|
| `DuplicateSubmissionTest` | 重复提交去重：同幂等键只执行一次、返回可区分原因；同键不同负载各自入队 |
| `RetryAndDeadLetterTest` | 重试耗尽进死信、退避重试不重复计数、死信查询与再投递 |
| `ConcurrentConsumptionTest` | 8 键 × 20 任务并发消费：同键严格保序、跨键并行、每任务恰好一次 |
| `RestartRecoveryTest` | 重启后已完成不重跑、PENDING 不丢、崩溃残留 RUNNING 被回收重投 |
| `PrioritySchedulingTest` | **新增**：跨键高优先级先消费、同键高优先级后辈不能插队前辈 |
| `DelayedDeliveryTest` | **新增**：notBefore 到点才执行、同键延迟队头挡住后辈、退避不与延迟叠加、过去时间立即执行 |
| `ScheduledRestartTest` | **新增**：重启后 notBefore 与退避 nextAttemptAt 都不丢、不被重置 |
| `CancelAndResubmitTest` | **新增**：PENDING/FAILED/DEAD 可取消、重复取消可区分、RUNNING/SUCCEEDED 结果、取消后同键同负载重提与顺序 |
| `PriorityDelayOrderingTest` | **新增**：优先级 × 定时 × 同键保序三方交叉顺序 |
| `LegacyDataOrderingTest` | **修复新增**：旧版本数据（无 `seq` 字段）下，同键高优新任务不得越过排队中/退避中/RUNNING 崩溃残留的旧任务；崩溃恢复与二次重启后顺序不漂移；跨键高优先级仍最先；SUCCEEDED/DEAD/CANCELLED 终态旧任务不再阻塞后继 |

运行 `mvn test` 时，每个用例会打印 `[CASE]`（覆盖的业务点）与 `[PASS]`（验证结果），汇总行可看到全部用例数。
