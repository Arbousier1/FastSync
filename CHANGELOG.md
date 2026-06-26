# 更新日志

本文件记录 FastSync 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### 性能优化

- **LatencyTracker 改用环形缓冲区**：`ConcurrentLinkedDeque<Long>` → 预分配 `long[]` + `AtomicInteger` head 指针，消除装箱分配；`logStats()` 从 3 次 `toArray()+sort()` 改为单次排序
- **CompressionUtil 减少数组分配**：`wrap()` 直接压缩进最终数组再裁剪（省 1 次分配 + 1 次 `arraycopy`）；`unwrap()` 去掉 `Arrays.copyOfRange`，直接带偏移量传给 LZ4 解压器
- **关服保存改为异步并行**：`saveAllOnlinePlayers()` 数据采集在主线程，序列化+DB写入派发到异步线程池，`CompletableFuture.allOf().join()` 等待全部完成
- **周期保存分批**：每 tick 最多处理 10 人，剩余玩家摊到后续 tick，避免大量在线玩家时卡顿

### 修复

- **`collectPDC()` 空操作**：移除只塞了 dummy `__pdc__` 标记的无意义代码，改为返回空 map + TODO 注释（Bukkit PDC API 不支持枚举所有 key）
- **冲突快照无限累积**：`ConflictManager.saveConflictSnapshot` 保存后未修剪，现已链式调用 `pruneSnapshots(uuid, maxSnapshots)` 与成功路径保持一致
- **RedissonManager 流消费者异常隔离**：单个 listener 回调抛异常会静默杀掉消费线程；改为 try-catch 隔离 + `ExecutorService` 替代裸 `Thread`
- **AsyncExecutor 日志**：`e.printStackTrace()` → `logger.log(Level.SEVERE, ..., e)`，统一走日志框架
- **FastSync 命令异常处理**：`/fastsync log` 的 limit 参数 `parseInt` 未捕获 `NumberFormatException`
- **DatabaseManager 死代码清理**：移除 `op_seq` 列、`incrementOpSeq()` 方法、`OP_SEQ_FIELD` 常量（操作日志已由 `FileOperationLogManager` 接管，SQL `op_seq` 不再使用）

### 变更

- **将 `PlayerSpawnLocationEvent` 替换为 `PlayerJoinEvent`**：玩家数据应用从出生位置事件改为加入事件，确保在 `EventPriority.LOWEST` 优先级下先于其他插件应用数据，避免物品复制漏洞
- **Redis 协调层从 Lettuce + sparrow-redis-message-broker 替换为 Redisson**：统一使用 `RedissonManager`，`RTopic` 做 Pub/Sub 锁通知，`RStream` 做关键事件可靠交付
- **SQL 操作日志替换为本地文件日志**：从 MySQL `fastsync_operation_log` 表改为纯 Java NIO 文件日志（`FileOperationLogManager`），每玩家独立 append-only 文件，无需 `--add-opens` JVM 参数
- **数据库层从原始 JDBC 替换为 jOOQ DSL**：所有 SQL 查询改用 jOOQ 类型安全 DSL 构建，OCC + fencing token CAS 语义不变
- **LZ4 压缩库坐标迁移**：从已废弃的 `org.lz4:lz4-java` 迁移到维护分支 `at.yawk.lz4:lz4-java:1.11.0`（修复 CVE-2025-12183）

### 移除

- 移除 `RedisManager`、`StreamManager`、`LockRequestMessage`、`LockReleasedMessage` 四个类（被 `RedissonManager` 统一替代）
- 移除 `OperationLogManager`（SQL 表日志，被 `FileOperationLogManager` 替代）
- 移除 `ChronicleQueueLogManager`（依赖 `sun.nio` 内部 API，被纯 Java NIO `FileOperationLogManager` 替代）
- 移除 `io.lettuce:lettuce-core` 依赖
- 移除 `net.momirealms:sparrow-redis-message-broker` 依赖
- 移除 `net.openhft:chronicle-queue` 依赖
- 移除 JVM 启动参数 `--add-opens` 要求（实现即插即用）

### 新增

- 新增 `RedissonManager`：单一 `RedissonClient` 统一管理 Pub/Sub + Streams
- 新增 `FileOperationLogManager`：纯 Java NIO 实现的 append-only 操作日志，零 JVM 参数
- 新增性能基准测试：LZ4 压缩、CRC32 校验、StreamEvent 序列化、文件日志吞吐量
- 新增 `ChronicleQueueBenchmark`（后随 Chronicle Queue 移除而删除）

### 修复

- 修复 Gradle 9.x test worker 的 `--add-opens` 传递问题（通过移除 Chronicle Queue 彻底解决）
- 修复 CRC32 性能测试中 `System.nanoTime()` 精度溢出导致的负吞吐率

### 文档

- 重写 README：五层架构说明、安全模型流程图、性能基准数据、致谢上游项目
- 新增 JVM 启动参数说明（后因移除 Chronicle Queue 而删除，改为"即插即用"声明）
