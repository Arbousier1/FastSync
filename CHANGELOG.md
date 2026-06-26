# 更新日志

本文件记录 FastSync 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

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
