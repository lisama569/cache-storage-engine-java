# 本地缓存存储引擎（cache-storage-engine-java）

基于 Spring Boot 的可对外访问本地缓存存储引擎：带 TTL 的键值读写与批量操作、
容量上限下的确定性淘汰、多线程并发安全、WAL 持久化与崩溃恢复。

## 快速开始

```bash
mvn spring-boot:run          # 启动服务（默认随机端口，可用 server.port 指定）
mvn test                     # 运行全部测试
```

### REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/cache/{key}` | 读取单键 |
| POST | `/api/cache` | 写入单键，body: `{"key","value","ttlMillis"}` |
| DELETE | `/api/cache/{key}` | 删除单键 |
| POST | `/api/cache/batch-get` | 批量读，body: `{"keys":[...]}`，逐键返回命中值或失败原因 |
| POST | `/api/cache/batch-put` | 原子批量写，body: `{"entries":{...},"ttlMillis"}` |
| POST | `/api/cache/batch-delete` | 批量删，逐键返回结果状态 |
| POST | `/api/cache/purge-expired` | 主动清理过期键，返回回收数量 |
| GET | `/api/cache/stats` | 统计：size/capacity/hits/misses/expirations/evictions |

失败原因通过 **HTTP 状态码 + 响应体 `code`** 双重区分：

| 失败原因 | HTTP | code |
|---|---|---|
| 键不存在 | 404 | `NOT_FOUND` |
| 键已过期 | 410 | `EXPIRED` |
| 容量不足 | 507 | `CAPACITY_FULL` |

## 一致性模型

- **线性化**：每个对外操作（get/put/delete/批量操作）对键空间状态构成一个线性化点。
  写路径（put/delete/批量写/清理/压缩）在单把引擎级写锁下串行执行；同一键上的
  更新、过期清理与删除互斥，不存在锁顺序环，**无死锁**。
- **无锁读路径**：读不加锁。写线程用"整体替换条目对象"的方式发布更新
  （`ConcurrentHashMap` 保证安全发布），读者只能看到更新前或更新后的完整对象，
  不会读到撕裂状态；单写者递增序列下读者不会观测到回退。
- **无丢失更新 / 无计数漂移**：容量占用与 hits/misses/expirations/evictions
  等计数只在写锁内或以原子变量修改；并发容量边界精确（放满即拒，不多不少）。
- **过期回收的并发安全**：读线程惰性回收过期条目时使用 CAS（`map.remove(key, entry)`），
  只有条目仍是同一个过期对象时才删除，绝不误删并发写入的新值。
- **批量写原子性**：`batch-put` 先校验全部键值、回收过期、核算容量，全部通过才生效；
  任一条件不满足则**整体拒绝**，已有数据与计数完全不变。

## 过期与淘汰规则

1. **过期判定**：`now >= expireAtMillis`，到期瞬间即过期；`ttlMillis <= 0` 表示永不过期。
2. **回收时机**（确定性）：
   - 惰性：读命中已过期键时 CAS 回收，返回 `EXPIRED`；
   - 主动：每次写入前、显式调用 `purge-expired`、容量判定前，先回收全部已过期条目。
3. **容量不足不静默丢数据**：回收过期条目后仍放不下时，写入被拒绝并返回
   `CAPACITY_FULL`——**未过期数据绝不因容量被静默淘汰**。覆盖已有键不占新槽位，
   容量满时仍可成功。
4. **状态可区分**：已被回收的过期键（物理删除）再访问返回 `NOT_FOUND`；
   已过期但尚未回收的键返回 `EXPIRED`。两者均可与彼此及 `OK` 明确区分。

## 持久化、恢复与容错边界

### WAL 格式

```
文件头: MAGIC(3B 'CLW') + VERSION(1B)
帧:    type(1B) | keyLen(4B) | valueLen(4B) | expireAtMillis(8B)
      | CRC32(4B，覆盖前述字段与 key/value 载荷) | key(UTF-8) | value(UTF-8)
```

- **写顺序**：先追加 WAL 并 `fsync`，再改内存。WAL 写失败则内存不变，调用方收到异常。
- **删除**：仅对存活条目的显式删除写 DELETE 记录；过期回收不写 WAL
  （重放时过期 PUT 按当时时钟跳过，语义等价）。

### 崩溃恢复

- 启动时顺序校验每一帧；遇到**帧头不完整、帧体截断、类型/长度非法或 CRC 不匹配**，
  立即停止重放，并将文件**截断到最后一条有效帧之后**——损坏尾部被识别并忽略，
  **不导致启动失败，也不会读出脏数据**；截断后文件可继续正常追加。
- 重放语义：PUT 覆盖、DELETE 删除、已过期 PUT 跳过；恢复后存活条目数超过容量上限时
  保留全部已恢复数据（上限只约束新写入），并输出告警日志。
- **容错边界**：WAL 头（MAGIC/VERSION）损坏时判定为不可识别文件并重建为空日志
  （原数据视为全部丢失，属已声明的边界）；磁盘级故障（fsync 之后数据仍丢失）
  不在本引擎保证范围内。
- **压缩**：`compact()` 用当前存活快照原子重写 WAL（临时文件 + fsync + 原子替换），
  丢弃被覆盖/已删除/已过期的历史记录。

## 配置项

| 配置 | 默认值 | 说明 |
|---|---|---|
| `cache.max-entries` | 1024 | 最大存活条目数；满且回收过期后仍放不下时写入返回 `CAPACITY_FULL` |
| `cache.data-file` | `data/cache.wal` | WAL 数据文件路径（目录自动创建） |
| `cache.compact-on-startup` | false | 启动恢复完成后是否立即压缩 WAL |
| `server.port` | 0（随机） | HTTP 服务端口 |

## 测试

`src/test/.../tests/` 下 33 个用例，日志中打印输入、判定依据与恢复结果：

- `ExpiryBoundaryTest`：到期瞬间边界、NOT_FOUND/EXPIRED 区分、过期槽位回收、重启后过期键缺席；
- `CapacityEvictionTest`：容量满拒绝且不丢存活数据、覆盖不占槽位、批量写原子性、统计一致性；
- `ConcurrencyTest`：同键并发写无丢失更新、并发容量边界精确、读写线性化（无回退）、
  混合操作无死锁、并发写后重启完整恢复；
- `RestartRecoveryTest`：正常重启恢复、删除持久化、撕裂写尾部截断、CRC 损坏帧及之后数据忽略、
  冷启动、压缩后状态保持、多轮重启稳定；
- `CacheApiTest`：REST 端到端，404/410/507 失败原因可区分，批量操作逐键结果。
