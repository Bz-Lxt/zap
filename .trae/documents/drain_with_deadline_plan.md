# 带硬期限的日志排空（Drain）实施计划

## 需求拆解

服务停机有硬期限，需要一条与普通 `Sync()` 不同的排空路径：

1. **排空边界**：只刷出 Drain 调用时刻*已接收*（已进入 BWS）的日志；调用之后并发到达的写入不属于本次排空。
2. **硬期限可中断**：Drain 接受 `context.Context`；期限到达后立即返回（即使底层输出阻塞），已在途的写操作允许在后台收尾。
3. **可续刷**：超时返回后再次调用 Drain，只接续未完成的段；已完成的段不重复写出。
4. **多输出并行**：底层为 `multiWriteSyncer`（或多个 BWS 组合）时，各输出的排空/同步并行推进，而非当前的顺序 `Sync`。
5. **排空期间写入不越界**：排空进行中并发写入照常接收（入内存段），但绝不被本次排空带出。
6. **普通 Sync 语义不变**：`Sync()` 仍刷出当时全部缓冲并调用 `WS.Sync()`；`Stop()` 行为不变；现有测试与 API 全部保持。

## Repository Research

- 目标代码全部在 [zapcore](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore)。
- [buffered_write_syncer.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/buffered_write_syncer.go)：`BufferedWriteSyncer` 目前用单个 `*bufio.Writer`，所有 `Write`/`Sync`/定时 flush/`Stop` 都在同一把 `mu` 下做物理 I/O。单缓冲无法表达"排空边界"，超时后也无法从中间接续。
- [write_syncer.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/write_syncer.go)：`multiWriteSyncer.Sync()` 顺序调用各子 syncer；`lockedWriteSyncer` 为 `*os.File` 等提供互斥。`WriteSyncer` 接口不能改（破坏外部实现者），新能力走**可选接口 + 类型断言**。
- `flushLoop` 每 30s 调一次 `Sync()`；`Stop()` 停 ticker、等 flushLoop 退出后 `Sync()`。
- 模块 `go 1.19`，可直接使用 `context`；已有 `multierr` 依赖。
- 测试约束：zapcore 与根包均有 `goleak.VerifyTestMain`，超时后遗留的后台 goroutine 必须能被后续 `Stop()`/续刷回收；现有 `FailWriter`/`ShortWriter` 的错误与短写语义需保持。
- 上游 zap（至 v1.28.0）无此 API，属于新增特性。

## 设计

### 对外 API（新增，不改既有签名）

```go
// zapcore/write_syncer.go
// DrainingWriteSyncer 是支持"带期限排空"的可选能力。
type DrainingWriteSyncer interface {
    WriteSyncer
    Drain(ctx context.Context) error
}
```

- `func (s *BufferedWriteSyncer) Drain(ctx context.Context) error`
- `func (ws multiWriteSyncer) Drain(ctx context.Context) error`：每个子输出一个 goroutine 并行排空，全部结束后汇总错误；`ctx.Done()` 时立即返回 `ctx.Err()`，在途子排空在后台继续结束（下次 Drain 续刷）。
- `func (s *lockedWriteSyncer) Drain(ctx context.Context) error`：保持 Lock 互斥语义，委托内部 Drain；内部不支持时用 goroutine+select 包住普通 `Sync`。
- BWS 末尾同步底层输出时：`WS` 实现了 `DrainingWriteSyncer` 则调 `Drain(ctx)`，否则用 goroutine+`select` 调 `Sync()`（超时常驻返回，底层 Sync 后台完成）。

### BWS 内部：分段缓冲（替换单个 bufio.Writer）

状态（均由 `mu` 保护，临界区内只做内存操作，不做 I/O）：

- `active []byte`：当前填充段（容量默认 Size=256kB）。
- `queued [][]byte`：已封口、按 FIFO 等待物理写出的段（每次拷贝一份，不持有用户切片）。
- `popped int`：累计已交付物理写的段序号，用于边界计数。
- `inflightSeg/inflightDone/inflightErr`：当前由脱离调用栈的后台 goroutine 正在写的一个段（超时返回后它仍能自行收尾）。
- `flushMu sync.Mutex`：物理写"协调者"锁，保证任一时刻只有一方在驱动写出（Drain / Sync / Write 溢出冲刷 / 定时 Sync 之间互斥），从而保持"BWS 无需外加 Lock 即可并发使用"的保证。

行为：

- **Write**：`mu` 下惰性 init；装不下则封口旧 active 入队；超大写入（>Size）独立成段。释放 `mu` 后若产生了排队段，获取 `flushMu` 以 `context.Background()` 冲刷当前队列（保持现有"写满即同步冲刷、短缓冲测试可观测"的语义），锁顺序始终为 flushMu→mu，不会 mu→flushMu。
- **Sync（语义不变）**：封口 active，目标 = 当时全部已排队段，用 `context.Background()` 协调刷完，再原样调用 `s.WS.Sync()`（多输出仍顺序，与今天一致）。
- **Drain**：
  1. `mu` 下封口 active，记录边界 `target = popped + len(queued)`（此后入队的段序号 ≥ target，天然越不过边界）；
  2. 持 `flushMu` 协调写出至 `popped >= target`：每段交给后台 goroutine 做 `writeAll`（循环处理短写，按 io 契约以返回 n 推进，出错把未写字节重新置于队首、记录错误），调用方 `select { done | ctx.Done() }`；超时直接返回，inflight 段保留，下次 Drain 先 join 它再继续剩余段——已完成段不再重复；
  3. 段刷完后按上面的规则带 ctx 同步底层 WS。
- **Stop / flushLoop**：代码路径不变；`Sync()` 会 join 残留的 inflight 后台写，保证 goroutine 不泄露、最终数据不丢。

## Files and Modules

- [zapcore/buffered_write_syncer.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/buffered_write_syncer.go)：分段缓冲重写；新增 `Drain`、内部协调者 `flushTo`/`waitInflight`/`writeSegment`、`syncUnderlying`；更新类型文档注释。`Write`/`Sync`/`Stop` 对外语义保持。
- [zapcore/write_syncer.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/write_syncer.go)：新增 `DrainingWriteSyncer` 接口、`multiWriteSyncer.Drain`（并行）、`lockedWriteSyncer.Drain`。
- [zapcore/buffered_write_syncer_test.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/buffered_write_syncer_test.go)：新增 Drain 系列测试（边界、超时、续刷无重复、并发写入不越界、Sync 语义、init 前/Stop 后）。
- [zapcore/write_syncer_test.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5310fe2-9bc0-4ea2-93f6-2884bfd1bc34/repo/zapcore/write_syncer_test.go)：新增多输出并行 Drain、超时返回、错误聚合、locked 透传测试。

## Implementation Steps

1. `write_syncer.go`：定义 `DrainingWriteSyncer`；实现 `multiWriteSyncer.Drain`（WaitGroup + 每输出一 goroutine + select ctx，成功路径用 `multierr.Append` 汇总；超时路径不读错误切片以免竞争）；实现 `lockedWriteSyncer.Drain`。
2. `buffered_write_syncer.go`：将单 `bufio.Writer` 替换为 active/queued 分段模型与 inflight 状态；重写 `Write`、`Sync`，保持 `initialize`/`flushLoop`/`Stop` 外部行为；实现协调者与 `Drain`、底层同步辅助函数；补 godoc。
3. 在既有两个测试文件中追加测试（含可阻塞 gate writer），覆盖六条需求。
4. 全部通过后运行验证命令。

## Dependencies and Considerations

- 不改任何现有接口签名；新能力只通过可选接口暴露，向后兼容。
- 锁层级固定为 `flushMu → mu`；`mu` 临界区永不做 I/O；后台写 goroutine 只取 `mu`，超时返回后也能自行收尾，杜绝自死锁。
- 超大写入由"零拷贝直通"变为拷贝成独立段（一次拷贝；普通写入本就拷贝进缓冲），换取排空期间的序列化安全；对基准中小消息场景无影响。
- 单输出内段必须严格按序写出（只有一个协调者，inflight 完成后才投递下一段）；多输出之间才并行。
- ctx 已取消时子 Drain 快速失败、由外层下一次带新 ctx 的调用续刷，会在 godoc 中说明"在途 Write/Sync 可能在后台完成"。

## Validation

- `go build ./...`、`go vet ./...`
- `go test -race -count=1 ./zapcore/...`
- `go test -race -count=1 ./...`（含根包 goleak）
- `go test -run '^$' -bench BenchmarkBufferedWriteSyncer -benchtime=1000x ./zapcore/` 冒烟

## Risks

- **goleak**：超时路径会留下后台写 goroutine → 测试中必须放开 gate 并以 `Stop()` 收尾（Stop→Sync 会 join inflight）；生产侧文档提示停机时 Stop 兜底。
- **短写/写错误语义回归**：沿用 io 契约（n 始终推进、错误时残段重回队首），对照现有 `flush error`/`stop twice` 用例验证。
- **并发竞争**：`-race` 必跑；多输出超时返回时不触碰各 goroutine 仍在写的错误槽。
