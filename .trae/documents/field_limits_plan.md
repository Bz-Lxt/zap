# 单条日志业务字段数 / 嵌套层数限制 实施计划

## 需求拆解（Repository Research）

目标库为 **uber-go/zap**（Go 1.19，module `go.uber.org/zap`）。需求要点：

1. 内置输出可限制**每条记录**的业务字段总数与嵌套层数；
2. 同名字段每出现一次计一次（不去重）；
3. 继承字段（`logger.With`）、内联字段（`zap.Inline`）、反射内容（`zap.Reflect` / `AddReflected`）全部计入；
4. 超限后：停止展开后续字段、**整条记录拒绝落盘**、并发出报告；
5. 并发记录各自独立计数；
6. 检查不得导致二次取值（不得为计数再跑一遍 marshal/反射）；
7. 拒绝后 logger 仍可正常记录后续日志；
8. 未超限或未启用时，输出与现状逐字节一致。

### 关键代码路径（调研结论）

- [zapcore/core.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/core.go)：`ioCore.With` 调 `addFields(clone.enc, fields)`，继承字段在 With 时**一次性序列化进 encoder 的累积 buffer**；`ioCore.Write` 调 `enc.EncodeEntry`，返回 error 时不写出（天然支持“整条拒绝”），error 最终经 [zapcore/entry.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/entry.go#L274-L286) 的 `CheckedEntry.Write` 写入 `errorOutput`（`%v write error: %v`），是现成的文本报告通道。
- [zapcore/json_encoder.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/json_encoder.go)：
  - `EncodeEntry` 每次先 `enc.clone()` 在**副本**上编码 → 天然并发独立；失败只丢弃副本、不污染 `c.enc` → 拒绝后后续记录正常。
  - 所有键值字段都经 `addKey` 落盘；`AppendObject` / `AppendArray` 是容器；`OpenNamespace` 产生嵌套对象且在 `closeOpenNamespaces` 闭合；`InlineMarshalerType` 在 [zapcore/field.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/field.go#L122-L123) 中直接 `MarshalLogObject(enc)`，无键、不增层。
  - 元数据（level/time/name/caller/function/msg/stacktrace）在 `EncodeEntry` 中直接写入，不经过业务 `addFields`，可用“计数武装开关”区分，不计为业务字段。
  - 反射内容由 `encodeReflected` 调 `json.Encoder` 产出 JSON 字节；对**已产出的字节**做一次轻量词法扫描即可统计成员数/深度，无需二次反射取值。
- [zapcore/console_encoder.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/console_encoder.go)：内嵌 `*jsonEncoder`，`writeContext` 对其 clone 后 `addFields`，是 console 路径的拒绝点（需改为返回 error）。
- `addFields`（[zapcore/field.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/field.go#L208-L212)）是顶层字段循环唯一入口，可在此通过**可选私有接口**在已拒绝时提前 break，避免对后续 `Stringer`/`Error` 字段调用 `String()`/`Error()`（停止展开、不取值）。
- `EncoderConfig`（[zapcore/encoder.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/f5fcd170-e88a-4d4e-9f27-9267bbf7a4ca/repo/zapcore/encoder.go#L331-L359)）已有函数字段先例（`NewReflectedEncoder`），且被 `Config.Build` 自动透传，是放置开关与回调的正确位置；不改 `Encoder`/`Core` 接口，第三方 encoder 零影响。

### 计数口径（明确约定）

- **字段数**：累计（非每层）计数。每个 `Add*(key, …)` 计 1；重名各计 1；error 的 `key` / `key+"Verbose"` / `key+"Causes"`、marshal 失败追加的 `key+"Error"` 各计 1；inline 自身无键不计，其内部字段逐个计；reflect 顶层键计 1，反射 JSON 内每个对象成员再各计 1；数组元素（无名）不计字段。
- **层数**：根记录 `{}` 为 0；object、namespace、array 容器各加深 1 层（array 计入以限制解析资源占用，注释中明示）；**inline 不增层**；反射 JSON 按其中最大 `{`/`[` 嵌套深度计入。
- 元数据字段（level/time/name/caller/function/msg/stacktrace）不计。

## Files and Modules

- `zapcore/encoder.go`：`EncoderConfig` 增加 `MaxFields`、`MaxDepth`、`OnFieldLimit` 三个字段及注释。
- `zapcore/field_limit.go`（**新建**）：`FieldLimitReason`（字段超限/深度超限）、`FieldLimitError`（含 Reason/Limit/Fields/Depth/Key，实现 `error`、`Unwrap`）、哨兵 `ErrFieldLimitExceeded`。
- `zapcore/json_scan.go`（**新建**）：`scanJSONFieldUsage(data []byte) (members, maxDepth int)`，对已序列化 JSON 字节做零分配词法扫描（正确处理字符串/转义/空白/数字/字面量）。
- `zapcore/json_encoder.go`：核心计数与拒绝逻辑。
- `zapcore/field.go`：`addFields` 增加可选 `fieldLimitTracker` 提前中断。
- `zapcore/console_encoder.go`：`writeContext` 传入 `Entry` 并返回 error，拒绝时释放资源并上报。
- `zapcore/field_limit_test.go`（**新建**，白盒 `package zapcore`）：扫描器单测 + 计数口径单测。
- `zapcore/json_encoder_test.go` 或新建 `field_limit_ext_test.go`（`package zapcore_test`）：端到端行为测试（含继承/内联/反射/并发/console/拒绝后恢复）。

## Implementation Steps

1. **类型与配置**：在 `zapcore/field_limit.go` 定义 reason/error/哨兵；在 `EncoderConfig` 增加：
   - `MaxFields int`（`<=0` 不限制）：每记录业务字段上限；
   - `MaxDepth int`（`<=0` 不限制）：最大嵌套层数；
   - `OnFieldLimit func(Entry, FieldLimitError)`：拒绝回调（注释要求：并发安全、快速返回、不得保留 `Entry`）。
2. **JSON 扫描器**：实现 `scanJSONFieldUsage`，维护容器栈（`{`/`[`）与“当前是否期待 key”状态；输出对象成员总数与最大容器深度；扫描异常时返回 `(0,0)`（不因扫描故障误拒，反射本身的错误仍走原路径）。
3. **jsonEncoder 状态**：新增 `fieldCount int`、`depth int`、`rejected *FieldLimitError`、`counting bool`：
   - `newJSONEncoder` 置 `counting=true`；`clone()` 复制全部计数状态（继承字段/未闭 namespace 基线随之传递）；`putJSONEncoder` 重置新字段。
   - `limitsOn()` 守卫：`MaxFields<=0 && MaxDepth<=0` 时所有计数路径为廉价早退，行为/输出不变。
4. **计数点**：
   - `addKey`：`counting && limitsOn()` 时 `fieldCount++`，超过 `MaxFields` 则 `reject()`；`rejected!=nil` 后不再写入。
   - `OpenNamespace`：键照常计 1，随后 `depth++` 并做深度检查（namespace 不立即回退，由 `closeOpenNamespaces` 按数量回退 `depth`）。
   - `AppendObject`/`AppendArray`：包裹 `depth++/--` 与超限检查；`rejected` 时**不调用**用户 `MarshalLogObject`/`MarshalLogArray`，仅维持括号/深度平衡（buffer 终将丢弃）。
   - `AddObject`/`AddArray`：已拒绝时直接返回哨兵，不展开。
   - `AddReflected`/`AppendReflected`：保持“先反射一次”的现有顺序；反射成功后扫描字节，折叠成员数与 `depth+maxDepth` 深度检查；超限则拒绝且不把字节写入 buffer；不额外取值。
   - `closeOpenNamespaces`：按闭合数量回退 `depth`。
5. **停止展开**：`addFields` 做一次可选接口断言（`fieldLimitTracker{ FieldLimitReached() bool }`），循环顶部命中即返回；`MapObjectEncoder` 等不实现该接口，行为完全不变。
6. **EncodeEntry 拒绝收尾（JSON）**：`clone` 后先置 `counting=false`（元数据段不计数）；检查基线（继承字段）已超标则直接进入拒绝收尾；在拷贝 context buffer 与 `addFields` 前置 `counting=true`，结束后置 false（stacktrace 不计数）；收尾时若 `rejected!=nil`：`buf.Free()` → 调用 `OnFieldLimit(ent, *err)` → `putJSONEncoder` → `return nil, err`；正常路径保持现有代码与字节输出不变。
7. **Console 路径**：`writeContext(line, extra, ent) error`；`context` clone 后检查基线、`addFields` 后检查 `rejected`；拒绝时释放 context/line、回调上报并返回 error；`EncodeEntry` 透传该 error。
8. **报告通道**：`FieldLimitError` 经 `ioCore.Write → CheckedEntry.Write` 自动进 logger 的 `errorOutput`（无需改 core/logger）；`OnFieldLimit` 提供结构化回调，供指标统计等用途。

## Dependencies and Considerations

- Go 版本 1.19；仅用标准库与现有依赖，`go.mod` 不变。
- 不修改任何公开接口（`Encoder`/`Core`/`ObjectEncoder`），第三方 encoder 与 `zaptest/observer` 不受影响。
- SugaredLogger、`Dict`、errorGroup、`WithLazy`、sampler/tee/hook 等包装均经同一 `EncodeEntry` 路径，自动生效。
- Fatal 条目被拒时仍执行既有 after-hook（与现有“write 返回 error”语义一致），不做特殊处理。
- With 阶段继承字段即超限时：该 core 之后每条记录在 EncodeEntry 基线检查处被拒绝并按条目报告（无 Entry 时不在 With 处回调）。
- 新文件需带 Uber MIT license 头（`checklicense.sh`）。

## Validation

- `gofmt`/`go vet ./...`；`go build ./...`。
- 根模块 `go test -race ./...`（Makefile 即以此方式跑，竞态检测器覆盖并发独立性）。
- 新增测试覆盖：
  - 未启用 / 未超限：输出与现状逐字节一致；
  - 顶层字段数超限（含重名字段逐次计数）：返回 `*FieldLimitError`、sink 无输出、回调收到 reason/计数/Key、下一条正常；
  - 继承字段计入：`With` 后超限拒绝、未超限正常且包含继承字段；
  - inline：内部字段计数但不增层；object/namespace/array 深度限制；
  - reflect：map/嵌套/数组/深嵌套/`nil` 的计数与拒绝；
  - error/stringer 附加字段计入；超限后后续对象 marshaler 与 String() 不被调用（用计数器/标记验证“停止展开、不二次取值”）；
  - 并发：多 goroutine 混合超限/未超限，计数互不串扰（`-race`）；
  - console encoder 同样拒绝/报告/恢复；
  - 经 `zap.Logger`/`SugaredLogger` 端到端：超限行缺失、`errorOutput` 有报告、后续行存在。
- 可选：跑一遍现有 JSON/console encoder 基准确认未启用路径无明显回归。

## Risks

- **扫描器正确性**：反射输出由 `encoding/json` 保证合法；以表驱动单测覆盖转义字符串/嵌套/数组/标量/`null`；扫描故障保守放行。
- **禁用路径性能**：全部钩子以 `limitsOn()` 单一判断守卫，热路径仅多一次可内联的分支；基准对比确认。
- **回调风险**（慢/阻塞/panic/重入）：注释明确契约（并发安全、快速返回、不保留 Entry）；回调 panic 不额外 recover（与 zap 既有 hook 策略一致），文档提示。
- **口径争议**（array 是否计层、元数据不计、错误附加字段计）：全部在代码注释与测试中固定，避免后续歧义。
