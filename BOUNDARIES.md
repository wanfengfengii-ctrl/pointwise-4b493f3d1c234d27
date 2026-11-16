# 状态归属与公开边界

本文件说明本轮交付的组件职责、公开端口、完成点与取消交接。调用方只需
`Collector(channel, trace)` 与 `Collector.iter_file(...)`；其余端口供独立运行与核验使用，
不要求调用方访问内部状态。

## 状态归属

| 状态 | 唯一持有者 | 说明 |
| --- | --- | --- |
| channel、请求编号 `serial`、在途表 `_pending`、接收任务 `_pump` | `broker.RequestBroker` | 编号从 1 递增，到 `0xffffffff` 后 `OverflowError('request ids exhausted')`；不接收、不解释偏移/EOF/槽位/交付规则 |
| 每个属主的在途编号集合、空闲事件 | `broker.BrokerRoute` | 每个属主独占一个 route；broker 只按编号把回复交给对应 route |
| 句柄、`cursor`、`next_offset`、`_slots` 槽位拼装、`_eof`、`_close_error`、交付与清理 | `reader.FileReader` | 不读编号表、不碰通道、不感知其他文件；只经 route 端口收发 |
| 参数校验与装配 | `collector.Collector` | 仅 `iter_file` 校验并新建 `FileReader`，`aclose` 检查“还有未关闭文件”后关闭 broker |
| 不可变结果 `Reply` 与端口协议 | `wire` | 只有数据定义，无业务执行器 |

两个组件不共享可变请求记录：broker 的在途表只存 `(route, operation, tag)`，
reader 只持有自己的槽位表；二者的唯一关联是不可变的 `Reply` 及其中不透明的 `tag`
（reader 选用自己的块起始偏移作为 tag，broker 原样带回）。

## 公开代理端口（`RequestBroker` → 属主）

`RequestBroker(channel, trace)` 只使用 `channel.send(packet)` 与 `await channel.recv()`。

`RequestBroker.open_route(handler, notify) -> BrokerRoute`：为调用方建立一个独占路由。
`handler(reply)` 与 `notify()` 均为同步回调，由 broker 的接收任务调用。

| 成员 | 效果完成点 |
| --- | --- |
| `route.submit_open(path)` | 报文已交给 `channel.send`，该编号进入在途表与 route 的在途集合 |
| `route.submit_read(handle, offset, length, tag)` | 同上；`offset`/`length` 只作为操作参数编码，代理不推导文件规则 |
| `route.submit_close(handle)` | 同上 |
| `route.outstanding` | 该 route 已发出但尚无回复的请求数 |
| `route.broken` | 连接失败对象，未失败为 `None` |
| `await route.wait_idle()` | 该 route 在途集合为空；若期间连接失败则抛出该失败 |

回复路由：broker 收到一个完整报文后先 `codec.decode`、记 `['recv'] + 已解码报文`、
按编号从在途表移除并同步调用 `handler(Reply(...))`，随后调用 `notify()`，然后才继续
`recv` 下一报文。因此 handler 内的即时补读发生在消费下一回复之前。

`Reply`（`wire.Reply`，冻结数据类）：`operation` 为 `'OPEN'|'READ'|'CLOSE'`，
`kind` 为 `'HANDLE'|'DATA'|'STATUS'`，`tag` 为提交时给出的不透明关联值，
`payload` 为 HANDLE/DATA 字节，`code`/`message` 为 STATUS 值。结果所有权在
`handler` 返回后归调用方；broker 不再持有或修改它。

## 公开读取端口（`FileReader` → 代理）

`FileReader(broker, path, block, window, trace)` 只调用 `broker.open_route(handler, notify)`
以及上表端口。它可作为 `async for` 迭代器，支持 `aclose()`。用仅实现上表端口的替身
（`component_checks.StubBroker`/`StubRoute`）即可独立运行，无需真实 broker、channel 或 Collector。

## 取消与清理交接

- 取消 `__anext__` 或调用 `aclose` 时，reader 先置停止标志并记 `['stop', path, 主异常类名或 '']`，
  此后到达的 DATA/STATUS 直接丢弃且不补读。
- reader 调 `await route.wait_idle()` 等“已发请求全部收清”（已收到的本地数据不算在途），
  再 `submit_close` 并再次 `wait_idle()`；只有这些之后才记 `['closed', path]` 并让
  取消/关闭协程返回或抛出。
- 主异常（读取错误或 `CancelledError`）优先于 CLOSE 失败；无主异常时 CLOSE 失败抛出；
  清理结束后重复 `aclose` 不再抛出旧错误。
- 其他文件继续使用同一 broker：它们的 route、在途集合与槽位互不影响。

## 依赖方向与装配

`broker.py` 不导入 `reader.py`/`collector.py`；`reader.py` 不导入 `broker.py`/`collector.py`；
`wire.py` 只依赖标准库数据类型。`Collector` 无基类、无 `_receive`/`_next`/`_fill`/`_finish`
旧链、无备用解释器、无全局注册表或反射执行路径，只做校验、装配与 `aclose` 转发。

`checks.verify` 在保留原有 GOLDEN 摘要、`byte_reference` 与断言的前提下追加
`component_checks.verify_components()`：用替身实际驱动乱序回复、空闲期间短读修补、
较后错误不得越前、取消收清后 CLOSE、关闭失败与幂等，以及 broker 的独占编号/跨属主路由；
并静态核对依赖方向与装配面。
