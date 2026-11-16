# 公告原文采集器：冻结行为与本轮交付

这是完整、可运行且业务回归通过的耦合实现，不含目标代理、读取器或交接方案。有限 SFTP v3 已协商连接由调用方提供；codec.py 是实际长度前缀报文编解码，portal.Channel 是可控录制通道。不是远程服务器部署题。

## 已有入口与输入输出

使用 Python 标准库。Collector(channel, trace) 来自 collector.py；trace 是调用方提供的列表。channel.send(packet) 同步接收一个完整 bytes 报文，返回 None；await channel.recv() 返回一个完整回复报文。send 不阻塞；recv 已有排队回复时可立即返回而不让出事件循环。合法输入是一条连接上的 OPEN、READ、CLOSE，以及 HANDLE、非空 DATA 和 STATUS；不增加协议操作或报文流拆包。

collector.iter_file(path, block=4, window=2) 返回异步 bytes 迭代器，支持 aclose()。path 为非空字符串；block、window 为严格正整数，bool 不算整数；非法参数抛 ValueError。文件是稳定的原文，正常 READ 的 DATA 为请求区间内的非空前缀且不超过请求长度，STATUS 1 是该请求位置的 EOF。其他非零 STATUS 为 SFTPError，公开 code 属性和原消息。OPEN 成功返回唯一句柄，失败返回非零 STATUS；CLOSE 返回 STATUS。响应编号匹配且每请求恰好回复一次；不要求处理不合法服务端、认证、重连、第二次取消或传输故障注入。

同文件按原 block 槽位顺序返回，最后一块可不足 block；空文件不返回块。多个文件共用同一连接且可同时拉取。同文件禁止并发 next/aclose，抛 ValueError('concurrent file operation')；取消正在进行的 next 是关闭该文件的入口。aclose 幂等，关闭后 next 直接结束。Collector.aclose() 在还有未关闭文件时抛 ValueError('files still open')，否则结束接收任务，之后调用方不再使用此 Collector。没有业务持久化或恢复格式。

trace 是公开观察面，包含 pull、send、recv、yield、stop、closed；精确列表格式见 samples.py 的实际输出。命令及回复用 codec.decode 的列表表示，句柄与正文用十六进制字符串。yield 的值对应本次实际返回块；stop 记录清理开始及主异常类型，closed 记录清理结束，包括失败清理。请求编号从 1 递增，不在连接内复用；到 32 位上界后抛 OverflowError('request ids exhausted')，不要求继续采集。

## 必须兼容的时点

短 DATA 必须在消费下一条连接回复之前立即补发剩余区间，即使该文件当前没有 next 等待；不得等调用方再拉取，也不得先消费已经排队的另一文件回复。补读仍属原槽位，只有 EOF 能终止不足一块的拼装。较后位置先到的 EOF 不得吞掉较前待拼装数据。

每次有效拉取先补足空闲槽位，再检查当前位置已收到的结果；已知 EOF 后不再开新槽位。后位置的错误不得越过较前数据。即使当前位置已有错误，若此前槽位交付腾出了窗口且尚未知 EOF，该次拉取仍先补发一个新槽位，随后进入错误收尾，不能以优化为由删掉这条命令。结果未交付期间不额外预取新槽位，短读修补除外。最后一次 yield 不关闭句柄，只有下次拉取发现结束或显式关闭才开始收尾。

取消或提前关闭先停止交付和新增读取；随后到达的 DATA 直接丢弃，不补读。该文件已发请求全部收清后才能发送 CLOSE，另一文件仍可处理回复、交付并关闭。取消等待必须等收清和 CLOSE 尝试后才返回 CancelledError。已收到但尚未交付的数据不能被当成仍在网络途中的回复；清掉本地结果也不能被当成网络已收清。

正文、分块、返回值、异常类型与消息、完整 trace 和每次拉取后的可见状态保持兼容。读取主异常或取消不得被 CLOSE 失败覆盖；没有主异常时 CLOSE 失败须抛出；清理结束后重复 close 不再抛出旧错误。每文件最多 window 个未交付槽位、window 个在途 READ，槽位正文总长度最多 block*window；不得用全文件缓存或停止其他文件来规避交接。

## 本轮结构交付

提供 broker.py 的 RequestBroker 与 reader.py 的 FileReader。连接代理独占通道、请求编号、在途请求和回复路由，不判断文件偏移、EOF、槽位或交付规则。文件读取器独占文件句柄生命周期、偏移、槽位拼装、错误及 EOF 裁决，只经公开代理端口发操作和接结果，不读写请求编号表或传输。请求参数可包含偏移与长度，代理只把它们作为操作参数交给既有编解码，不从中推导文件规则。

两组件不得共享可变请求记录；回复可传递不可变数据或明确转移的结果。broker.py 不导入 reader.py 或 collector.py，reader.py 不导入 broker.py 或 collector.py；共同的端口或数据定义可另设模块，但不能藏业务执行器。Collector 只装配和兼容转发，不保留旧 _receive、_next、_fill、_finish 业务链或另一路解释器。不得借继承、回调隐藏的旧实例、全局注册表或反射继续执行耦合逻辑。

公开代理端口的具体方法与结果交接形式由本次重构决定，须在 BOUNDARIES.md 写清签名、效果完成点、结果所有权与取消交接。不得事后要求调用方访问内部状态。连接代理必须可由仅实现 send/recv 的替身通道独立运行；文件读取器必须可由仅实现所公布代理端口的替身独立运行，不装配真实代理或通道。新增独立核验必须实际驱动乱序结果、空闲期间短读修补和取消收清，并确认只操作自己的状态，不能仅做 import 检查。

保留 checks.py 现有断言、GOLDEN 和 samples.py 录制事件，不得归一化或重录轨迹；允许 checks.verify 追加调用新核验。交付兼容代码及 BOUNDARIES.md 的简短状态归属说明，将独立组件、依赖方向和替身核验接入原 verify；不能只运行样例自称结构已通过。

## 运行与样例

.pointwise-runtime.json 是公开运行清单：version 为 1，kind 为 cli；setup 是 argv 数组的列表，verify 和 demo 分别是一个 argv 数组。现有 setup 为空；verify 为 python3 -B checks.py，demo 为 python3 -B samples.py。清单用于启动真实业务核验和录制演示，交付后仍须有效。

checks.py 的 SOURCES 提供公告、空文件和测量原文，独立按 bytes 切片核对多文件采集，逆序回复并故意给短 DATA。samples.py 是可重放输入：short 返回十六进制 616263；cancel 只返回 B 的 42424242，A 最终为 CancelledError，B 在 A 最后回复之前结束；error 返回 676f6f64 后抛 code=4、消息 read denied；early 返回 31323334 后显式关闭抛 close denied；burst 在 A 暂停拉取时先补读再接收 B 的排队回复，A 最终两块为 61626364、65666768，B 为 42424242。demo 输出所有真实结果和完整轨迹，verify 同时逐字节核对这些结果及冻结摘要。它们是通过的已有行为，不是待修缺陷。
