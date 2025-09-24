My [Lean](https://github.com/QuantConnect/Lean) platform prototyp

TODO:
仿照Lean cli以项目为主的管理方式，把我这里的Algorithm拆离出去

疑似Lean扩展的标准名称叫做plugins

把Launcher和Data的功能和cli分离，Launcher用Avalonia/Fun.Blazor加个webui可以代替cli与功能交互，webui还可以批量进行管理

Launcher在跑之前要编译一下算法

Lean所用到的数据，有多少种类型，分别是什么样的格式，都可以从哪里获取


下面按“数据类型 → 文件格式/目录结构 → 获取途径”给你一张速览表。

核心行情类型

TradeBar: 成交价OHLCV，撮合与多数策略基础。分辨率 tick/second/minute/hour/daily。
QuoteBar: 买卖盘顶层(Bid/Ask)的OHLC以及最后一笔挂单量，用于更真实的成交/滑点。
Tick: 逐笔成交或逐笔报价，精度最高、体量也最大。
OpenInterest: 期货/期权未平仓量，通常日频或更低。
期货/期权价格: 同样以 TradeBar/QuoteBar/Tick 表示，按合约与到期日组织。
自定义数据: 任何 CSV/JSON 等（经济数据、情绪、链上数据…），由算法自定义解析。
辅助与参考数据

Factor Files: 拆分/分红调整系数，用于后复权等。
Map Files: 股票换代码/并表等映射，保证历史连贯性。
Coarse/Fine Fundamentals: 粗/细基本面（市值、行业、财务科目等），用于选股与风格过滤。
交易所日历/交易时段: 交易时段、假期、盘后等。
Symbol Properties: 最小报价跳动、合约乘数、最小下单量等。
目录结构与文件格式（Lean 标准）

根目录: data-folder（示例：lean/Data）
分辨率组织规则
分钟/秒: 按“天”为单位分片
路径示例: /crypto/binance/minute/btcusdt/20240830_trade.zip
Zip 内条目名: 用 LeanData.GenerateZipEntryName(...) 生成
TradeBar 行格式: millis,open,high,low,close,volume
QuoteBar 行格式: millis,bidO,bidH,bidL,bidC,lastBidSize,askO,askH,askL,askC,lastAskSize
小时/日: 单个符号一个 zip
路径示例: /equity/usa/hour/spy.zip, /equity/usa/daily/spy.zip
Tick 数据: 路径类似但文件/字段更细；不同资产类别列格式略有差异（成交/报价各自包含价格、数量及附加字段）。
辅助数据
Factor files: equity/usa/factor_files/{symbol}.csv
Map files: equity/usa/map_files/{symbol}.csv
Fundamentals: 通常在 equity/usa/fundamental/... 下（coarse/fine 各有目录）
资产类别前缀
Equity: equity/{market}/...
Forex: forex/{market}/...
Crypto: crypto/{market}/...
Future: future/{market}/...
Option: option/{market}/...
Alternative: alternative/...（各种另类数据）
数据获取途径

QuantConnect Data Library: 官方数据商店，覆盖主流资产与多种另类/基本面数据，格式即为 Lean 标准，开箱即用。
交易所公开存档
Crypto: 如 Binance Vision（bookTicker/aggTrades/klines/depth 等原始数据），可自聚合为 Lean 所需的 Trade/QuoteBar。
期货/期权/股票: 各交易所或第三方公开档案，通常需转换格式。
商业数据供应商/API
如 Polygon, Tiingo, IEX Cloud, CME DataMine 等，需按其许可与 API 拉取后落盘为 Lean 标准目录。
自建数据管道
你的工程已实现 SQLite → Zip on-the-fly（Lean.Extension/MyDataProvider.fs + LeanSqlite.MarketData/SqliteStore.fs），可继续扩展生成 _quote.zip、小时/日线聚合以及 tick。
需要的话，我可以：

在 MyDataProvider.fs 增加 _quote.zip 的生成（用你现有分钟 TradeBar 合成对称 Quote，或接入你将来从 Binance bookTicker 聚合的数据）。
写一个小工具把 Binance Vision 的 bookTicker/aggTrades 聚合并导出为 Lean 的分钟 Quote/Trade zip。