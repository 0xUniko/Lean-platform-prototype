# Lean Lean

## A lean wrapper of [Lean](https://github.com/QuantConnect/Lean), supporting onchain trading

### 项目结构

**Algorithm**: 算法，包括策略和数据分析脚本等

**Launcher**: 启动器，直接移植的Lean的Launcher，配置文件改成了toml格式，内容不变

example: dotnet run --project Launcher -- -c Launcher/backtesting.toml

**MarketData**: DuckDB 行情数据工具集，可单独使用

**Lean.Extension**: Lean扩展，比如把Lean回测的数据源换成本地的Data

### TODO:

需要创建一个Blazor项目作为管理后台，可以管理多个项目

涉及并实现编译部署跑实盘的架构与流程

### Lean代码解析

#### 运行流程

TODO:

#### 插件与扩展

TODO:

#### 数据结构

TODO:

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
你的工程已实现 DuckDB → Zip on-the-fly（Lean.Extension/MyDataProvider.fs + LeanDuckDb.MarketData/DuckDbStore.fs），可继续扩展生成 _quote.zip、小时/日线聚合以及 tick。
需要的话，我可以：

在 MyDataProvider.fs 增加 _quote.zip 的生成（用你现有分钟 TradeBar 合成对称 Quote，或接入你将来从 Binance bookTicker 聚合的数据）。
写一个小工具把 Binance Vision 的 bookTicker/aggTrades 聚合并导出为 Lean 的分钟 Quote/Trade zip。

我在Laucnher这里并没有导入Lean.Extension，为啥我在配置文件里面指定了data-provider，程序就能找到相应的data-provider

原因是 LEAN 用运行时的“按名称发现 + 组合”(MEF/Composer)机制来实例化组件，而不是靠你在代码里手动引用类型。

关键点

Launcher 已经项目级引用了 Lean.Extension，所以构建后 Lean.Extension.dll 会被复制到可执行目录，即使 Program.fs 里没有 open Lean.Extension 也没关系。参见 Launcher/Launcher.fsproj (line 24)。
引擎启动时读取配置项 data-provider，并通过 Composer.Instance.GetExportedValueByTypeName `<IDataProvider>`(…) 按“类型名”创建实现。参见 lean/Engine/LeanEngineAlgorithmHandlers.cs (line 209)。
Composer 会从可执行目录（以及可选的 plugin-directory）加载所有 *.dll，建立 MEF 目录并按需实例化。参见 lean/Common/Util/Composer.cs (line 86) 和方法 GetExportedValueByTypeName `<T>` 于 lean/Common/Util/Composer.cs (line 207)。
接口 QuantConnect.Interfaces.IDataProvider 带有 [InheritedExport(typeof(IDataProvider))]，意味着任何实现该接口的类（比如你的 Lean.Extension.MyDataProvider）都会自动成为可发现的导出，无需在类上再标注 Export，也无需手动在代码里引用。参见 lean/Common/Interfaces/IDataProvider.cs (line 26)。
配置里只写类型简单名就能匹配（也支持全名/程序集限定名），所以 data-provider = "MyDataProvider" 可以直接定位到你的实现。参见 Launcher/backtesting.toml (line 12)。
小结

你“没有导入 Lean.Extension”指的是没有在代码里 open/using 或 new；但项目已经引用并输出了该 DLL，Composer 扫描目录 + MEF 的 InheritedExport 让它自动被发现和按配置名实例化。
