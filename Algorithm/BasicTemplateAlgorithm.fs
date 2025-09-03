namespace Algorithm

open QuantConnect
open QuantConnect.Algorithm
open QuantConnect.Data

/// F# 版本的 BasicTemplateAlgorithm，与 C# 示例等价：
/// - 设置回测时间、初始资金
/// - 订阅 SPY 分钟数据
/// - 首次收到数据后全仓买入
type BasicTemplateAlgorithm() =
    inherit QCAlgorithm()

    let spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA)

    override this.Initialize() =
        this.SetStartDate(2013, 10, 7)
        this.SetEndDate(2013, 10, 11)
        this.SetCash(100000m)

        // 订阅 SPY 分钟级别数据
        this.AddEquity("SPY", Resolution.Minute) |> ignore

    override this.OnData(slice: Slice) =
        if not this.Portfolio.Invested then
            this.SetHoldings(spy, 1m) |> ignore
            this.Debug("Purchased Stock")
