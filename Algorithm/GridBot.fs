namespace Algorithm

open System
open QuantConnect
open QuantConnect.Algorithm
open QuantConnect.Data
open QuantConnect.Data.Market
open QuantConnect.Orders
open QuantConnect.Securities

/// 最小网格策略：
/// - 开盘时若没有挂单，则在当前价上下各挂一档（±stepPct）
/// - 买单成交 -> 立刻在+stepPct价位挂同数量的卖单
/// - 卖单成交 -> 立刻在-stepPct价位挂同数量的买单
/// - 仅作为连通性/流程验证（不考虑风控/复用/持仓管理）
type GridBot() =
    inherit QCAlgorithm()

    // 配置参数（可直接改，或用 TOML parameters 注入后在 Initialize 读取）
    let stepPct = 0.002m // 0.2% 网格间距
    let qty = 0.001m // 0.001 BTC 每档

    // 运行时状态
    let mutable symbol = Unchecked.defaultof<Symbol>
    
    // 将价格按该标的最小跳动单位进行取整，避免被引擎事后改价并产生告警
    let roundToTick (alg: QCAlgorithm) (sym: Symbol) (price: decimal) =
        let sec = alg.Securities.[sym]
        let inc = sec.PriceVariationModel.GetMinimumPriceVariation(new GetMinimumPriceVariationParameters(sec, price))
        if inc > 0m then Math.Round(price / inc) * inc else price

    override this.Initialize() =
        // 回测时间请与你的 SQLite 数据覆盖范围一致（这里仅兜底，真正以 TOML 的 start/end 为准）
        this.SetStartDate(2024, 8, 30)
        this.SetEndDate(2025, 8, 30)

        this.SetAccountCurrency "USDT"
        this.SetCash("USDT", 10_000m)

        // 使用 Binance 的 BTCUSDT 分钟级
        symbol <- this.AddCrypto("BTCUSDT", Resolution.Minute, Market.Binance).Symbol

        // 预热一小时，避免刚启动就下单
        this.SetWarmup(TimeSpan.FromMinutes(60.))

    override this.OnData(slice: Slice) =
        if this.IsWarmingUp then
            ()
        else if not (slice.Bars.ContainsKey(symbol)) then
            ()
        else

            // 当前价（decimal）
            let price = slice.Bars.[symbol].Close |> decimal

            // 若没有任何未完成订单，就在当前价±stepPct 各挂一笔
            if this.Transactions.GetOpenOrders(symbol).Count = 0 then
                let buyPx = price * (1m - stepPct) |> roundToTick this symbol
                let sellPx = price * (1m + stepPct) |> roundToTick this symbol
                this.LimitOrder(symbol, +qty, buyPx) |> ignore
                this.LimitOrder(symbol, -qty, sellPx) |> ignore

    override this.OnOrderEvent(e: OrderEvent) =
        if e.Status = OrderStatus.Filled && e.Symbol = symbol then
            if e.Direction = OrderDirection.Buy then
                // 买入成交：在+stepPct 处挂同量卖单
                let tp = e.FillPrice * (1m + stepPct) |> roundToTick this symbol
                this.LimitOrder(symbol, -qty, tp) |> ignore
            elif e.Direction = OrderDirection.Sell then
                // 卖出成交：在-stepPct 处挂同量买单
                let rp = e.FillPrice * (1m - stepPct) |> roundToTick this symbol
                this.LimitOrder(symbol, +qty, rp) |> ignore
