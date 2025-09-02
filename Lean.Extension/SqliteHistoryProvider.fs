namespace Lean.Extension

open System
open System.Collections.Generic
open System.IO
open NodaTime
open QuantConnect
open QuantConnect.Data
open QuantConnect.Data.Market
open QuantConnect.Configuration
open LeanSqlite.MarketData

type SqliteHistoryProvider() =
    inherit HistoryProviderBase()

    let mutable count = 0
    let mutable connStr : string = null

    override _.DataPointCount = count

    override this.Initialize(parameters: HistoryProviderInitializeParameters) =
        // 拿到 Lean 注入的各种 Provider
        let cache = parameters.DataCacheProvider
        let maps = parameters.MapFileProvider
        let factors = parameters.FactorFileProvider
        // 这里可以读 config.json 里的连接串；若未提供，则尝试基于 PWD 环境变量推断仓库根
        // 推荐在 TOML 中显式设置 `sqlite-connection`，避免路径不一致
        let pwd = Environment.GetEnvironmentVariable("PWD")
        let inferred =
            if String.IsNullOrWhiteSpace pwd then
                // 回退到当前工作目录（通常是 runs/.../timestamp），不可靠，仅作最后兜底
                Path.Combine(Directory.GetCurrentDirectory(), "marketdata.db")
            else
                Path.Combine(pwd, "LeanSqlite.MarketData", "marketdata.db")

        let defaultConn = $"Data Source={inferred}"
        connStr <- Config.Get("sqlite-connection", defaultConn)
        ()

    override this.GetHistory(requests, sliceTimeZone: DateTimeZone) =
        // 最小实现：仅支持 TradeBar，忽略 FillForward/公司行为等
        let buckets = Dictionary<DateTime, ResizeArray<BaseData>>() // key: utc slice time (bar EndTime UTC)
        let mutable total = 0

        for req in requests do
            // 仅处理 TradeBar 类型
            if req.DataType = typeof<TradeBar> then
                let res = req.Resolution
                let per = res.ToTimeSpan()

                // 查询时间窗（UTC，右开区间）
                let range : Domain.DateRange =
                    { FromUtc = req.StartTimeUtc; ToUtc = req.EndTimeUtc }

                let bars : TradeBar[] = SqliteStore.query (Some connStr) req.Symbol res range

                for b in bars do
                    // DB 中的 b.Time 为 UTC 起始；Slice 时间取 EndTime 的 UTC
                    let utcEnd = b.Time.ToUniversalTime() + per
                    let timeLocal = utcEnd.ConvertFromUtc(sliceTimeZone)

                    // 生成以本地时间为起点的 TradeBar，以便算法侧使用本地时间
                    let tb = TradeBar(timeLocal - per, req.Symbol, b.Open, b.High, b.Low, b.Close, b.Volume, per)

                    let ok, lst = buckets.TryGetValue utcEnd
                    let list = if ok then lst else let l = ResizeArray<BaseData>() in buckets.[utcEnd] <- l; l
                    list.Add(tb :> BaseData)
                    total <- total + 1

        count <- total

        // 产生按时间排序的 Slice 序列
        seq {
            for utc in buckets.Keys |> Seq.sort do
                let data = buckets.[utc] |> Seq.toList
                let timeLocal = utc.ConvertFromUtc(sliceTimeZone)
                yield new Slice(timeLocal, data, utc)
        }
