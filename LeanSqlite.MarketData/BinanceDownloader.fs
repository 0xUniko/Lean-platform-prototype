namespace LeanSqlite.MarketData

open System
open System.Threading
open Binance.Net.Clients
open Binance.Net.Enums
open QuantConnect
open QuantConnect.Data.Market

[<RequireQualifiedAccess>]
module BinanceDownloader =

    let private toInterval =
        function
        | Resolution.Minute -> KlineInterval.OneMinute
        | Resolution.Hour -> KlineInterval.OneHour
        | Resolution.Daily -> KlineInterval.OneDay
        | r -> invalidArg (nameof r) $"Unsupported: %A{r}"

    /// 保留 Seq.unfold；内部同步阻塞调用
    let fetch (symbol: Symbol) (resolution: Resolution) (range: DateRange) =
        use client = new BinanceRestClient()
        let interval = toInterval resolution
        let limit = 1000
        let period = periodOf resolution

        // unfold 需要一个纯同步的 step 函数
        let step (fromT: DateTime, retries: int) =
            if fromT >= range.ToUtc then
                None
            else
                printfn "[%s] Fetching %s %A klines from %O (retry=%d)"
                    (DateTime.UtcNow.ToString("u")) symbol.Value resolution fromT retries

                let resp =
                    client.SpotApi.ExchangeData
                        .GetKlinesAsync(symbol.Value, interval, fromT, Nullable range.ToUtc, Nullable limit)
                        .GetAwaiter()
                        .GetResult()

                if not resp.Success then
                    printfn "[%s] ❌ Failed to fetch %s at %O: %s"
                        (DateTime.UtcNow.ToString("u")) symbol.Value fromT (string resp.Error)

                    if retries < 5 then
                        Thread.Sleep 1000
                        Some(Array.empty, (fromT, retries + 1))
                    else
                        raise (Exception $"Binance klines error: {resp.Error}")
                else
                    let bars =
                        resp.Data
                        |> Array.Parallel.map (fun k ->
                            TradeBar(
                                k.OpenTime.ToUniversalTime(),
                                symbol,
                                k.OpenPrice,
                                k.HighPrice,
                                k.LowPrice,
                                k.ClosePrice,
                                int64 k.Volume,
                                period
                            ))

                    printfn "[%s] ✅ Got %d bars for %s (from %O)"
                        (DateTime.UtcNow.ToString("u")) bars.Length symbol.Value fromT

                    // 推进到下一页：最后一根 + period
                    let nextFrom =
                        match Array.tryLast bars with
                        | None -> range.ToUtc
                        | Some b -> b.Time + period

                    Thread.Sleep 100

                    if Array.isEmpty bars then
                        None
                    else
                        Some(bars, (nextFrom, 0))

        Seq.unfold step (range.FromUtc, 0) |> Seq.collect id |> Seq.toArray
