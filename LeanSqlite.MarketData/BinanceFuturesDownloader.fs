namespace LeanSqlite.MarketData

open System
open System.Threading
open Binance.Net.Clients
open Binance.Net.Enums
open QuantConnect
open QuantConnect.Data.Market

[<RequireQualifiedAccess>]
module BinanceFuturesDownloader =

    let private toInterval =
        function
        | Resolution.Minute -> KlineInterval.OneMinute
        | Resolution.Hour -> KlineInterval.OneHour
        | Resolution.Daily -> KlineInterval.OneDay
        | r -> invalidArg (nameof r) $"Unsupported: %A{r}"

    /// futuresKind: "um" (USDT-M) | "cm" (Coin-M)
    let fetch (symbol: Symbol) (resolution: Resolution) (range: DateRange) (futuresKind: string) =
        use client = new BinanceRestClient()
        let interval = toInterval resolution
        let limit = 1000
        let period = periodOf resolution
        let fk = (if String.IsNullOrWhiteSpace futuresKind then "um" else futuresKind).Trim().ToLowerInvariant()

        let getPage (fromT: DateTime) =
            if fk = "cm" then
                client.CoinFuturesApi.ExchangeData
                    .GetKlinesAsync(symbol.Value, interval, fromT, Nullable range.ToUtc, Nullable limit)
                    .GetAwaiter().GetResult()
            else
                client.UsdFuturesApi.ExchangeData
                    .GetKlinesAsync(symbol.Value, interval, fromT, Nullable range.ToUtc, Nullable limit)
                    .GetAwaiter().GetResult()

        let step (fromT: DateTime, retries: int) =
            if fromT >= range.ToUtc then None else
            printfn "[%s] Futures Fetch %s %A from %O (retry=%d, %s)"
                (DateTime.UtcNow.ToString("u")) symbol.Value resolution fromT retries fk
            let resp = getPage fromT
            if not resp.Success then
                printfn "[%s] ❌ Futures fetch failed %s at %O: %s" (DateTime.UtcNow.ToString("u")) symbol.Value fromT (string resp.Error)
                if retries < 5 then
                    Thread.Sleep 1000
                    Some(Array.empty, (fromT, retries + 1))
                else
                    failwithf "Binance futures klines error: %O" resp.Error
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
                printfn "[%s] ✅ Futures got %d bars for %s (from %O)"
                    (DateTime.UtcNow.ToString("u")) bars.Length symbol.Value fromT
                let nextFrom =
                    match Array.tryLast bars with
                    | None -> range.ToUtc
                    | Some b -> b.Time + period
                Thread.Sleep 100
                if Array.isEmpty bars then None else Some(bars, (nextFrom, 0))

        Seq.unfold step (range.FromUtc, 0) |> Seq.collect id |> Seq.toArray
