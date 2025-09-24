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

    let private downloadBatch
        (client: BinanceRestClient)
        (symbol: Symbol)
        (resolution: Resolution)
        (interval: KlineInterval)
        (period: TimeSpan)
        (rangeToUtc: DateTime)
        (fromT: DateTime)
        (retries: int)
        : Result<TradeBar array * DateTime, string> =
        let limit = 1000

        printfn "[%s] Fetching %s %A klines from %O (retry=%d)"
            (DateTime.UtcNow.ToString("u")) symbol.Value resolution fromT retries

        let resp =
            client.SpotApi.ExchangeData
                .GetKlinesAsync(symbol.Value, interval, fromT, Nullable rangeToUtc, Nullable limit)
                .GetAwaiter()
                .GetResult()

        if not resp.Success then
            Error(string resp.Error)
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

            printfn "[%s] Got %d bars for %s (from %O)"
                (DateTime.UtcNow.ToString("u")) bars.Length symbol.Value fromT

            let nextFrom =
                match Array.tryLast bars with
                | None -> rangeToUtc
                | Some b -> b.Time + period

            Thread.Sleep 100

            Ok(bars, nextFrom)

    /// Keep Seq.unfold; synchronous blocking inside
    let fetch (symbol: Symbol) (resolution: Resolution) (range: DateRange) =
        use client = new BinanceRestClient()
        let interval = toInterval resolution
        let period = periodOf resolution

        // unfold needs a purely synchronous step function
        let step (fromT: DateTime, retries: int) =
            if fromT >= range.ToUtc then
                None
            else
                match downloadBatch client symbol resolution interval period range.ToUtc fromT retries with
                | Ok(bars, nextFrom) ->
                    if Array.isEmpty bars then
                        None
                    else
                        Some(bars, (nextFrom, 0))
                | Error errorMessage ->
                    printfn "[%s] Failed to fetch %s at %O: %s"
                        (DateTime.UtcNow.ToString("u")) symbol.Value fromT errorMessage

                    if retries < 5 then
                        Thread.Sleep 1000
                        Some(Array.empty, (fromT, retries + 1))
                    else
                        raise (Exception $"Binance klines error: {errorMessage}")

        Seq.unfold step (range.FromUtc, 0) |> Seq.collect id |> Seq.toArray
