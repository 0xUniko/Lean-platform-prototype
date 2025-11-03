namespace LeanDuckDb.MarketData

open System
open System.Threading
open Binance.Net.Clients
open Binance.Net.Enums
open Binance.Net.Interfaces
open CryptoExchange.Net.Objects
open QuantConnect
open QuantConnect.Data.Market

[<RequireQualifiedAccess>]
module BinanceDownloader =

    type private DownloadContext =
        { Label: string
          FetchPage: DateTime -> WebCallResult<IBinanceKline[]> }

    let private maxRetries = 5
    let private limit = 1000

    let private toInterval =
        function
        | Resolution.Minute -> KlineInterval.OneMinute
        | Resolution.Hour -> KlineInterval.OneHour
        | Resolution.Daily -> KlineInterval.OneDay
        | r -> invalidArg (nameof r) $"Unsupported: %A{r}"

    let private downloadBatch
        (ctx: DownloadContext)
        (symbol: Symbol)
        (resolution: Resolution)
        (period: TimeSpan)
        (rangeToUtc: DateTime)
        (fromT: DateTime)
        (retries: int)
        : Result<TradeBar array * DateTime, string> =

        printfn
            "[%s] %s fetch %s %A from %O (retry=%d)"
            (DateTime.UtcNow.ToString("u"))
            ctx.Label
            symbol.Value
            resolution
            fromT
            retries

        let resp = ctx.FetchPage fromT

        if not resp.Success then
            Result.Error(string resp.Error)
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

            printfn
                "[%s] %s got %d bars for %s (from %O)"
                (DateTime.UtcNow.ToString("u"))
                ctx.Label
                bars.Length
                symbol.Value
                fromT

            let nextFrom =
                match Array.tryLast bars with
                | None -> rangeToUtc
                | Some b -> b.Time + period

            Thread.Sleep 100

            Result.Ok(bars, nextFrom)

    let private streamBatches
        (ctx: DownloadContext)
        (symbol: Symbol)
        (resolution: Resolution)
        (period: TimeSpan)
        (range: DateRange)
        =

        let rangeFrom = range.FromUtc
        let rangeTo = range.ToUtc

        let rec nextState (fromT: DateTime, retries: int) =
            if fromT >= rangeTo then
                None
            else
                match downloadBatch ctx symbol resolution period rangeTo fromT retries with
                | Result.Ok(bars, nextFrom) ->
                    if Array.isEmpty bars then
                        None
                    else
                        Some(bars, (nextFrom, 0))
                | Result.Error errorMessage ->
                    printfn
                        "[%s] %s fetch failed %s at %O: %s"
                        (DateTime.UtcNow.ToString("u"))
                        ctx.Label
                        symbol.Value
                        fromT
                        errorMessage

                    if retries < maxRetries then
                        Thread.Sleep 1000
                        nextState (fromT, retries + 1)
                    else
                        raise (Exception $"Binance {ctx.Label.ToLowerInvariant()} klines error: {errorMessage}")

        Seq.unfold nextState (rangeFrom, 0)

    let fetchSpot (symbol: Symbol) (resolution: Resolution) (range: DateRange) (connection: string option) : int =

        use client = new BinanceRestClient()
        let interval = toInterval resolution
        let period = periodOf resolution
        let rangeToUtc = range.ToUtc

        let fetchPage (fromT: DateTime) : WebCallResult<IBinanceKline[]> =
            client.SpotApi.ExchangeData
                .GetKlinesAsync(symbol.Value, interval, fromT, Nullable rangeToUtc, Nullable limit)
                .GetAwaiter()
                .GetResult()

        let ctx =
            { Label = "Spot"
              FetchPage = fetchPage }

        streamBatches ctx symbol resolution period range
        |> Seq.fold
            (fun total bars ->
                DuckDbStore.upsert connection symbol resolution bars
                total + bars.Length)
            0

    /// futuresKind: "um" (USDT-M) | "cm" (Coin-M); persists each batch immediately
    let fetchFutures
        (symbol: Symbol)
        (resolution: Resolution)
        (range: DateRange)
        (futuresKind: string)
        (connection: string option)
        : int =

        use client = new BinanceRestClient()
        let interval = toInterval resolution
        let period = periodOf resolution
        let rangeToUtc = range.ToUtc

        let fk =
            if String.IsNullOrWhiteSpace futuresKind then
                "um"
            else
                futuresKind.Trim().ToLowerInvariant()

        let fetchPage (fromT: DateTime) : WebCallResult<IBinanceKline[]> =
            if fk = "cm" then
                client.CoinFuturesApi.ExchangeData
                    .GetKlinesAsync(symbol.Value, interval, fromT, Nullable rangeToUtc, Nullable limit)
                    .GetAwaiter()
                    .GetResult()
            else
                client.UsdFuturesApi.ExchangeData
                    .GetKlinesAsync(symbol.Value, interval, fromT, Nullable rangeToUtc, Nullable limit)
                    .GetAwaiter()
                    .GetResult()

        let ctx =
            { Label = "Futures"
              FetchPage = fetchPage }

        streamBatches ctx symbol resolution period range
        |> Seq.fold
            (fun total bars ->
                DuckDbStore.upsert connection symbol resolution bars
                total + bars.Length)
            0
