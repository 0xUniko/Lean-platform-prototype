// dotnet run --project LeanSqlite.MarketData -- data -a cryptofuture -s BTCUSDT -m binance -r minute -b 2024-08-30 -e 2025-08-31 -k trade

namespace LeanSqlite.MarketData

open System
open Argu
open Microsoft.EntityFrameworkCore
open QuantConnect
open QuantConnect.Data.Market

// ------------------------------
// 解析辅助
// ------------------------------
module private Parse =
    let defaultMarket = "binance"

    let parseDate (s: string) =
        let ok, dt = DateTime.TryParse s

        if ok then
            dt.ToUniversalTime()
        else
            let s' = if s.Length = 8 then $"{s[0..3]}-{s[4..5]}-{s[6..7]}" else s
            DateTime.Parse(s').ToUniversalTime()

    let parseRes =
        function
        | None -> Resolution.Minute
        | Some(s: string) ->
            match s.Trim().ToLowerInvariant() with
            | "tick" -> Resolution.Tick
            | "second" -> Resolution.Second
            | "minute" -> Resolution.Minute
            | "hour" -> Resolution.Hour
            | "daily" -> Resolution.Daily
            | _ -> failwithf "Unknown resolution: %s" s

    /// 将用户输入规范化为 Lean 的 Market/Asset；不认识的直接报错
    let makeSymbol (marketOpt: string option) (assetOpt: string option) (ticker: string) =
        let market =
            match marketOpt with
            | Some m -> m
            | None -> defaultMarket

        let mkt =
            match market.Trim().ToLowerInvariant() with
            | "binance"
            | "bn" -> Market.Binance
            | "binanceus"
            | "bnus"
            | "bus" -> Market.BinanceUS
            | "gdax"
            | "coinbase"
            | "coinbasepro"
            | "cb" -> Market.Coinbase
            | "kraken" -> Market.Kraken
            | "bitfinex" -> Market.Bitfinex
            // —— 常见外汇经纪商 ——（Lean 常量）
            | "oanda" -> Market.Oanda
            | "fxcm" -> Market.FXCM
            // —— 美国股票（聚合市场）——
            | "usa"
            | "us" -> Market.USA
            // 其余未知：提示用户
            | other ->
                failwithf
                    "Unknown/unsupported market: %s. Try one of: binance, binanceus, gdax, kraken, bitfinex, oanda, fxcm, usa"
                    other
        let asset =
            match assetOpt |> Option.map (fun s -> s.Trim().ToLowerInvariant()) with
            | Some "cryptofuture" -> SecurityType.CryptoFuture
            | _ -> SecurityType.Crypto

        Symbol.Create(ticker.ToUpperInvariant(), asset, mkt)

    let todayUtcRightOpen () = DateTime.UtcNow.Date.AddDays 1.0 // 右开区间上界

// ------------------------------
// 子命令参数
// ------------------------------
type DownloadArgs =
    | [<AltCommandLine("-s")>] Symbol of string
    | [<AltCommandLine("-m")>] Market of string
    | [<AltCommandLine("-r")>] Resolution of string
    | [<AltCommandLine("-b")>] Start of string
    | [<AltCommandLine("-e")>] End_ of string
    | [<AltCommandLine("-c")>] Connection of string
    | [<AltCommandLine("-k")>] Kind of string // trade | quote
    | [<AltCommandLine("-a")>] Asset of string // crypto | cryptofuture
    | [<AltCommandLine("-f")>] Futures of string // um | cm (for cryptofuture)

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Symbol _ -> "交易对（可多次）例：-s BTCUSDT -s ETHUSDT"
            | Market _ -> "市场，默认 Binance"
            | Resolution _ -> "Tick/Second/Minute/Hour/Daily，默认 Minute"
            | Start _ -> "起始日期（UTC，YYYY-MM-DD 或 YYYYMMDD）"
            | End_ _ -> "结束日期（UTC，默认为今天的次日，右开区间）"
            | Connection _ -> "SQLite 连接串，默认 Data Source=marketdata.db"
            | Kind _ -> "数据类型：trade 或 quote（默认 trade）"
            | Asset _ -> "资产类型：crypto（现货）或 cryptofuture（合约），默认 crypto"
            | Futures _ -> "合约市场：um(USDT-M) 或 cm(Coin-M)，仅当 -a cryptofuture 时有效；默认 um"

type ListArgs =
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member _.Usage = "列出库内已有 (market/security/ticker/res) 及行数"

type StatsArgs =
    | [<AltCommandLine("-s")>] Symbol of string
    | [<AltCommandLine("-m")>] Market of string
    | [<AltCommandLine("-r")>] Resolution of string
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Symbol _ -> "交易对（如 BTCUSDT）"
            | Market _ -> "市场（默认 Binance）"
            | Resolution _ -> "K线周期（默认 Minute）"
            | Connection _ -> "SQLite 连接串（默认 Data Source=marketdata.db）"

type VacuumArgs =
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member _.Usage = "VACUUM 压缩数据库"

type VerifyArgs =
    | [<AltCommandLine("-s")>] Symbol of string
    | [<AltCommandLine("-m")>] Market of string
    | [<AltCommandLine("-r")>] Resolution of string
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Symbol _ -> "交易对（如 BTCUSDT）"
            | Market _ -> "市场（默认 Binance）"
            | Resolution _ -> "K线周期（默认 Minute）"
            | Connection _ -> "SQLite 连接串（默认 Data Source=marketdata.db）"

[<CliPrefix(CliPrefix.None)>]
type Commands =
    | Data of ParseResults<DownloadArgs>
    | List of ParseResults<ListArgs>
    | Stats of ParseResults<StatsArgs>
    | Vacuum of ParseResults<VacuumArgs>
    | Verify of ParseResults<VerifyArgs>

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Data _ -> "下载并写入历史行情（trade bars）"
            | List _ -> "列出库内已有的 (market/security/ticker/res) 及行数"
            | Stats _ -> "查看某合约的时间范围与行数"
            | Vacuum _ -> "VACUUM 压缩数据库"
            | Verify _ -> "检查连续性（简单缺口检测）"

// ------------------------------
// 子命令实现
// ------------------------------
module private Impl =
    open Parse

    let runDownload (args: ParseResults<DownloadArgs>) =
        let conn = args.TryGetResult DownloadArgs.Connection

        let res = args.TryGetResult DownloadArgs.Resolution |> parseRes
        let kind = args.TryGetResult DownloadArgs.Kind |> Option.defaultValue "trade" |> fun s -> s.Trim().ToLowerInvariant()

        let mkt = args.TryGetResult DownloadArgs.Market

        let syms =
            match args.GetResults DownloadArgs.Symbol with
            | [] -> failwith "至少需要一个 --symbol"
            | xs -> xs

        let start =
            args.TryGetResult Start
            |> Option.map parseDate
            |> Option.defaultValue (DateTime.UtcNow.AddYears -1)

        let fin =
            args.TryGetResult End_
            |> Option.map parseDate
            |> Option.defaultValue (todayUtcRightOpen ())

        let range =
            { Domain.DateRange.FromUtc = start
              ToUtc = fin }

        let asset = args.TryGetResult DownloadArgs.Asset |> Option.defaultValue "crypto" |> fun s -> s.Trim().ToLowerInvariant()
        let fut = args.TryGetResult DownloadArgs.Futures |> Option.defaultValue "um" |> fun s -> s.Trim().ToLowerInvariant()

        for tkr in syms do
            let symbol = makeSymbol mkt (Some asset) tkr
            printfn "Downloading %s (%s) %A [%s .. %s) ..." tkr asset res (start.ToString("u")) (fin.ToString("u"))
            let bars: TradeBar[] =
                if asset = "cryptofuture" then
                    BinanceFuturesDownloader.fetch symbol res range fut
                else
                    BinanceDownloader.fetch symbol res range
            SqliteStore.upsert conn symbol res bars
            printfn "Saved %d bars for %s" bars.Length tkr

    let runList (args: ParseResults<ListArgs>) =
        let conn = args.TryGetResult ListArgs.Connection
        let rows = SqliteStore.listInstruments conn

        if rows.Length = 0 then
            printfn "No data."
        else
            printfn "market   security  ticker   res   count"

            for m, s, t, r, cnt in rows do
                printfn "%-8s %-8s %-8s %-5s %8d" m s t r cnt

    let runStats (args: ParseResults<StatsArgs>) =
        let conn = args.TryGetResult StatsArgs.Connection

        let mkt = args.TryGetResult StatsArgs.Market

        let res = args.TryGetResult StatsArgs.Resolution |> parseRes

        let tkr =
            match args.TryGetResult StatsArgs.Symbol with
            | Some s -> s
            | None -> failwith "需要 --symbol"

        let symbol = makeSymbol mkt None tkr

        match SqliteStore.datasetStats conn symbol res with
        | None -> printfn "No rows for %s %A." tkr res
        | Some(fromT, toT, n) -> printfn "%s %A: %d bars [%s .. %s]" tkr res n (fromT.ToString("u")) (toT.ToString("u"))

    let runVacuum (args: ParseResults<VacuumArgs>) =
        let conn = args.TryGetResult VacuumArgs.Connection

        use ctx = new MarketDataContext(mkOptions conn)
        ctx.Database.EnsureCreated() |> ignore
        ctx.Database.ExecuteSqlRaw "VACUUM;" |> ignore
        printfn "VACUUM done."

    let runVerify (args: ParseResults<VerifyArgs>) =
        let conn = args.TryGetResult Connection
        let mkt = args.TryGetResult Market
        let res = args.TryGetResult Resolution |> parseRes

        let tkr =
            match args.TryGetResult <@ Symbol @> with
            | Some s -> s
            | None -> failwith "需要 --symbol"

        let symbol = makeSymbol mkt None tkr
        let per = periodOf res

        let all =
            SqliteStore.queryBars
                conn
                symbol
                res
                { FromUtc = DateTime.MinValue
                  ToUtc = DateTime.MaxValue }

        if all.Length <= 1 then
            printfn "Rows: %d (nothing to verify)" all.Length
        else
            let mutable gaps = 0

            for i = 1 to all.Length - 1 do
                let expected = all[i - 1].Time + per

                if all[i].Time <> expected then
                    gaps <- gaps + 1
                    printfn "Gap between %s and %s" (all[i - 1].Time.ToString("u")) (all[i].Time.ToString("u"))

            if gaps = 0 then
                printfn "No gaps. Rows=%d" all.Length
            else
                printfn "Found %d gaps. Rows=%d" gaps all.Length

// ------------------------------
// 程序入口
// ------------------------------
module Program =
    [<EntryPoint>]
    let main argv =
        let errorHandler =
            ProcessExiter(
                colorizer =
                    function
                    | ErrorCode.HelpText -> None
                    | _ -> Some ConsoleColor.Red
            )

        let parser =
            ArgumentParser.Create<Commands>(programName = "leansqlite", errorHandler = errorHandler)

        if argv.Length = 0 then
            printfn
                "Usage: leansqlite <command> [options]\nCommands: data | list | stats | vacuum | verify\nUse --help on each for details."

            0
        else
            let results = parser.Parse(argv, raiseOnUsage = false)

            match results.GetSubCommand() with
            | Data sub ->
                Impl.runDownload sub
                0
            | List sub ->
                Impl.runList sub
                0
            | Stats sub ->
                Impl.runStats sub
                0
            | Vacuum sub ->
                Impl.runVacuum sub
                0
            | Verify sub ->
                Impl.runVerify sub
                0
