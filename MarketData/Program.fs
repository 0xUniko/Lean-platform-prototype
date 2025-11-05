namespace MarketData

open System
open Argu
open QuantConnect

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

    // ---- Strongly-typed enums for CLI ----
    type AssetKind =
        | Crypto
        | CryptoFuture

    type FuturesKind =
        | Um // USDT-M
        | Cm // Coin-M

    let assetKindToSecurityType =
        function
        | AssetKind.Crypto -> SecurityType.Crypto
        | AssetKind.CryptoFuture -> SecurityType.CryptoFuture

    let assetKindName =
        function
        | AssetKind.Crypto -> "crypto"
        | AssetKind.CryptoFuture -> "cryptofuture"

    let futuresKindName =
        function
        | FuturesKind.Um -> "um"
        | FuturesKind.Cm -> "cm"

    // Data kind: trade bars vs quote (not implemented yet for quote)
    type DataKind =
        | Trade
        | Quote

    let dataKindName =
        function
        | DataKind.Trade -> "trade"
        | DataKind.Quote -> "quote"

    let parseAssetKind (sOpt: string option) =
        match sOpt |> Option.map (fun s -> s.Trim().ToLowerInvariant()) with
        | None -> AssetKind.Crypto
        | Some "crypto" -> AssetKind.Crypto
        | Some "cryptofuture" -> AssetKind.CryptoFuture
        | Some other -> failwithf "Unknown asset: %s. Allowed: crypto | cryptofuture" other

    // Validate and parse futures kind. Only valid when asset is CryptoFuture.
    let parseFuturesKind (asset: AssetKind) (sOpt: string option) =
        match asset, sOpt |> Option.map (fun s -> s.Trim().ToLowerInvariant()) with
        | AssetKind.Crypto, None -> None
        | AssetKind.Crypto, Some _ -> failwith "--futures/-f is only valid when --asset cryptofuture"
        | AssetKind.CryptoFuture, None -> Some FuturesKind.Um // default
        | AssetKind.CryptoFuture, Some "um" -> Some FuturesKind.Um
        | AssetKind.CryptoFuture, Some "cm" -> Some FuturesKind.Cm
        | AssetKind.CryptoFuture, Some other -> failwithf "Unknown futures kind: %s. Allowed: um | cm" other

    let parseDataKind (sOpt: string option) =
        match sOpt |> Option.map (fun s -> s.Trim().ToLowerInvariant()) with
        | None -> DataKind.Trade
        | Some "trade" -> DataKind.Trade
        | Some "quote" -> DataKind.Quote
        | Some other -> failwithf "Unknown kind: %s. Allowed: trade | quote" other

    /// 将用户输入规范化为 Lean 的 Market/Asset；不认识的直接报错
    let makeSymbol (marketOpt: string option) (assetOpt: AssetKind option) (ticker: string) =
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
            match assetOpt with
            | Some a -> assetKindToSecurityType a
            | None -> SecurityType.Crypto

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
            | Connection _ -> "数据库连接串，默认 Data Source=marketdata.duckdb"
            | Kind _ -> "数据类型：trade 或 quote（默认 trade）"
            | Asset _ -> "资产类型：crypto（现货）或 cryptofuture（合约），默认 crypto"
            | Futures _ -> "合约市场：um(USDT-M) 或 cm(Coin-M)，仅当 -a cryptofuture 时有效；默认 um"

type ListArgs =
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member _.Usage = "列出库内已有 (market/security/ticker/res) 及行数"

type VacuumArgs =
    | [<AltCommandLine("-c")>] Connection of string

    interface IArgParserTemplate with
        member _.Usage = "VACUUM 压缩数据库"

[<CliPrefix(CliPrefix.None)>]
type Commands =
    | Data of ParseResults<DownloadArgs>
    | List of ParseResults<ListArgs>
    | Vacuum of ParseResults<VacuumArgs>

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Data _ -> "下载并写入历史行情（trade bars）"
            | List _ -> "列出库内已有的 (market/security/ticker/res) 及行数"
            | Vacuum _ -> "VACUUM 压缩数据库"

// ------------------------------
// 子命令实现
// ------------------------------
module private Impl =
    open Parse

    let runDownload (args: ParseResults<DownloadArgs>) =
        let conn = args.TryGetResult DownloadArgs.Connection

        let res = args.TryGetResult DownloadArgs.Resolution |> parseRes

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

        // Validate and parse enum-like options
        let kind = args.TryGetResult Kind |> parseDataKind
        let assetKind = args.TryGetResult Asset |> parseAssetKind
        let futuresKindOpt = args.TryGetResult Futures |> parseFuturesKind assetKind

        for tkr in syms do
            let symbol = makeSymbol mkt (Some assetKind) tkr

            printfn
                "Downloading %s (%s, %s) %A [%s .. %s) ..."
                tkr
                (assetKindName assetKind)
                (dataKindName kind)
                res
                (start.ToString("u"))
                (fin.ToString("u"))

            // Only trade bars are implemented currently
            if kind = Quote then
                failwith "Kind=quote is not supported yet"
            elif assetKind = CryptoFuture then
                let fk = futuresKindOpt |> Option.get |> futuresKindName
                let saved = BinanceDownloader.fetchFutures symbol res range fk conn
                printfn "Saved %d bars for %s" saved tkr
            else
                let saved = BinanceDownloader.fetchSpot symbol res range conn
                printfn "Saved %d bars for %s" saved tkr

    let runList (args: ParseResults<ListArgs>) =
        let conn = args.TryGetResult ListArgs.Connection
        let rows = DuckDbStore.listInstruments conn

        if rows.Length = 0 then
            printfn "No data."
        else
            printfn "market   security  ticker        res   start (UTC)             end (UTC)"

            for m, s, t, r, startT, endT in rows do
                printfn "%-8s %-8s %-12s %-5s %s %s" m s t r (startT.ToString("u")) (endT.ToString("u"))

    let runVacuum (args: ParseResults<VacuumArgs>) =
        let conn = args.TryGetResult VacuumArgs.Connection
        DuckDbStore.vacuum conn
        printfn "VACUUM done."

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
            ArgumentParser.Create<Commands>(errorHandler = errorHandler)

        let isHelpArg =
            function
            | null -> false
            | s when String.Equals(s, "--help", StringComparison.OrdinalIgnoreCase) -> true
            | s when String.Equals(s, "-h", StringComparison.OrdinalIgnoreCase) -> true
            | s when String.Equals(s, "help", StringComparison.OrdinalIgnoreCase) -> true
            | _ -> false

        let hasHelpArg (args: string array) = args |> Array.exists isHelpArg

        let printRootUsage () =
            parser.PrintUsage() |> printfn "%s"

        let printCommandUsage (cmd: string) =
            match cmd.Trim().ToLowerInvariant() with
            | "data" ->
                ArgumentParser
                    .Create<DownloadArgs>(errorHandler = errorHandler)
                    .PrintUsage()
                |> printfn "%s"
            | "list" ->
                ArgumentParser
                    .Create<ListArgs>(errorHandler = errorHandler)
                    .PrintUsage()
                |> printfn "%s"
            | "vacuum" ->
                ArgumentParser
                    .Create<VacuumArgs>(errorHandler = errorHandler)
                    .PrintUsage()
                |> printfn "%s"
            | _ -> printRootUsage ()

        match argv with
        | [||] ->
            printRootUsage ()
            0
        | _ when hasHelpArg argv && isHelpArg argv[0] ->
            printRootUsage ()
            0
        | _ when argv.Length > 0 && hasHelpArg argv[1..] ->
            printCommandUsage argv[0]
            0
        | _ ->
            let results = parser.Parse(argv, raiseOnUsage = false)

            match results.GetSubCommand() with
            | Data sub ->
                Impl.runDownload sub
                0
            | List sub ->
                Impl.runList sub
                0
            | Vacuum sub ->
                Impl.runVacuum sub
                0
