namespace Lean.Extension

open System
open System.IO
open System.IO.Compression
open System.Text
open System.Globalization
open System.Text.RegularExpressions
open QuantConnect
open QuantConnect.Configuration
open QuantConnect.Interfaces
open QuantConnect.Logging

/// A custom data provider that, for equity minute data requests,
/// generates a ZIP stream on the fly containing constant OHLCV bars for the day.
/// For any other request it falls back to reading from local disk.
type MyDataProvider() as this =
    // One-time warning flag for missing data directory when falling back to disk
    let mutable oneTimeWarningLog = false

    // .NET event backing field
    let newDataRequest = new Event<EventHandler<DataProviderNewDataRequestEventArgs>, DataProviderNewDataRequestEventArgs>()

    // Regex to match crypto minute trade zip path: crypto/{market}/minute/{symbol}/{yyyymmdd}_trade.zip
    // Works with either '/' or '\\' as separators
    let cryptoMinuteZip = new Regex(@"crypto[\/\\](?<mkt>[a-z0-9_]+)[\/\\]minute[\/\\](?<sym>[a-z0-9]+)[\/\\](?<date>\d{8})_trade\.zip($|#)", RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
    // Regex to match cryptofuture minute trade zip path
    let cryptoFutureMinuteZip = new Regex(@"cryptofuture[\/\\](?<mkt>[a-z0-9_]+)[\/\\]minute[\/\\](?<sym>[a-z0-9]+)[\/\\](?<date>\d{8})_trade\.zip($|#)", RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
    

    // Build in-memory zip stream for a given date+symbol with constant OHLC values
    // Locate the sqlite db connection string like SqliteHistoryProvider does
    let getConnStr () =
        let pwd = Environment.GetEnvironmentVariable("PWD")
        let inferred =
            if String.IsNullOrWhiteSpace pwd then
                Path.Combine(Directory.GetCurrentDirectory(), "marketdata.db")
            else
                Path.Combine(pwd, "LeanSqlite.MarketData", "marketdata.db")
        let defaultConn = $"Data Source={inferred}"
        Config.Get("sqlite-connection", defaultConn)

    // Build in-memory zip from DB (crypto minute trade bars for a given date)
    let buildCryptoMinuteZipFromDb (market: string) (symbolLower: string) (date: DateTime) : Stream =
        let symbol = Symbol.Create(symbolLower.ToUpperInvariant(), SecurityType.Crypto, market)
        let res = Resolution.Minute
        let fromUtc = DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc)
        let toUtc = fromUtc.AddDays(1.0)
        let range : LeanSqlite.MarketData.Domain.DateRange = { FromUtc = fromUtc; ToUtc = toUtc }
        let bars : QuantConnect.Data.Market.TradeBar[] = LeanSqlite.MarketData.SqliteStore.queryBars (Some (getConnStr())) symbol res range

        let entryName = QuantConnect.Util.LeanData.GenerateZipEntryName(symbol, date, res, TickType.Trade)

        let ms = new MemoryStream()
        // Scope to ensure writer/entry are disposed before the archive,
        // and the archive is disposed before we rewind the MemoryStream
        do
            use zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen = true)
            let entry = zip.CreateEntry(entryName, CompressionLevel.Optimal)
            use es = entry.Open()
            use sw = new StreamWriter(es, Encoding.UTF8)

            for b in bars do
                // milliseconds since midnight for this row
                let millis = int (b.Time.TimeOfDay.TotalMilliseconds)
                // No scaling for crypto
                let o = b.Open.ToString(CultureInfo.InvariantCulture)
                let h = b.High.ToString(CultureInfo.InvariantCulture)
                let l = b.Low.ToString(CultureInfo.InvariantCulture)
                let c = b.Close.ToString(CultureInfo.InvariantCulture)
                let v = b.Volume.ToString(CultureInfo.InvariantCulture)
                sw.WriteLine(sprintf "%d,%s,%s,%s,%s,%s" millis o h l c v)

            // disposing 'sw' will flush to 'es' and then to the zip entry
            ()

        ms.Position <- 0L
        ms :> Stream

    // Build in-memory zip from DB (crypto future minute trade bars)
    let buildCryptoFutureMinuteZipFromDb (market: string) (symbolLower: string) (date: DateTime) : Stream =
        let symbol = Symbol.Create(symbolLower.ToUpperInvariant(), SecurityType.CryptoFuture, market)
        let res = Resolution.Minute
        let fromUtc = DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc)
        let toUtc = fromUtc.AddDays(1.0)
        let range : LeanSqlite.MarketData.Domain.DateRange = { FromUtc = fromUtc; ToUtc = toUtc }
        let bars : QuantConnect.Data.Market.TradeBar[] = LeanSqlite.MarketData.SqliteStore.queryBars (Some (getConnStr())) symbol res range

        let entryName = QuantConnect.Util.LeanData.GenerateZipEntryName(symbol, date, res, TickType.Trade)

        let ms = new MemoryStream()
        do
            use zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen = true)
            let entry = zip.CreateEntry(entryName, CompressionLevel.Optimal)
            use es = entry.Open()
            use sw = new StreamWriter(es, Encoding.UTF8)

            for b in bars do
                let millis = int (b.Time.TimeOfDay.TotalMilliseconds)
                let o = b.Open.ToString(CultureInfo.InvariantCulture)
                let h = b.High.ToString(CultureInfo.InvariantCulture)
                let l = b.Low.ToString(CultureInfo.InvariantCulture)
                let c = b.Close.ToString(CultureInfo.InvariantCulture)
                let v = b.Volume.ToString(CultureInfo.InvariantCulture)
                sw.WriteLine(sprintf "%d,%s,%s,%s,%s,%s" millis o h l c v)
            ()
        ms.Position <- 0L
        ms :> Stream


    interface IDataProvider with
        /// Event raised each time data fetch is finished (successfully or not)
        [<CLIEvent>]
        member _.NewDataRequest = newDataRequest.Publish

        /// Retrieves data for Lean. For equity minute requests, returns constant bars; otherwise, reads from disk.
        member _.Fetch(key: string) : Stream =
            let mutable success = true
            let mutable errorMessage = String.Empty

            let result =
                try
                    // Handle crypto minute TRADE from sqlite market data
                    match cryptoMinuteZip.Match(key) with
                    | m when m.Success ->
                        let market = m.Groups.["mkt"].Value.ToLowerInvariant()
                        let sym = m.Groups.["sym"].Value.ToLowerInvariant()
                        let dateStr = m.Groups.["date"].Value
                        let ok, date = DateTime.TryParseExact(dateStr, "yyyyMMdd", null, Globalization.DateTimeStyles.None)
                        if not ok then failwithf "Invalid date in path: %s" key

                        buildCryptoMinuteZipFromDb market sym date
                    | _ ->
                    // Handle cryptofuture minute TRADE from sqlite
                    match cryptoFutureMinuteZip.Match(key) with
                    | fm when fm.Success ->
                        let market = fm.Groups.["mkt"].Value.ToLowerInvariant()
                        let sym = fm.Groups.["sym"].Value.ToLowerInvariant()
                        let dateStr = fm.Groups.["date"].Value
                        let ok, date = DateTime.TryParseExact(dateStr, "yyyyMMdd", null, Globalization.DateTimeStyles.None)
                        if not ok then failwithf "Invalid date in path: %s" key

                        buildCryptoFutureMinuteZipFromDb market sym date
                    | _ ->
                        // Fallback to local disk behavior
                        try
                            new FileStream(FileExtension.ToNormalizedPath(key), FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
                        with
                        | :? DirectoryNotFoundException as ex ->
                            success <- false
                            errorMessage <- ex.Message
                            if not oneTimeWarningLog then
                                oneTimeWarningLog <- true
                                Log.Debug(sprintf "MyDataProvider.Fetch(): DirectoryNotFoundException: please review data paths, current 'Globals.DataFolder': %s" Globals.DataFolder)
                            null :> Stream
                        | :? FileNotFoundException as ex ->
                            success <- false
                            errorMessage <- ex.Message
                            null :> Stream
                        | ex ->
                            success <- false
                            errorMessage <- ex.Message
                            reraise()
                finally
                    newDataRequest.Trigger(this, new DataProviderNewDataRequestEventArgs(key, success, errorMessage))

            result

    interface IDisposable with
        /// The stream is managed by the consumer; no resources to dispose here.
        member _.Dispose() = ()
