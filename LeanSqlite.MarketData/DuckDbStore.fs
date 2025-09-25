namespace LeanSqlite.MarketData

open System
open System.Data
open System.Data.Common
open DuckDB.NET.Data
open QuantConnect
open QuantConnect.Data.Market

[<AutoOpen>]
module private Duck =
    [<Literal>]
    let private SourceDir = __SOURCE_DIRECTORY__

    // Default DB file placed next to this source file
    let private defaultDbPath = IO.Path.Combine(SourceDir, "marketdata.duckdb")

    // DuckDB ADO.NET connection string
    let private defaultConn = $"Data Source={defaultDbPath}"

    let connString (connStrOpt: string option) =
        connStrOpt |> Option.defaultValue defaultConn

    let ensureSchema (conn: DuckDBConnection) =
        use cmd = conn.CreateCommand()

        cmd.CommandText <-
            """
            CREATE TABLE IF NOT EXISTS Candles (
                Market   VARCHAR NOT NULL,
                Security VARCHAR NOT NULL,
                Ticker   VARCHAR NOT NULL,
                Res      VARCHAR NOT NULL,
                Time     TIMESTAMP NOT NULL,
                Open     DECIMAL(38, 18) NOT NULL,
                High     DECIMAL(38, 18) NOT NULL,
                Low      DECIMAL(38, 18) NOT NULL,
                Close    DECIMAL(38, 18) NOT NULL,
                Volume   BIGINT NOT NULL,
                PeriodS  INTEGER NOT NULL,
                PRIMARY KEY (Market, Security, Ticker, Res, Time)
            );
            """

        cmd.ExecuteNonQuery() |> ignore

    let resKey r = Domain.resKey r
    let periodOf r = Domain.periodOf r

    // Centralize column names to avoid duplication/typos
    module SqlParts =
        [<Literal>]
        let Table = "Candles"

        let Keys = [| "Market"; "Security"; "Ticker"; "Res"; "Time" |]
        let NonKeys = [| "Open"; "High"; "Low"; "Close"; "Volume"; "PeriodS" |]
        let All = Array.append Keys NonKeys
        let colsCsv (cols: string array) = String.Join(", ", cols)
        let keyCsv = colsCsv Keys
        let allCsv = colsCsv All
        let placeholders = All |> Array.mapi (fun idx _ -> sprintf "$%d" (idx + 1))
        let parameterNames = All |> Array.mapi (fun idx _ -> string (idx + 1))
        let insertValuesCsv = placeholders |> colsCsv
        let updateSetCsv = NonKeys |> Array.map (fun c -> $"{c} = EXCLUDED.{c}") |> colsCsv

        let insertOnConflictSql =
            $"INSERT INTO {Table} ({allCsv}) VALUES ({insertValuesCsv})\n"
            + $"ON CONFLICT ({keyCsv}) DO UPDATE SET {updateSetCsv};"

    let setParam (p: DbParameter) (name: string) (t: DbType) (v: obj) =
        p.ParameterName <- name
        p.DbType <- t
        p.Value <- v
        p

    let addParam (cmd: DuckDBCommand) (name: string) (t: DbType) (v: obj) =
        let p = cmd.CreateParameter()
        setParam p name t v |> ignore
        cmd.Parameters.Add(p) |> ignore

    let toParams (cmd: DuckDBCommand) (symbol: Symbol) (resolution: Resolution) (bar: TradeBar) =
        let per = periodOf resolution

        SqlParts.All
        |> Array.iteri (fun idx col ->
            let name = SqlParts.parameterNames[idx]

            let t, v =
                match col with
                | "Market" -> DbType.String, box symbol.ID.Market
                | "Security" -> DbType.String, box (symbol.ID.SecurityType.ToString().ToLowerInvariant())
                | "Ticker" -> DbType.String, box symbol.Value
                | "Res" -> DbType.String, box (resKey resolution)
                | "Time" -> DbType.DateTime, box (bar.Time.ToUniversalTime())
                | "Open" -> DbType.Decimal, box bar.Open
                | "High" -> DbType.Decimal, box bar.High
                | "Low" -> DbType.Decimal, box bar.Low
                | "Close" -> DbType.Decimal, box bar.Close
                | "Volume" -> DbType.Int64, box (int64 bar.Volume)
                | "PeriodS" -> DbType.Int32, box (int per.TotalSeconds)
                | _ -> DbType.Object, null

            addParam cmd name t v)

    let toTradeBar
        (symbol: Symbol)
        (per: TimeSpan)
        (time: DateTime)
        (o: decimal)
        (h: decimal)
        (l: decimal)
        (c: decimal)
        (v: int64)
        =
        TradeBar(time, symbol, o, h, l, c, v, per)


// ---------------------------
// Public API: Upsert / Query (DuckDB)
// ---------------------------
[<RequireQualifiedAccess>]
module DuckDbStore =

    /// Batch upsert executing per row within a single transaction for reliable DuckDB parameter binding
    let upsert (connStr: string option) (symbol: Symbol) (resolution: Resolution) (bars: TradeBar array) : unit =
        if bars.Length = 0 then
            ()
        else
            use conn = new DuckDBConnection(connString connStr)
            conn.Open()
            ensureSchema conn

            use tx = conn.BeginTransaction()

            let exec bar =
                use cmd = conn.CreateCommand()
                cmd.Transaction <- tx
                cmd.CommandText <- Duck.SqlParts.insertOnConflictSql
                Duck.toParams cmd symbol resolution bar
                cmd.ExecuteNonQuery() |> ignore

            Array.iter exec bars

            tx.Commit()


    /// Distinct list of (Market, Security, Ticker, Res) with counts
    let listInstruments (connStr: string option) : (string * string * string * string * int64) array =
        use conn = new DuckDBConnection(connString connStr)
        conn.Open()
        ensureSchema conn

        use cmd = conn.CreateCommand()

        cmd.CommandText <-
            """
            SELECT Market, Security, Ticker, Res, COUNT(*)
            FROM Candles
            GROUP BY Market, Security, Ticker, Res
            ORDER BY Market, Security, Ticker, Res;
            """

        use reader = cmd.ExecuteReader()
        let buf = Collections.Generic.List<_>()

        while reader.Read() do
            let m = reader.GetString 0
            let s = reader.GetString 1
            let t = reader.GetString 2
            let r = reader.GetString 3
            let c = reader.GetInt64 4
            buf.Add(m, s, t, r, c)

        buf.ToArray()


    /// Stats: (minTime, maxTime, count) for a symbol/resolution
    let datasetStats (connStr: string option) (symbol: Symbol) (resolution: Resolution) =
        use conn = new DuckDBConnection(connString connStr)
        conn.Open()
        Duck.ensureSchema conn

        use cmd = conn.CreateCommand()

        cmd.CommandText <-
            $"SELECT MIN(Time), MAX(Time), COUNT(*) FROM {Duck.SqlParts.Table} WHERE Market=$1 AND Security=$2 AND Ticker=$3 AND Res=$4;"

        Duck.addParam cmd "1" DbType.String (box symbol.ID.Market)
        Duck.addParam cmd "2" DbType.String (box (symbol.ID.SecurityType.ToString().ToLowerInvariant()))
        Duck.addParam cmd "3" DbType.String (box symbol.Value)
        Duck.addParam cmd "4" DbType.String (box (Domain.resKey resolution))

        use reader = cmd.ExecuteReader()

        if reader.Read() then
            let cnt = reader.GetInt64(2)

            if cnt = 0L then
                None
            else
                let minT =
                    if reader.IsDBNull(0) then
                        Nullable()
                    else
                        reader.GetFieldValue<DateTime> 0 |> Nullable

                let maxT =
                    if reader.IsDBNull 1 then
                        Nullable()
                    else
                        reader.GetFieldValue<DateTime> 1 |> Nullable

                Some(minT.Value, maxT.Value, int cnt)
        else
            None


    /// Query TradeBars in ascending time order for a given range
    let queryBars
        (connStr: string option)
        (symbol: Symbol)
        (resolution: Resolution)
        (range: Domain.DateRange)
        : TradeBar array =
        use conn = new DuckDBConnection(Duck.connString connStr)
        conn.Open()
        Duck.ensureSchema conn

        let per = Domain.periodOf resolution

        use cmd = conn.CreateCommand()

        cmd.CommandText <-
            $"SELECT Time, Open, High, Low, Close, Volume FROM {Duck.SqlParts.Table} WHERE Market=$1 AND Security=$2 AND Ticker=$3 AND Res=$4 AND Time>=$5 AND Time<$6 ORDER BY Time ASC;"

        Duck.addParam cmd "1" DbType.String (box symbol.ID.Market)
        Duck.addParam cmd "2" DbType.String (box (symbol.ID.SecurityType.ToString().ToLowerInvariant()))
        Duck.addParam cmd "3" DbType.String (box symbol.Value)
        Duck.addParam cmd "4" DbType.String (box (Domain.resKey resolution))
        Duck.addParam cmd "5" DbType.DateTime (box range.FromUtc)
        Duck.addParam cmd "6" DbType.DateTime (box range.ToUtc)

        use reader = cmd.ExecuteReader()
        let rows = System.Collections.Generic.List<TradeBar>()

        while reader.Read() do
            let t = reader.GetFieldValue<DateTime>(0)
            let o = reader.GetFieldValue<decimal>(1)
            let h = reader.GetFieldValue<decimal>(2)
            let l = reader.GetFieldValue<decimal>(3)
            let c = reader.GetFieldValue<decimal>(4)
            let v = reader.GetFieldValue<int64>(5)
            rows.Add(Duck.toTradeBar symbol per t o h l c v)

        rows.ToArray()
