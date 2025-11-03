namespace LeanSqlite.MarketData

open System
open Microsoft.EntityFrameworkCore
open EFCore.BulkExtensions
open System.Linq
open QuantConnect
open QuantConnect.Data.Market
open QuantConnect.Data

// =======================================================================
// SqliteStore.fs
// - EF Core + SQLite 持久层（LINQ 查询）
// - EFCore.BulkExtensions 批量 Upsert（InsertOrUpdate）
// - 复用 Lean 类型：Symbol / Resolution / TradeBar
// - 依赖 Domain.resKey / Domain.periodOf / Domain.DateRange
// =======================================================================

// ---------------------------
// EF 实体与 DbContext
// ---------------------------
type CandleEntity() =
    member val Market = "" with get, set
    member val Security = "" with get, set
    member val Ticker = "" with get, set
    member val Res = "" with get, set
    member val Time = DateTime.UtcNow with get, set // UTC
    member val Open = 0.0m with get, set
    member val High = 0.0m with get, set
    member val Low = 0.0m with get, set
    member val Close = 0.0m with get, set
    member val Volume = 0L with get, set
    member val PeriodS = 60 with get, set

type MarketDataContext(options: DbContextOptions<MarketDataContext>) =
    inherit DbContext(options)

    [<DefaultValue>]
    val mutable candles: DbSet<CandleEntity>

    member x.Candles
        with get () = x.candles
        and set v = x.candles <- v

    override _.OnModelCreating mb =
        let e = mb.Entity<CandleEntity>()
        // 复合主键：用于 Upsert 的匹配列
        e.HasKey [| "Market"; "Security"; "Ticker"; "Res"; "Time" |] |> ignore
        e.HasIndex [| "Market"; "Security"; "Ticker"; "Res"; "Time" |] |> ignore
        ()

[<AutoOpen>]
module private Ef =
    /// 编译期常量：本文件（SqliteStore.fs）所在目录
    [<Literal>]
    let private SourceDir = __SOURCE_DIRECTORY__

    /// 写死为：SqliteStore.fs 同目录下的 marketdata.db
    let private defaultDbPath = IO.Path.Combine(SourceDir, "marketdata.db")
    /// 建议带上更稳的参数（WAL/Shared）
    // let private defaultConn =
    // $"Data Source={defaultDbPath};Cache=Shared;Mode=ReadWriteCreate;Journal Mode=WAL;Synchronous=Normal"

    let private defaultConn = $"Data Source={defaultDbPath}"

    let mkOptions (connStrOpt: string option) =
        let connStr = connStrOpt |> Option.defaultValue defaultConn
        DbContextOptionsBuilder<MarketDataContext>().UseSqlite(connStr).Options


    let toEntity (symbol: Symbol) (res: Resolution) (bar: TradeBar) =
        CandleEntity(
            Market = symbol.ID.Market,
            Security = symbol.ID.SecurityType.ToString().ToLowerInvariant(),
            Ticker = symbol.Value,
            Res = resKey res,
            Time = bar.Time.ToUniversalTime(),
            Open = bar.Open,
            High = bar.High,
            Low = bar.Low,
            Close = bar.Close,
            Volume = int64 bar.Volume,
            PeriodS = int (periodOf res).TotalSeconds
        )

    let toTradeBar (symbol: Symbol) (per: TimeSpan) (r: CandleEntity) =
        TradeBar(r.Time, symbol, r.Open, r.High, r.Low, r.Close, r.Volume, per)

// ---------------------------
// 公共 API：Upsert / Query
// ---------------------------
[<RequireQualifiedAccess>]
module SqliteStore =

    /// 批量 Upsert（同步）：EFCore.BulkExtensions
    /// - 复合键：Market, Security, Ticker, Res, Time
    /// - 大批量时自动分块
    let upsert (connStr: string option) (symbol: Symbol) (resolution: Resolution) (bars: TradeBar array) : unit =
        use ctx = new MarketDataContext(mkOptions connStr)
        ctx.Database.EnsureCreated() |> ignore

        // 映射为实体并一次性物化
        let entities = bars |> Array.Parallel.map (toEntity symbol resolution)

        if entities.Length > 0 then
            // SQLite 单条 SQL 参数上限约 999，BatchSize 设到 ~700 比较稳
            let config =
                BulkConfig(
                    UpdateByProperties =
                        Collections.Generic.List<string> [ "Market"; "Security"; "Ticker"; "Res"; "Time" ],
                    SetOutputIdentity = false,
                    PreserveInsertOrder = true,
                    BatchSize = 700
                )

            ctx.BulkInsertOrUpdate(entities, config)


    /// 列出库中已有 (Market, Security, Ticker, Res) 的去重清单
    let listInstruments (connStr: string option) =
        use ctx = new MarketDataContext(mkOptions connStr)
        ctx.Database.EnsureCreated() |> ignore

        let sw = Diagnostics.Stopwatch.StartNew()

        let q =
            query {
                for c in ctx.Candles do
                    groupBy (c.Market, c.Security, c.Ticker, c.Res) into g
                    select (g.Key, g.Count())
            }
            |> Seq.toArray
            |> Array.Parallel.map (fun ((m, s, t, r), cnt) -> m, s, t, r, cnt)

        sw.Stop()
        printfn "Query executed in: %d ms" sw.ElapsedMilliseconds

        q


    /// LINQ 查询：返回按时间升序的 TradeBar 列表
    let queryBars (connStr: string option) (symbol: Symbol) (resolution: Resolution) (range: Domain.DateRange) =
        use ctx = new MarketDataContext(mkOptions connStr)
        ctx.Database.EnsureCreated() |> ignore

        let per = periodOf resolution
        let m = symbol.ID.Market
        let s = symbol.ID.SecurityType.ToString().ToLowerInvariant()
        let tkr = symbol.Value
        let rk = resKey resolution

        let rows =
            query {
                for c in ctx.Candles do
                    where (
                        c.Market = m
                        && c.Security = s
                        && c.Ticker = tkr
                        && c.Res = rk
                        && c.Time >= range.FromUtc
                        && c.Time < range.ToUtc
                    )

                    sortBy c.Time
                    select c
            }
            |> Seq.toArray

        rows |> Array.Parallel.map (toTradeBar symbol per)

    
