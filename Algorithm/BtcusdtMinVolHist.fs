namespace Algorithm
// ===========================
// btcusdt_min_vol_hist.fsx
// 统计过去一年 BTCUSDT 分钟线“波动（|log return|）”的频数分布
// - 直方：以“绝对对数收益 * 10000”的基点（bp）为单位
// - 导出 CSV：bin_from_bp,bin_to_bp,count,rel_freq,cum_freq
// - 输出摘要：样本数、均值/标准差(bp)、P50/P90/P95/P99(bp)
// ===========================

// 引用命名空间
open System
open System.IO
open System.Globalization
open MarketData
open QuantConnect
open QuantConnect.Data.Market

// ---------------------------
// 参数：数据库、合约、时间窗
// ---------------------------
module BtcusdtMinVolHist =
    [<EntryPoint>]
    let main _ =
        let symbol = Symbol.Create("BTCUSDT", SecurityType.Crypto, Market.Binance)
        let resolution = Resolution.Minute

        let utcNow = DateTime.UtcNow
        let fromUtc = utcNow.AddDays(-365.0)

        // ⚠️ 假设 Domain.DateRange 为记录类型。如与你项目不同，请按你的构造函数修改。
        let range: Domain.DateRange = { FromUtc = fromUtc; ToUtc = utcNow }

        // ---------------------------
        // 取数
        // ---------------------------
        let bars =
            DuckDbStore.queryBars None symbol resolution range
            |> Array.sortBy (fun (tb: TradeBar) -> tb.Time)

        // 足够样本才计算
        if bars.Length < 2 then
            failwithf "样本太少：仅 %d 条。" bars.Length

        // ---------------------------
        // 计算分钟对数收益的绝对值（bp）
        // ---------------------------
        let absLogRetBp =
            bars
            |> Array.pairwise
            |> Array.choose (fun (a, b) ->
                if a.Close > 0m && b.Close > 0m then
                    let r = Math.Log(float b.Close / float a.Close)
                    Some(abs r * 10000.0) // 转为基点
                else
                    None)

        // ---------------------------
        // 直方分箱（bp）
        // 0~50 步长1bp；50~100 步长5bp；100~500 步长25bp；500+ 合并为最后一箱
        // 你可按需调整
        // ---------------------------
        let mkBins () =
            let inline fr (a: float) (b: float) step = [ a..step..b ] |> List.pairwise
            let b1 = fr 0.0 50.0 1.0
            let b2 = fr 50.0 100.0 5.0
            let b3 = fr 100.0 500.0 25.0
            let bins = b1 @ b2 @ b3
            // 最后一箱：500 ~ +∞
            bins @ [ (500.0, Double.PositiveInfinity) ]

        let bins = mkBins ()
        let n = float absLogRetBp.Length

        let hist =
            bins
            |> List.map (fun (lo, hi) ->
                let c =
                    absLogRetBp
                    |> Array.Parallel.filter (fun x -> x >= lo && x < hi)
                    |> Array.length

                let rf = if n > 0.0 then float c / n else 0.0
                lo, hi, c, rf)

        // 累计频率
        let histWithCum =
            let mutable acc = 0.0

            hist
            |> List.map (fun (lo, hi, c, rf) ->
                acc <- acc + rf
                (lo, hi, c, rf, acc))

        // ---------------------------
        // 摘要统计（bp）
        // ---------------------------
        let stats =
            let xs = absLogRetBp |> Array.sort
            let m = xs |> Array.average

            let std =
                let m0 = m
                xs |> Array.averageBy (fun v -> (v - m0) * (v - m0)) |> sqrt

            let inline pct p =
                if xs.Length = 0 then
                    nan
                else
                    let rank = p * float (xs.Length - 1)
                    let i = int (Math.Floor rank)
                    let t = rank - float i

                    if i + 1 < xs.Length then
                        xs[i] * (1.0 - t) + xs[i + 1] * t
                    else
                        xs[i]

            let p50 = pct 0.50
            let p90 = pct 0.90
            let p95 = pct 0.95
            let p99 = pct 0.99
            m, std, p50, p90, p95, p99

        // ---------------------------
        // 输出：控制台 + CSV
        // ---------------------------
        printfn "样本条数: %d" absLogRetBp.Length
        let (meanBp, stdBp, p50, p90, p95, p99) = stats
        printfn "均值(bp)=%.4f, 标准差(bp)=%.4f" meanBp stdBp
        printfn "P50=%.4f, P90=%.4f, P95=%.4f, P99=%.4f (单位: bp)" p50 p90 p95 p99

        printfn "\n分布 (bp)："
        printfn "%8s  %8s  %8s  %10s  %10s" "from" "to" "count" "rel_freq" "cum_freq"

        histWithCum
        |> List.iter (fun (lo, hi, c, rf, cum) ->
            let hiDisp = if Double.IsPositiveInfinity hi then Double.NaN else hi
            printfn "%8.2f  %8.2f  %8d  %10.6f  %10.6f" lo hiDisp c rf cum)

        // 导出 CSV


        let outPath = "btcusdt_minute_abslogret_hist.csv"

        let lines =
            seq {
                yield "bin_from_bp,bin_to_bp,count,rel_freq,cum_freq"

                for (lo, hi, c, rf, cum) in histWithCum do
                    let hiCsv =
                        if Double.IsPositiveInfinity hi then
                            ""
                        else
                            hi.ToString("0.##", CultureInfo.InvariantCulture)

                    yield
                        String.Join(
                            ",",
                            lo.ToString("0.##", CultureInfo.InvariantCulture),
                            hiCsv,
                            string c,
                            rf.ToString("0.######", CultureInfo.InvariantCulture),
                            cum.ToString("0.######", CultureInfo.InvariantCulture)
                        )
            }

        File.WriteAllLines(outPath, lines)
        printfn "已导出: %s" (Path.GetFullPath(outPath))
        0
