namespace LeanSqlite.HistoryProvider

open System
open System.Collections.Generic
open NodaTime
open QuantConnect
open QuantConnect.Data
open QuantConnect.Data.Market
open QuantConnect.Interfaces
open QuantConnect.Packets
open QuantConnect.Securities
open LeanSqlite.MarketData

type SqliteHistoryProvider(connStr: string) =
    let mutable count = 0

    let toRange (r: HistoryRequest) =
        { Domain.DateRange.FromUtc = r.StartTimeUtc
          ToUtc = r.EndTimeUtc }

// let toSlices (bars: TradeBar list) : IEnumerable<Slice> =
//     seq {
//         for b in bars do
//             let ts =
//                 TimeSlice.Create(
//                     b.EndTime,
//                     DateTimeZone.Utc,
//                     [| System.Collections.Generic.KeyValuePair(b.Symbol, (b :> IBaseData)) |],
//                     [||],
//                     [||],
//                     [||],
//                     [||],
//                     0
//                 )

//             yield ts.Slice
//     }
//     :> _

// interface IHistoryProvider with
//     member _.DataPointCount = count

//     member _.Initialize(_parameters: HistoryProviderInitializeParameters) = ()

//     member _.GetHistory(req: IEnumerable<Data.HistoryRequest>, tz: DateTimeZone) : IEnumerable<Slice> =
//         if req.Symbol.ID.SecurityType <> SecurityType.Crypto then
//             Seq.empty :> _
//         else
//             let bars = SqliteStore.query connStr req.Symbol req.Resolution (toRange req)
//             count <- count + bars.Length
//             toSlices bars

//     member this.GetHistory(reqs: IEnumerable<HistoryRequest>, tz: DateTimeZone) : IEnumerable<Slice> =
//         reqs |> Seq.collect (fun r -> (this :> IHistoryProvider).GetHistory(r, tz)) :> _

//     member _.Dispose() = ()
