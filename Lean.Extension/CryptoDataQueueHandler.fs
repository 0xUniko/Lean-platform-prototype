namespace Lean.Extension

open System
open QuantConnect
open QuantConnect.Interfaces
open QuantConnect.Data
open QuantConnect.Data.Market

type UnikoCryptoDataQueueHandler() =
    let mutable handler: EventHandler = null

    interface IDataQueueHandler with
        member _.IsConnected = true
        member _.SetJob(job) = ()

        member _.Subscribe(cfg: SubscriptionDataConfig, newDataHandler: EventHandler) =
            handler <- newDataHandler
            Seq.empty<BaseData>.GetEnumerator()

        member _.Unsubscribe(cfg: SubscriptionDataConfig) = ()
        member _.Dispose() = ()
