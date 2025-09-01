namespace Lean.Extension

open System
open System.Collections.Generic
open QuantConnect
open QuantConnect.Configuration
open QuantConnect.Interfaces
open QuantConnect.Brokerages
open QuantConnect.Packets
open QuantConnect.Util
open QuantConnect.Securities   // IOrderProvider（若你的接口签名需要）

type UnikoCryptoBrokerageFactory () =

    interface IBrokerageFactory with
        member _.BrokerageType = typeof<UnikoCryptoBrokerage>

        member _.BrokerageData
            with get() =
                let d = Dictionary<string,string>()
                let inline addIf (k:string) =
                    let v = Config.Get(k)
                    if not (String.IsNullOrWhiteSpace v) then d.[k] <- v
                addIf "uniko-api-key"
                addIf "uniko-api-secret"
                addIf "uniko-ws-url"
                addIf "uniko-rest-url"
                d

        // 你的 Lean 版本若要求带 IOrderProvider 参数，请保留这个签名
        member _.GetBrokerageModel(_orderProvider: IOrderProvider) : IBrokerageModel =
            upcast DefaultBrokerageModel()

        member _.CreateBrokerage(job: LiveNodePacket, algorithm: IAlgorithm) : IBrokerage =
            let get k =
                if job.BrokerageData.ContainsKey k then job.BrokerageData.[k] else Config.Get(k)

            let apiKey    = get "uniko-api-key"
            let apiSecret = get "uniko-api-secret"
            let wsUrl     = get "uniko-ws-url"
            let restUrl   = get "uniko-rest-url"

            let brokerage = new UnikoCryptoBrokerage(apiKey, apiSecret, wsUrl, restUrl, algorithm, job)

            // 如果 Brokerage 也实现了 IDataQueueHandler，则注册给 Composer
            match box brokerage with
            | :? IDataQueueHandler as q ->
                Composer.Instance.AddPart<IDataQueueHandler>(q) |> ignore
            | _ -> ()

            upcast brokerage

        // ✅ 新增：返回默认消息处理器（可处理信息/警告/错误、重连提示等）
        member _.CreateBrokerageMessageHandler(algorithm: IAlgorithm, job: AlgorithmNodePacket, api: IApi)
            : IBrokerageMessageHandler =
            upcast new DefaultBrokerageMessageHandler(algorithm, job, api)

        // ✅ IBrokerageFactory 也继承 IDisposable（在你的版本中），补一个空实现
        member _.Dispose() = ()
