namespace Lean.Extension

open System
open System.Collections.Generic
open QuantConnect
open QuantConnect.Interfaces
open QuantConnect.Brokerages
open QuantConnect.Orders
open QuantConnect.Securities
open QuantConnect.Packets
open QuantConnect.Data

type UnikoCryptoBrokerage
    (apiKey: string, apiSecret: string, wsUrl: string, restUrl: string, algorithm: IAlgorithm, job: LiveNodePacket) =

    // ---------- 事件后备字段 ----------
    let orderIdChanged =
        Event<EventHandler<BrokerageOrderIdChangedEvent>, BrokerageOrderIdChangedEvent>()

    let ordersStatusChanged = Event<EventHandler<List<OrderEvent>>, List<OrderEvent>>()
    let orderUpdated = Event<EventHandler<OrderUpdateEvent>, OrderUpdateEvent>()
    let optionPositionAssigned = Event<EventHandler<OrderEvent>, OrderEvent>()

    let optionNotification =
        Event<EventHandler<OptionNotificationEventArgs>, OptionNotificationEventArgs>()

    let newOrderNotification =
        Event<EventHandler<NewBrokerageOrderNotificationEventArgs>, NewBrokerageOrderNotificationEventArgs>()

    let delistingNotification =
        Event<EventHandler<DelistingNotificationEventArgs>, DelistingNotificationEventArgs>()

    let accountChanged = Event<EventHandler<AccountEvent>, AccountEvent>()
    let messageEvt = Event<EventHandler<BrokerageMessageEvent>, BrokerageMessageEvent>()

    // ---------- 状态 ----------
    let mutable connected = false
    let mutable baseCurrency = "USD"
    let mutable concurrencyEnabled = true
    let mutable lastCashSyncUtc = DateTime.MinValue

    // （可选）触发器封装，供真实实现里调用
    member private this.RaiseMessage e = messageEvt.Trigger(this, e)
    member private this.RaiseOrdersStatusChanged es = ordersStatusChanged.Trigger(this, es)
    // ... 其他 RaiseXxx 同理

    interface IBrokerage with
        // ---- 事件 add/remove ----
        member _.add_OrderIdChanged(h) = orderIdChanged.Publish.AddHandler h
        member _.remove_OrderIdChanged(h) = orderIdChanged.Publish.RemoveHandler h

        member _.add_OrdersStatusChanged(h) =
            ordersStatusChanged.Publish.AddHandler h

        member _.remove_OrdersStatusChanged(h) =
            ordersStatusChanged.Publish.RemoveHandler h

        member _.add_OrderUpdated(h) = orderUpdated.Publish.AddHandler h
        member _.remove_OrderUpdated(h) = orderUpdated.Publish.RemoveHandler h

        member _.add_OptionPositionAssigned(h) =
            optionPositionAssigned.Publish.AddHandler h

        member _.remove_OptionPositionAssigned(h) =
            optionPositionAssigned.Publish.RemoveHandler h

        member _.add_OptionNotification(h) = optionNotification.Publish.AddHandler h

        member _.remove_OptionNotification(h) =
            optionNotification.Publish.RemoveHandler h

        member _.add_NewBrokerageOrderNotification(h) =
            newOrderNotification.Publish.AddHandler h

        member _.remove_NewBrokerageOrderNotification(h) =
            newOrderNotification.Publish.RemoveHandler h

        member _.add_DelistingNotification(h) =
            delistingNotification.Publish.AddHandler h

        member _.remove_DelistingNotification(h) =
            delistingNotification.Publish.RemoveHandler h

        member _.add_AccountChanged(h) = accountChanged.Publish.AddHandler h
        member _.remove_AccountChanged(h) = accountChanged.Publish.RemoveHandler h
        member _.add_Message(h) = messageEvt.Publish.AddHandler h
        member _.remove_Message(h) = messageEvt.Publish.RemoveHandler h

        // ---- 属性 ----
        member _.Name = "UnikoCrypto"
        member _.IsConnected = connected
        member _.AccountInstantlyUpdated = true
        member _.AccountBaseCurrency = baseCurrency

        // ✅ 新增：并发开关
        member _.ConcurrencyEnabled
            with get () = concurrencyEnabled
            and set v = concurrencyEnabled <- v

        // ✅ 新增：现金同步（IBrokerageCashSynchronizer）
        member _.LastSyncDateTimeUtc = lastCashSyncUtc

        member _.ShouldPerformCashSync(currentTimeUtc: DateTime) =
            // 最小实现：例如 30 分钟同步一次
            (currentTimeUtc - lastCashSyncUtc) >= TimeSpan.FromMinutes(30.0)

        member _.PerformCashSync
            (_algorithm: IAlgorithm, currentTimeUtc: DateTime, _getTimeSinceLastFill: Func<TimeSpan>)
            =
            // 最小实现：标记已同步并返回 true
            lastCashSyncUtc <- currentTimeUtc
            true

        // ---- 连接/断开 ----
        member _.Connect() =
            // TODO: 建连、鉴权、订阅账户/订单等
            connected <- true

        member _.Disconnect() =
            // TODO: 断开、清理
            connected <- false

        // ---- 交易指令 ----
        member _.PlaceOrder(_o: Order) = true
        member _.UpdateOrder(_o: Order) = true
        member _.CancelOrder(_o: Order) = true

        // ---- 查询 ----
        member _.GetOpenOrders() : List<Order> = List<Order>()
        member _.GetAccountHoldings() : List<Holding> = List<Holding>()
        member _.GetCashBalance() : List<CashAmount> = List<CashAmount>() // 你已确认是新版

        // ✅ 新增：历史数据（可返回空；之后接入你的历史/回补逻辑）
        member _.GetHistory(request: HistoryRequest) : IEnumerable<BaseData> = Seq.empty<BaseData>

        // ---- 释放 ----
        member _.Dispose() = ()
