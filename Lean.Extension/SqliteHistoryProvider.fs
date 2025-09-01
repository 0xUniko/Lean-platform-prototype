namespace Lean.Extension

open NodaTime
open QuantConnect.Data
open QuantConnect.Configuration

type SqliteHistoryProvider() =
    inherit HistoryProviderBase()

    let mutable count = 0

    override _.DataPointCount = count

    override this.Initialize(parameters: HistoryProviderInitializeParameters) =
        // 拿到 Lean 注入的各种 Provider
        let cache = parameters.DataCacheProvider
        let maps = parameters.MapFileProvider
        let factors = parameters.FactorFileProvider
        // 这里可以读 config.json 里的连接串
        let conn = Config.Get("sqlite-connection", "Data Source=marketdata.db")
        ()

    override this.GetHistory(requests, sliceTimeZone: DateTimeZone) =
        seq {
            for req in requests do
                // 这里从 SQLite 查询数据
                // yield return 一个个 Slice
                ()
        }
