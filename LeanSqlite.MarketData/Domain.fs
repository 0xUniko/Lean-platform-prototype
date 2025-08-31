namespace LeanSqlite.MarketData

open System
open QuantConnect

[<AutoOpen>]
module Domain =
  type DateRange = { FromUtc: DateTime; ToUtc: DateTime }

  let periodOf = function
    | Resolution.Minute -> TimeSpan.FromMinutes 1.0
    | Resolution.Hour   -> TimeSpan.FromHours   1.0
    | Resolution.Daily  -> TimeSpan.FromDays    1.0
    | r -> invalidArg (nameof r) $"Unsupported: %A{r}"

  let resKey = function
    | Resolution.Minute -> "m1"
    | Resolution.Hour   -> "h1"
    | Resolution.Daily  -> "d1"
    | r -> invalidArg (nameof r) $"Unsupported: %A{r}"
