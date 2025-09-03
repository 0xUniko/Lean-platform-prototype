namespace Lean.Extension

open System
open System.IO
open QuantConnect
open QuantConnect.Interfaces
open QuantConnect.Logging

/// F# implementation of a local disk data provider.
/// Mirrors the behavior of the C# DefaultDataProvider: reads files from disk,
/// logs a one-time warning for missing directories, and raises a completion event.
type FsDefaultDataProvider() as this =
    // One-time warning flag for missing data directory
    let mutable oneTimeWarningLog = false

    // .NET event backing field
    let newDataRequest = new Event<EventHandler<DataProviderNewDataRequestEventArgs>, DataProviderNewDataRequestEventArgs>()

    interface IDataProvider with
        /// Event raised each time data fetch is finished (successfully or not)
        [<CLIEvent>]
        member _.NewDataRequest = newDataRequest.Publish

        /// Retrieves data from disk to be used in an algorithm
        member _.Fetch(key: string) : Stream =
            let mutable success = true
            let mutable errorMessage = String.Empty
            let result =
                try
                    // Inner try/with handles success/error assignment and rethrows unexpected errors
                    try
                        new FileStream(FileExtension.ToNormalizedPath(key), FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
                    with
                    | :? DirectoryNotFoundException as ex ->
                        success <- false
                        errorMessage <- ex.Message
                        if not oneTimeWarningLog then
                            oneTimeWarningLog <- true
                            Log.Debug(sprintf "FsDefaultDataProvider.Fetch(): DirectoryNotFoundException: please review data paths, current 'Globals.DataFolder': %s" Globals.DataFolder)
                        null :> Stream
                    | :? FileNotFoundException as ex ->
                        success <- false
                        errorMessage <- ex.Message
                        null :> Stream
                    | ex ->
                        // Pass other exceptions up the stack after recording state for the event
                        success <- false
                        errorMessage <- ex.Message
                        reraise()
                finally
                    newDataRequest.Trigger(this, new DataProviderNewDataRequestEventArgs(key, success, errorMessage))
            result

    interface IDisposable with
        /// The stream is managed by the consumer; no resources to dispose here.
        member _.Dispose() = ()
