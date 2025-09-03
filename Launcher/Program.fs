(*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *)
namespace Launcher

open System
open System.IO
open System.Text.Json
open System.Threading
open Argu
open QuantConnect
open QuantConnect.Configuration
open QuantConnect.Lean.Engine
open QuantConnect.Logging
open QuantConnect.Packets
open QuantConnect.Python
open QuantConnect.Util
open Tomlyn
open Tomlyn.Model
open System.Text.RegularExpressions

module Program =
    let collapseMessage =
        "Unhandled exception breaking past controls and causing collapse of algorithm node. This is likely a memory leak of an external dependency or the underlying OS terminating the LEAN engine."

    let mutable leanEngineSystemHandlers: LeanEngineSystemHandlers = null
    let mutable leanEngineAlgorithmHandlers: LeanEngineAlgorithmHandlers = null
    let mutable job: AlgorithmNodePacket = null
    let mutable algorithmManager: AlgorithmManager = null

    do
        AppDomain.CurrentDomain.add_AssemblyLoad (fun _ e ->
            if e.LoadedAssembly.FullName.ToLowerInvariant().Contains("python") then
                Log.Trace(sprintf "Python for .NET Assembly: %O" (e.LoadedAssembly.GetName())))

    let Exit (exitCode: int) =
        if job <> null then
            leanEngineSystemHandlers.JobQueue.AcknowledgeJob(job)
            Log.Trace("Engine.Main(): Packet removed from queue: " + job.AlgorithmId)

        leanEngineSystemHandlers.DisposeSafely() |> ignore
        leanEngineAlgorithmHandlers.DisposeSafely() |> ignore
        Log.LogHandler.DisposeSafely() |> ignore
        OS.Dispose()

        PythonInitializer.Shutdown()

        Log.Trace "Program.Main(): Exiting Lean..."
        Environment.Exit exitCode

    let ExitKeyPress (sender: obj) (args: ConsoleCancelEventArgs) =
        args.Cancel <- true
        algorithmManager.SetStatus AlgorithmStatus.Stopped
        Log.Trace "Program.ExitKeyPress(): Lean instance has been cancelled, shutting down safely now"

    // let launch (args: string array) =
    let launch (cliArguments: Collections.Generic.Dictionary<string, obj> option) =
        if OS.IsWindows then
            Console.OutputEncoding <- Text.Encoding.UTF8

        // if args.Length > 0 then
        //     Config.MergeCommandLineArgumentsWithConfiguration(LeanArgumentParser.ParseArguments args)

        if Option.isSome cliArguments then
            Config.MergeCommandLineArgumentsWithConfiguration(Option.get cliArguments)

        Thread.CurrentThread.Name <- "Algorithm Analysis Thread"

        Initializer.Start()
        leanEngineSystemHandlers <- Initializer.GetSystemHandlers()

        let mutable assemblyPath = ""
        job <- leanEngineSystemHandlers.JobQueue.NextJob(&assemblyPath)

        leanEngineAlgorithmHandlers <- Initializer.GetAlgorithmHandlers()

        if job = null then
            let jobNullMessage =
                "Engine.Main(): Sorry we could not process this algorithm request."

            Log.Error jobNullMessage
            Exit 1

        PythonInitializer.ActivatePythonVirtualEnvironment job.PythonVirtualEnvironment
        |> ignore

        if job.Redelivered then
            Log.Error(
                "Engine.Run(): Job Version: "
                + job.Version
                + "  Deployed Version: "
                + Globals.Version
                + " Redelivered: "
                + job.Redelivered.ToString()
            )

            leanEngineSystemHandlers.Api.SetAlgorithmStatus(
                job.AlgorithmId,
                AlgorithmStatus.RuntimeError,
                collapseMessage
            )

            leanEngineSystemHandlers.Notify.SetAuthentication job
            leanEngineSystemHandlers.Notify.Send(new RuntimeErrorPacket(job.UserId, job.AlgorithmId, collapseMessage))
            leanEngineSystemHandlers.JobQueue.AcknowledgeJob job
            Exit 1

        try
            Console.add_CancelKeyPress ExitKeyPress

            algorithmManager <- new AlgorithmManager(Globals.LiveMode, job)

            leanEngineSystemHandlers.LeanManager.Initialize(
                leanEngineSystemHandlers,
                leanEngineAlgorithmHandlers,
                job,
                algorithmManager
            )

            OS.Initialize()

            let engine =
                new Engine(leanEngineSystemHandlers, leanEngineAlgorithmHandlers, Globals.LiveMode)

            engine.Run(job, algorithmManager, assemblyPath, WorkerThread.Instance)
        finally
            let algorithmStatus =
                if algorithmManager <> null then
                    algorithmManager.State
                else
                    AlgorithmStatus.DeployError

            Exit(
                if algorithmStatus <> AlgorithmStatus.Completed then
                    1
                else
                    0
            )

        0 // This line is unreachable due to Environment.Exit, but satisfies the EntryPoint return type

    // ---- Helpers: TOML -> JSON file for --config ----
    module private TomlToJson =
        let private expandEnv (text: string) =
            Regex.Replace(
                text,
                @"\$\{([A-Z0-9_]+)\}",
                fun (m: Match) ->
                    let key = m.Groups.[1].Value

                    match Environment.GetEnvironmentVariable(key) with
                    | null -> m.Value
                    | v -> v
            )

        let private toJsonElement (value: obj) : JsonElement =
            let rec write (w: Utf8JsonWriter) (v: obj) =
                match v with
                | null -> w.WriteNullValue()
                | :? string as s -> w.WriteStringValue(s)
                | :? bool as b -> w.WriteBooleanValue(b)
                | :? int8 as i -> w.WriteNumberValue(int i)
                | :? int16 as i -> w.WriteNumberValue(int i)
                | :? int as i -> w.WriteNumberValue(i)
                | :? int64 as i -> w.WriteNumberValue(i)
                | :? uint8 as i -> w.WriteNumberValue(int i)
                | :? uint16 as i -> w.WriteNumberValue(int i)
                | :? uint32 as i -> w.WriteNumberValue(uint32 i)
                | :? uint64 as i -> w.WriteNumberValue(uint64 i)
                | :? double as d -> w.WriteNumberValue(d)
                | :? decimal as d -> w.WriteNumberValue(d)
                | :? System.Collections.IDictionary as dict ->
                    w.WriteStartObject()

                    for key in dict.Keys do
                        let ks = key :?> string
                        w.WritePropertyName(ks)
                        write w (dict.[key])

                    w.WriteEndObject()
                | :? System.Collections.IEnumerable as seq when not (v :? string) ->
                    w.WriteStartArray()

                    for item in seq do
                        write w item

                    w.WriteEndArray()
                | :? DateTimeOffset as dto -> w.WriteStringValue(dto.UtcDateTime.ToString("O"))
                | :? DateTime as dt -> w.WriteStringValue(dt.ToUniversalTime().ToString("O"))
                | _ -> w.WriteStringValue(v.ToString())

            use ms = new MemoryStream()
            use w = new Utf8JsonWriter(ms, JsonWriterOptions(Indented = false))
            write w value
            w.Flush()
            JsonDocument.Parse(ms.ToArray()).RootElement.Clone()

        let fromTomlText (text: string) : JsonElement =
            let text = expandEnv text
            let model = Toml.ToModel(text)

            let rec toObj (t: obj) : obj =
                match t with
                | :? TomlTable as tbl ->
                    let d = System.Collections.Generic.Dictionary<string, obj>()

                    for KeyValue(k, v) in tbl do
                        d.[k] <- toObj v

                    d :> obj
                | :? TomlArray as arr ->
                    let l = System.Collections.Generic.List<obj>()

                    for v in arr do
                        l.Add(toObj v)

                    l :> obj
                | :? string
                | :? bool
                | :? int64
                | :? double
                | :? decimal
                | :? DateTime
                | :? DateTimeOffset -> t
                | null -> null
                | _ -> t

            toJsonElement (toObj model)

        let fromTomlFile (path: string) : JsonElement = File.ReadAllText path |> fromTomlText

        let assertHasAlgorithmLocation (src: JsonElement) : unit =
            if src.ValueKind <> JsonValueKind.Object then
                failwith "配置根节点不是对象（应为 TOML 的顶层表）。"

            let mutable prop = Unchecked.defaultof<JsonElement>
            let ok = src.TryGetProperty("algorithm-location", &prop)

            if not ok then
                failwith "配置缺少必需键：algorithm-location（请在 TOML 根级指定算法 DLL/脚本路径）。"

            if prop.ValueKind <> JsonValueKind.String then
                failwith "algorithm-location 必须是字符串。"

            let v = prop.GetString()

            if String.IsNullOrWhiteSpace v then
                failwith "algorithm-location 不能为空字符串。"

        let writeJson (path: string) (je: JsonElement) =
            use fs = File.Create path
            use w = new Utf8JsonWriter(fs, JsonWriterOptions(Indented = true))
            je.WriteTo(w)
            w.Flush()

        let resolveTomlPath (input: string) =
            let hasSep =
                input.IndexOfAny([| Path.DirectorySeparatorChar; Path.AltDirectorySeparatorChar |])
                >= 0

            let isToml = input.EndsWith(".toml", StringComparison.OrdinalIgnoreCase)

            if hasSep || isToml then
                let p = if isToml then input else input + ".toml"

                if File.Exists p then
                    Path.GetFullPath p
                else
                    failwithf "未找到 TOML 配置文件：%s" p
            else
                let p = Path.Combine(Directory.GetCurrentDirectory(), input + ".toml")

                if File.Exists p then
                    Path.GetFullPath p
                else
                    failwithf "未在当前工作目录找到 TOML 配置文件：%s" p

        // 将 JsonElement 转为 Dictionary<string, obj> 以供 Config.MergeCommandLineArgumentsWithConfiguration 使用
        let rec private jsonToObj (je: JsonElement) : obj =
            match je.ValueKind with
            | JsonValueKind.Null
            | JsonValueKind.Undefined -> null
            | JsonValueKind.String -> box (je.GetString())
            | JsonValueKind.Number ->
                // 尝试保留整数，否则为 double
                match je.TryGetInt64() with
                | true, v -> box v
                | _ -> box (je.GetDouble())
            | JsonValueKind.True -> box true
            | JsonValueKind.False -> box false
            | JsonValueKind.Array ->
                let l = System.Collections.Generic.List<obj>()

                for item in je.EnumerateArray() do
                    l.Add(jsonToObj item)

                box l
            | JsonValueKind.Object ->
                let d = System.Collections.Generic.Dictionary<string, obj>()

                for p in je.EnumerateObject() do
                    d.[p.Name] <- jsonToObj p.Value

                box d
            | _ -> box (je.ToString())

        let toDictionary (je: JsonElement) : Collections.Generic.Dictionary<string, obj> =
            if je.ValueKind <> JsonValueKind.Object then
                failwith "TOML 顶层必须是对象/表。"

            jsonToObj je :?> Collections.Generic.Dictionary<string, obj>

    type CLIArgs =
        | [<AltCommandLine("-c")>] Config of string
        | Hello

        interface IArgParserTemplate with
            member x.Usage =
                match x with
                | Config _ -> "TOML 配置文件路径，或只写文件名（不含路径），将从当前工作目录查找同名 .toml。（注意：TOML 必须包含根级键 algorithm-location）"
                | Hello -> "打印探测信息。"

    [<EntryPoint>]
    let main (argv: string array) =
        let parser = ArgumentParser.Create<CLIArgs>(programName = "launcher")
        let results = parser.Parse argv

        let cliArguments =
            match results.TryGetResult Config with
            | Some tomlArg ->
                try
                    let tomlPath = TomlToJson.resolveTomlPath tomlArg
                    let cfg = TomlToJson.fromTomlFile tomlPath
                    TomlToJson.assertHasAlgorithmLocation cfg

                    // in-memory 合并（不落地 JSON 文件）
                    // let argsDict = TomlToJson.toDictionary cfg
                    TomlToJson.toDictionary cfg |> Some
                // launch (Some argsDict)
                with ex ->
                    // Console.Error.WriteLine($"TOML 解析失败：{ex.Message}")
                    failwith ($"TOML 解析失败：{ex.Message}")
            | None ->
                match results.TryGetResult Hello with
                | Some _ ->
                    Console.WriteLine("用法：launcher -c <配置.toml>")
                    Console.WriteLine("示例：dotnet run --project Launcher -- -c Launcher/backtesting.toml")
                    None
                | None ->
                    Console.WriteLine("请使用 -c <配置.toml> 指定配置文件；或用 --help 查看帮助。")
                    None

        launch cliArguments
