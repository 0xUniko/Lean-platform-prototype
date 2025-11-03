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
    let launch (cliArguments: Collections.Generic.Dictionary<string, obj>) =
        if OS.IsWindows then
            Console.OutputEncoding <- Text.Encoding.UTF8

        // if args.Length > 0 then
        //     Config.MergeCommandLineArgumentsWithConfiguration(LeanArgumentParser.ParseArguments args)

        // if Option.isSome cliArguments then
        Config.MergeCommandLineArgumentsWithConfiguration cliArguments

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
                failwith "传入的根节点不是对象，应为 TOML 的表类型"

            let mutable prop = Unchecked.defaultof<JsonElement>
            let ok = src.TryGetProperty("algorithm-location", &prop)

            if not ok then
                failwith "配置缺少必填项 algorithm-location，请在 TOML 中指定算法 DLL 或脚本路径"

            if prop.ValueKind <> JsonValueKind.String then
                failwith "algorithm-location 必须是字符串"

            let v = prop.GetString()

            if String.IsNullOrWhiteSpace v then
                failwith "algorithm-location 不能为空字符串"

        // let writeJson (path: string) (je: JsonElement) =
        //     use fs = File.Create path
        //     use w = new Utf8JsonWriter(fs, JsonWriterOptions(Indented = true))
        //     je.WriteTo(w)
        //     w.Flush()

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
                    failwithf "在当前工作目录未找到 TOML 配置文件：%s" p

        // 将 JsonElement 转为 Dictionary<string, obj> 供 Config.MergeCommandLineArgumentsWithConfiguration 使用
        let rec private jsonToObj (je: JsonElement) : obj =
            match je.ValueKind with
            | JsonValueKind.Null
            | JsonValueKind.Undefined -> null
            | JsonValueKind.String -> box (je.GetString())
            | JsonValueKind.Number ->
                // TOML 数值默认解析为 double
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
                failwith "TOML 根节点必须是对象/表"

            jsonToObj je :?> Collections.Generic.Dictionary<string, obj>

    type CLIArgs =
        | [<AltCommandLine("-c")>] Config of string
        | [<AltCommandLine("--no-build")>] NoBuild

        interface IArgParserTemplate with
            member x.Usage =
                match x with
                | Config _ -> "TOML 配置文件路径；可写文件名或完整路径，若仅提供文件名会在当前工作目录查找同名 .toml。注意：TOML 配置必须包含 algorithm-location。"
                | NoBuild -> "跳过预构建当前 Algorithm 项目"

    [<EntryPoint>]
    let main (argv: string array) =
        let parser = ArgumentParser.Create<CLIArgs>(programName = "launcher")

        let isHelpArg =
            function
            | null -> false
            | s when String.Equals(s, "--help", StringComparison.OrdinalIgnoreCase) -> true
            | s when String.Equals(s, "-h", StringComparison.OrdinalIgnoreCase) -> true
            | s when String.Equals(s, "help", StringComparison.OrdinalIgnoreCase) -> true
            | _ -> false

        let printUsage () = parser.PrintUsage() |> printfn "%s"

        if Array.exists isHelpArg argv then
            printUsage ()
            0
        else
            let results = parser.Parse argv

            match results.TryGetResult Config with
            | Some tomlArg ->
                try
                    let tomlPath = TomlToJson.resolveTomlPath tomlArg
                    let cfg = TomlToJson.fromTomlFile tomlPath
                    TomlToJson.assertHasAlgorithmLocation cfg
    
                    // run a quick build of the Algorithm project unless --no-build is specified
                    let skipBuild =
                        match results.TryGetResult NoBuild with
                        | Some _ -> true
                        | None -> false
    
                    // pre-build Algorithm project in Debug configuration
                    let buildExitCode =
                        if skipBuild then
                            0
                        else
                            try
                                let projPath = Path.Combine(Directory.GetCurrentDirectory(), "Algorithm", "Algorithm.fsproj")
                                if not (File.Exists projPath) then
                                    Console.Error.WriteLine($"未找到算法项目文件：{projPath}")
                                    3
                                else
                                    let startInfo = new System.Diagnostics.ProcessStartInfo()
                                    startInfo.FileName <- "dotnet"
                                    startInfo.Arguments <- $"build \"{projPath}\" -c Debug"
                                    startInfo.WorkingDirectory <- Path.GetDirectoryName projPath
                                    startInfo.RedirectStandardOutput <- true
                                    startInfo.RedirectStandardError <- true
                                    startInfo.UseShellExecute <- false
                                    use p = System.Diagnostics.Process.Start(startInfo)
                                    p.OutputDataReceived.Add(fun e -> if not (isNull e.Data) then Console.WriteLine e.Data)
                                    p.ErrorDataReceived.Add(fun e -> if not (isNull e.Data) then Console.Error.WriteLine e.Data)
                                    p.BeginOutputReadLine()
                                    p.BeginErrorReadLine()
                                    p.WaitForExit()
                                    if p.ExitCode <> 0 then
                                        Console.Error.WriteLine($"Algorithm 项目构建失败，退出码 {p.ExitCode}")
                                    else
                                        Console.WriteLine("Algorithm 预构建成功（Debug 配置）")
                                    p.ExitCode
                            with ex ->
                                Console.Error.WriteLine($"构建 Algorithm 项目时发生错误：{ex.Message}")
                                3

                    if buildExitCode <> 0 then
                        buildExitCode
                    else
                        // 在内存中合并配置并转换为 JSON 供引擎使用
                        TomlToJson.toDictionary cfg |> launch |> ignore
                        0

                    // 如需调试可将配置写入本地 config.json 供引擎使用
                    // let debugConfigPath = Path.Combine(Directory.GetCurrentDirectory(), "config.json")
                    // TomlToJson.writeJson debugConfigPath cfg
    
                // launch (Some argsDict)
                with ex ->
                    Console.Error.WriteLine($"TOML 解析失败：{ex.Message}")
                    2
            | None ->
                    Console.WriteLine("请使用 -c <配置.toml> 指定配置文件，或 --help 查看详细帮助")
                    1
