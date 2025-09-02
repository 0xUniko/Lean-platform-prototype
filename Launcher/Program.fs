namespace Launcher

open System
open System.Collections
open System.IO
open System.Diagnostics
open System.Text.Json
open System.Text.Json.Serialization
open Argu
open Serilog

// ======================================================
// CLI 定义
// ======================================================
type BacktestArgs =
    | [<Mandatory; AltCommandLine("-t")>] AlgoTypeName of string
    | [<AltCommandLine("-d")>] AlgoDll of path: string
    | [<AltCommandLine("-L")>] AlgoLanguage of string
    | [<AltCommandLine("-s")>] Start of string
    | [<AltCommandLine("-e")>] End of string
    | [<AltCommandLine("-T")>] ConfigTemplate of path: string
    | [<AltCommandLine("-O")>] Overrides of path: string
    | [<AltCommandLine("-H")>] HistoryProvider of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | AlgoTypeName _ -> "算法类型名（命名空间+类名，如 Algorithm.MyAlgo）。"
            | AlgoDll _ -> "算法 DLL 路径（若不指定，会自动在 Algorithm/bin/**/ 下查找 Algorithm.dll）。"
            | AlgoLanguage _ -> "算法语言（默认 FSharp，可填 CSharp/Python/FSharp）。"
            | Start _ -> "回测起始日期（如 2020-01-01）。"
            | End _ -> "回测结束日期（如 2020-12-31）。"
            | ConfigTemplate _ -> "配置模板路径（支持 .toml/.json；默认尝试 <lean>/Launcher/config.json）。"
            | Overrides _ -> "覆盖配置的文件（支持 .toml/.json；合并时后者覆盖前者）。"
            | HistoryProvider _ -> "自定义历史数据提供者类型全名（默认 Lean.Extension.SqliteHistoryProvider）。"

type LiveArgs =
    | [<Mandatory; AltCommandLine("-t")>] AlgoTypeName of string
    | [<AltCommandLine("-d")>] AlgoDll of path: string
    | [<AltCommandLine("-L")>] AlgoLanguage of string
    | [<AltCommandLine("-T")>] ConfigTemplate of path: string
    | [<AltCommandLine("-O")>] Overrides of path: string
    | [<AltCommandLine("-b")>] Brokerage of string
    | [<AltCommandLine("-q")>] DataQueueHandler of string
    // 常见凭证直传（也可以只放在 overrides 里）
    | [<AltCommandLine("--api-key")>] ApiKey of string
    | [<AltCommandLine("--api-secret")>] ApiSecret of string
    | [<AltCommandLine("--ws-url")>] WsUrl of string
    | [<AltCommandLine("--rest-url")>] RestUrl of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | AlgoTypeName _ -> "算法类型名（命名空间+类名，如 Algorithm.MyLiveAlgo）。"
            | AlgoDll _ -> "算法 DLL 路径（若不指定，会自动在 Algorithm/bin/**/ 下查找 Algorithm.dll）。"
            | AlgoLanguage _ -> "算法语言（默认 FSharp）。"
            | ConfigTemplate _ -> "配置模板路径（支持 .toml/.json）。"
            | Overrides _ -> "覆盖配置的文件（支持 .toml/.json）。"
            | Brokerage _ -> "Brokerage 名称（默认 UnikoCrypto）。"
            | DataQueueHandler _ -> "实时数据队列处理器类型全名（默认 YourCompany.Lean.UnikoCryptoDataQueueHandler）。"
            | ApiKey _ -> "API Key（也可放在 overrides）。"
            | ApiSecret _ -> "API Secret（也可放在 overrides）。"
            | WsUrl _ -> "WebSocket 端点（也可放在 overrides）。"
            | RestUrl _ -> "REST 端点（也可放在 overrides）。"

type CLIArgs =
    // 通用
    | [<AltCommandLine("-r")>] RepoRoot of path: string
    | [<AltCommandLine("-l")>] LeanDir of path: string
    | [<AltCommandLine("-A")>] AlgorithmDir of path: string
    | [<AltCommandLine("-o")>] OutputDir of path: string
    | [<AltCommandLine("--no-build")>] NoBuild
    | [<AltCommandLine("--plugin-proj")>] PluginProject of path: string
    | [<AltCommandLine("--plugin-bin")>] PluginBin of path: string
    | [<AltCommandLine("--plugins-out")>] PluginsOut of path: string
    | [<AltCommandLine("--lean-launcher-dll")>] LeanLauncherDll of path: string

    // 子命令
    | Backtest of ParseResults<BacktestArgs>
    | Live of ParseResults<LiveArgs>
    | Build
    | Hello

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | RepoRoot _ -> "仓库根目录（默认：当前目录）。"
            | LeanDir _ -> "Lean 子模块目录（默认：<repo>/lean）。"
            | AlgorithmDir _ -> "Algorithm 项目目录（默认：<repo>/Algorithm）。"
            | OutputDir _ -> "输出目录（默认：<repo>/runs）。"
            | NoBuild -> "跳过 dotnet build。"
            | PluginProject _ -> "需要构建并拷贝到 ./plugins 的插件项目（可多次传入）。"
            | PluginBin _ -> "已构建好的插件 bin 目录（可多次传入，如 src/Plugin/bin/Release）。"
            | PluginsOut _ -> "运行目录内插件输出相对路径（默认 plugins）。"
            | LeanLauncherDll _ -> "显式指定 QuantConnect.Lean.Launcher.dll 路径。"
            | Backtest _ -> "启动回测。"
            | Live _ -> "启动实盘。"
            | Build -> "仅构建（Algorithm、Lean.Launcher、插件）。"
            | Hello -> "打印探测信息。"

// ======================================================
// JSON 工具
// ======================================================
module Json =
    let options =
        let o = JsonSerializerOptions(WriteIndented = true)
        o.DefaultIgnoreCondition <- JsonIgnoreCondition.WhenWritingNull
        o

    let parseFile (path: string) =
        if File.Exists path then
            use s = File.OpenRead path
            JsonDocument.Parse(s).RootElement.Clone()
        else
            JsonDocument.Parse("{}").RootElement.Clone()

    let parseString (s: string) =
        JsonDocument.Parse(s).RootElement.Clone()

    let private elementFromDict (dict: System.Collections.Generic.IDictionary<string, JsonElement>) =
        use ms = new MemoryStream()
        use writer = new Utf8JsonWriter(ms)
        writer.WriteStartObject()

        for KeyValue(k, v) in dict do
            writer.WritePropertyName(k)
            v.WriteTo(writer)

        writer.WriteEndObject()
        writer.Flush()
        JsonDocument.Parse(ms.ToArray()).RootElement.Clone()

    let rec merge (a: JsonElement) (b: JsonElement) =
        if a.ValueKind <> JsonValueKind.Object then
            b
        elif b.ValueKind <> JsonValueKind.Object then
            b
        else
            let dict = Generic.Dictionary<string, JsonElement>()

            for p in a.EnumerateObject() do
                dict[p.Name] <- p.Value

            for p in b.EnumerateObject() do
                match dict.TryGetValue p.Name with
                | true, oldv when
                    oldv.ValueKind = JsonValueKind.Object
                    && p.Value.ValueKind = JsonValueKind.Object
                    ->
                    dict[p.Name] <- merge oldv p.Value
                | _ -> dict[p.Name] <- p.Value

            elementFromDict dict

    let addPairs (pairs: (string * string) list) =
        let dict = Generic.Dictionary<string, JsonElement>()

        for (k, v) in pairs do
            dict[k] <- JsonDocument.Parse($"\"{v}\"").RootElement.Clone()

        elementFromDict dict

    let write (path: string) (je: JsonElement) =
        use fs = File.Create path
        use w = new Utf8JsonWriter(fs, JsonWriterOptions(Indented = true))
        je.WriteTo(w)
        w.Flush()

// ======================================================
// TOML 解析（TOML -> JsonElement），含 ${ENV_VAR} 展开
// ======================================================
module ConfigParse =
    open System.Text.RegularExpressions
    open Tomlyn
    open Tomlyn.Model

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
            | :? int8
            | :? int16
            | :? int
            | :? int64
            | :? uint8
            | :? uint16
            | :? uint32
            | :? uint64
            | :? float
            | :? double
            | :? decimal ->
                match v with
                | :? decimal as d -> w.WriteNumberValue(d)
                | :? double as d -> w.WriteNumberValue(d)
                | _ -> w.WriteNumberValue(Convert.ToDouble v)
            | :? IDictionary as dict ->
                w.WriteStartObject()

                for key in dict.Keys do
                    let ks = key :?> string
                    w.WritePropertyName(ks)
                    write w (dict.[key])

                w.WriteEndObject()
            | :? IEnumerable as seq when not (v :? string) ->
                w.WriteStartArray()

                for item in seq do
                    write w item

                w.WriteEndArray()
            | :? DateTime as dt -> w.WriteStringValue(dt.ToUniversalTime().ToString("O"))
            | _ -> w.WriteStringValue(v.ToString())

        use ms = new MemoryStream()
        use w = new Utf8JsonWriter(ms, JsonWriterOptions(Indented = false))
        write w value
        w.Flush()
        JsonDocument.Parse(ms.ToArray()).RootElement.Clone()

    let fromTomlFile (path: string) : JsonElement =
        let text = File.ReadAllText path |> expandEnv
        let model = Toml.ToModel text

        let rec tomlToObj (t: obj) : obj =
            match t with
            | :? TomlTable as tbl ->
                let d = Generic.Dictionary<string, obj>()

                for KeyValue(k, v) in tbl do
                    d.[k] <- tomlToObj v

                d :> obj
            | :? TomlArray as arr ->
                let l = Generic.List<obj>()

                for v in arr do
                    l.Add(tomlToObj v)

                l :> obj
            // 叶子类型，直接返回 .NET 值
            | :? string as s -> s :> obj
            | :? bool as b -> b :> obj
            | :? int64 as i -> i :> obj
            | :? double as d -> d :> obj
            | :? DateTimeOffset as dt -> dt :> obj
            | null -> null
            | _ -> t // 兜底：原样返回

        toJsonElement (tomlToObj model)

    /// 统一入口：支持 .toml / .json
    let fromAnyFile (path: string) : JsonElement =
        match Path.GetExtension(path).ToLowerInvariant() with
        | ".toml" -> fromTomlFile path
        | ".json" -> Json.parseFile path
        | ext -> failwithf "不支持的配置文件类型：%s（仅支持 .toml / .json）" ext

// ======================================================
// 进程与路径工具
// ======================================================
module Proc =
    let run (workingDir: string) (exe: string) (args: string) =
        let psi = ProcessStartInfo()
        psi.FileName <- exe
        psi.Arguments <- args
        psi.WorkingDirectory <- workingDir
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.UseShellExecute <- false
        psi.CreateNoWindow <- true
        let p = new Process()
        p.StartInfo <- psi

        p.OutputDataReceived.Add(fun d ->
            if not (isNull d.Data) then
                Log.Information("{Line}", d.Data))

        p.ErrorDataReceived.Add(fun d ->
            if not (isNull d.Data) then
                Log.Error("{Line}", d.Data))

        if p.Start() then
            p.BeginOutputReadLine()
            p.BeginErrorReadLine()
            p.WaitForExit()
            p.ExitCode
        else
            -1

module Paths =
    let norm (p: string) = Path.GetFullPath p

    let ensureDir (p: string) =
        Directory.CreateDirectory(p) |> ignore
        p

    let guessRepoRoot () = Directory.GetCurrentDirectory() |> norm

    let findFirst (root: string) (pattern: string) =
        if Directory.Exists root then
            Directory.GetFiles(root, pattern, SearchOption.AllDirectories) |> Array.tryHead
        else
            None

    let copyFileTo (src: string) (dstDir: string) =
        let file = Path.GetFileName src
        File.Copy(src, Path.Combine(dstDir, file), true)

// ======================================================
// 构建、拷贝插件与 Lean/Algorithm 探测
// ======================================================
module Build =
    open Proc

    let buildProject (repo: string) (projPath: string) =
        Log.Information("构建项目：{Proj}", projPath)
        let ec = run repo "dotnet" $"build \"{projPath}\" -c Release"

        if ec <> 0 then
            failwithf "构建失败（%s），退出码 %d" projPath ec

    let buildAll (repo: string) (algoDir: string) (leanDir: string) (pluginProjects: string list) =
        // Algorithm
        let algoProj =
            let fsproj = Path.Combine(algoDir, "Algorithm.fsproj")
            let csproj = Path.Combine(algoDir, "Algorithm.csproj")

            if File.Exists fsproj then fsproj
            elif File.Exists csproj then csproj
            else algoDir

        buildProject repo algoProj

        // Lean.Launcher
        let leanLauncherProj = Path.Combine(leanDir, "Launcher")
        buildProject repo leanLauncherProj

        // Plugins
        for p in pluginProjects do
            buildProject repo p

    let findAlgoDll (algorithmDir: string) (cliDll: string option) =
        match cliDll with
        | Some p when File.Exists p -> p
        | _ ->
            let bin = Path.Combine(algorithmDir, "bin")

            match Paths.findFirst bin "Algorithm.dll" with
            | Some p -> p
            | None -> failwith "未找到 Algorithm.dll，请用 -d 指定或先构建 Algorithm。"

    let findLeanLauncherDll (leanDir: string) (cliDll: string option) =
        match cliDll with
        | Some p when File.Exists p -> p
        | _ ->
            let root = Path.Combine(leanDir, "Launcher", "bin")

            match Paths.findFirst root "QuantConnect.Lean.Launcher.dll" with
            | Some p -> p
            | None -> failwith "未找到 QuantConnect.Lean.Launcher.dll，请先构建 Lean 子模块的 Launcher。"

    let rec copyAll (srcRoot: string) (dstDir: string) (pred: FileInfo -> bool) =
        if Directory.Exists srcRoot then
            for f in Directory.EnumerateFiles(srcRoot, "*", SearchOption.AllDirectories) do
                let fi = FileInfo f

                if pred fi then
                    let name = fi.Name
                    let dst = Path.Combine(dstDir, name)
                    File.Copy(fi.FullName, dst, true)

    let collectPlugins (runPluginsDir: string) (pluginBins: string list) (pluginProjects: string list) =
        Directory.CreateDirectory(runPluginsDir) |> ignore

        // 从 bin 目录拷贝（直接指定的）
        for bin in pluginBins do
            if Directory.Exists bin then
                Log.Information("复制插件产物（bin）：{Bin}", bin)

                copyAll bin runPluginsDir (fun fi ->
                    fi.Extension.Equals(".dll", StringComparison.OrdinalIgnoreCase)
                    || fi.Name.EndsWith(".deps.json", StringComparison.OrdinalIgnoreCase)
                    || fi.Name.EndsWith(".runtimeconfig.json", StringComparison.OrdinalIgnoreCase))

        // 从项目默认输出目录拷贝（Release/**/）
        for proj in pluginProjects do
            let out = Path.Combine(Path.GetDirectoryName proj, "bin", "Release")

            if Directory.Exists out then
                Log.Information("复制插件产物（proj）：{Out}", out)

                copyAll out runPluginsDir (fun fi ->
                    fi.Extension.Equals(".dll", StringComparison.OrdinalIgnoreCase)
                    || fi.Name.EndsWith(".deps.json", StringComparison.OrdinalIgnoreCase)
                    || fi.Name.EndsWith(".runtimeconfig.json", StringComparison.OrdinalIgnoreCase))

// ======================================================
// 配置生成（模板+补丁+overrides）
// ======================================================
module ConfigGen =
    open Json
    open ConfigParse

    let loadTemplate (leanDir: string) (tpl: string option) =
        match tpl with
        | Some p when File.Exists p -> fromAnyFile p
        | _ ->
            let candidate = Path.Combine(leanDir, "Launcher", "config.json")

            if File.Exists candidate then
                parseFile candidate
            else
                parseString
                    """{
                  "environment":"backtesting",
                  "live-mode": false,
                  "algorithm-type-name": "BasicTemplateAlgorithm",
                  "algorithm-language": "CSharp",
                  "algorithm-location": "QuantConnect.Algorithm.CSharp.dll",
                  "data-folder": "data"
                }"""

    let patchCommon (algoType: string) (algoLang: string) (algoDll: string) =
        addPairs
            [ "algorithm-type-name", algoType
              "algorithm-language", algoLang
              "algorithm-location", (algoDll.Replace("\\", "\\\\"))
              "plugin-directory", "./plugins"
              "composer-dll-directory", "./plugins" ]

    let patchBacktest (historyProvider: string) (startOpt: string option) (endOpt: string option) =
        let basePairs =
            [ "environment", "backtesting"
              "live-mode", "false"
              "history-provider", historyProvider ]

        let baseCfg = addPairs basePairs

        let withStart =
            match startOpt with
            | Some s -> merge baseCfg (parseString (sprintf """{"start-date":"%s"}""" s))
            | None -> baseCfg

        match endOpt with
        | Some e -> merge withStart (parseString (sprintf """{"end-date":"%s"}""" e))
        | None -> withStart

    let patchLive
        (brokerage: string)
        (dqHandler: string)
        (apiKey: string option)
        (apiSecret: string option)
        (wsUrl: string option)
        (restUrl: string option)
        =
        let baseCfg =
            addPairs
                [ "environment", "live"
                  "live-mode", "true"
                  "brokerage", brokerage
                  "data-queue-handler", dqHandler ]

        let add k v (acc: JsonElement) =
            match v with
            | Some s -> merge acc (addPairs [ k, s ])
            | None -> acc

        baseCfg
        |> add "uniko-api-key" apiKey
        |> add "uniko-api-secret" apiSecret
        |> add "uniko-ws-url" wsUrl
        |> add "uniko-rest-url" restUrl

    let finalize (tpl: JsonElement) (commonPatch: JsonElement) (modePatch: JsonElement) (overrides: string option) =
        let merged = tpl |> merge commonPatch |> merge modePatch

        match overrides with
        | Some p when File.Exists p -> merge merged (fromAnyFile p)
        | _ -> merged

// ======================================================
// 运行 Lean
// ======================================================
module Runner =
    open Proc

    let runLean (launcherDll: string) (runDir: string) =
        Log.Information("启动 Lean：{Dll}", launcherDll)
        let ec = run runDir "dotnet" $"\"{launcherDll}\""

        if ec <> 0 then
            failwithf "Lean 运行失败，退出码 %d" ec

        Log.Information("运行完成，产出目录：{RunDir}", runDir)

// ======================================================
// Main
// ======================================================
module Main =
    open Paths
    open Build
    open ConfigGen
    open Runner

    [<EntryPoint>]
    let main argv =
        Log.Logger <-
            LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File("launcher.log", rollingInterval = RollingInterval.Day, shared = true)
                .CreateLogger()

        let parser = ArgumentParser.Create<CLIArgs>(programName = "launcher")

        try
            let results = parser.Parse argv

            // 通用路径
            let repo = results.GetResult(RepoRoot, defaultValue = guessRepoRoot ())
            let leanDir = results.GetResult(LeanDir, defaultValue = Path.Combine(repo, "lean"))

            let algoDir =
                results.GetResult(AlgorithmDir, defaultValue = Path.Combine(repo, "Algorithm"))

            let outRoot =
                results.GetResult(OutputDir, defaultValue = Path.Combine(repo, "runs"))

            let pluginsRel = results.GetResult(PluginsOut, defaultValue = "plugins")
            let skipBuild = results.Contains NoBuild
            let pluginProjects = results.GetResults PluginProject |> List.ofSeq
            let pluginBins = results.GetResults PluginBin |> List.ofSeq
            Directory.CreateDirectory outRoot |> ignore

            match results.TryGetSubCommand() with
            // ---------------- BACKTEST -----------------
            | Some(Backtest sub) ->
                if not skipBuild then
                    buildAll repo algoDir leanDir pluginProjects

                let algoDll = findAlgoDll algoDir (sub.TryGetResult BacktestArgs.AlgoDll)
                let launcherDll = findLeanLauncherDll leanDir (results.TryGetResult LeanLauncherDll)

                let stamp = DateTime.UtcNow.ToString "yyyyMMdd-HHmmss"
                let runDir = Path.Combine(outRoot, "backtest", stamp) |> ensureDir
                let runPlugins = Path.Combine(runDir, pluginsRel) |> ensureDir
                collectPlugins runPlugins pluginBins pluginProjects

                let tpl = loadTemplate leanDir (sub.TryGetResult BacktestArgs.ConfigTemplate)
                let algoType = sub.GetResult BacktestArgs.AlgoTypeName
                let algoLang = sub.GetResult(BacktestArgs.AlgoLanguage, defaultValue = "FSharp")

                let historyProvider =
                    sub.GetResult(HistoryProvider, defaultValue = "Lean.Extension.SqliteHistoryProvider")

                let commonPatch = patchCommon algoType algoLang algoDll

                let modePatch =
                    patchBacktest historyProvider (sub.TryGetResult Start) (sub.TryGetResult End)

                let finalCfg =
                    finalize tpl commonPatch modePatch (sub.TryGetResult BacktestArgs.Overrides)

                Json.write (Path.Combine(runDir, "config.json")) finalCfg
                Log.Information("配置已写入：{Path}", Path.Combine(runDir, "config.json"))

                runLean launcherDll runDir
                0

            // ---------------- LIVE -----------------
            | Some(Live sub) ->
                if not skipBuild then
                    buildAll repo algoDir leanDir pluginProjects

                let algoDll = findAlgoDll algoDir (sub.TryGetResult AlgoDll)
                let launcherDll = findLeanLauncherDll leanDir (results.TryGetResult LeanLauncherDll)

                let stamp = DateTime.UtcNow.ToString "yyyyMMdd-HHmmss"
                let runDir = Path.Combine(outRoot, "live", stamp) |> ensureDir
                let runPlugins = Path.Combine(runDir, pluginsRel) |> ensureDir
                collectPlugins runPlugins pluginBins pluginProjects

                let tpl = loadTemplate leanDir (sub.TryGetResult ConfigTemplate)
                let algoType = sub.GetResult AlgoTypeName
                let algoLang = sub.GetResult(AlgoLanguage, defaultValue = "FSharp")
                let brokerage = sub.GetResult(Brokerage, defaultValue = "UnikoCrypto")

                let dqHandler =
                    sub.GetResult(DataQueueHandler, defaultValue = "YourCompany.Lean.UnikoCryptoDataQueueHandler")

                let commonPatch = patchCommon algoType algoLang algoDll

                let modePatch =
                    patchLive
                        brokerage
                        dqHandler
                        (sub.TryGetResult ApiKey)
                        (sub.TryGetResult ApiSecret)
                        (sub.TryGetResult WsUrl)
                        (sub.TryGetResult RestUrl)

                let finalCfg = finalize tpl commonPatch modePatch (sub.TryGetResult Overrides)

                Json.write (Path.Combine(runDir, "config.json")) finalCfg
                Log.Information("配置已写入：{Path}", Path.Combine(runDir, "config.json"))

                runLean launcherDll runDir
                0

            // ---------------- 仅构建 -----------------
            | Some Build ->
                buildAll repo algoDir leanDir pluginProjects
                Log.Information("构建完成。")
                0

            // ---------------- Hello / 默认 -----------------
            | Some Hello
            | None ->
                Console.WriteLine("Repo Root : {0}", repo)
                Console.WriteLine("Lean Dir  : {0}", leanDir)
                Console.WriteLine("Algo Dir  : {0}", algoDir)
                Console.WriteLine("Output    : {0}", outRoot)
                Console.WriteLine()
                Console.WriteLine("用法示例：")
                Console.WriteLine("  回测：")

                Console.WriteLine(
                    "    dotnet run --project Launcher -- backtest -t Algorithm.MyAlgo -L FSharp -s 2020-01-01 -e 2020-12-31 -T ./Launcher/Configs/backtest.toml -O ./Launcher/Configs/overrides.toml --plugin-proj ./src/Lean.Extensions.Uniko/Lean.Extensions.Uniko.fsproj"
                )

                Console.WriteLine("  实盘：")

                Console.WriteLine(
                    "    dotnet run --project Launcher -- live -t Algorithm.MyLiveAlgo -L FSharp -b UnikoCrypto -q YourCompany.Lean.UnikoCryptoDataQueueHandler -T ./Launcher/Configs/live.toml -O ./Launcher/Configs/overrides.toml --plugin-proj ./src/Lean.Extensions.Uniko/Lean.Extensions.Uniko.fsproj"
                )

                0
            | _ -> 0
        with ex ->
            Log.Fatal(ex, "Launcher 失败")
            2
