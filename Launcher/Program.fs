namespace Launcher

open System
open System.IO
open System.Diagnostics
open System.Text.Json
open System.Text.Json.Serialization
open Argu
open Serilog

// ======================================================
// CLI 定义（只保留 Toml 配置，不再支持 Json/合并/补丁）
// ======================================================
type BacktestArgs =
    | [<Mandatory; AltCommandLine("-T")>] ConfigToml of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | ConfigToml _ -> "TOML 配置文件路径，或只写文件名（不含路径），将从当前工作目录查找同名 .toml。"

type LiveArgs =
    | [<Mandatory; AltCommandLine("-T")>] ConfigToml of string
    | [<AltCommandLine("-b")>] Brokerage_IGNORED of string
    | [<AltCommandLine("-q")>] DataQueueHandler_IGNORED of string
    | [<AltCommandLine("--api-key")>] ApiKey_IGNORED of string
    | [<AltCommandLine("--api-secret")>] ApiSecret_IGNORED of string
    | [<AltCommandLine("--ws-url")>] WsUrl_IGNORED of string
    | [<AltCommandLine("--rest-url")>] RestUrl_IGNORED of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | ConfigToml _ -> "TOML 配置文件路径，或只写文件名（不含路径），将从当前工作目录查找同名 .toml。"
            | Brokerage_IGNORED _ -> "[已忽略] 仅为兼容旧参数，当前不再对 TOML 打补丁。"
            | DataQueueHandler_IGNORED _ -> "[已忽略] 仅为兼容旧参数，当前不再对 TOML 打补丁。"
            | ApiKey_IGNORED _ -> "[已忽略]"
            | ApiSecret_IGNORED _ -> "[已忽略]"
            | WsUrl_IGNORED _ -> "[已忽略]"
            | RestUrl_IGNORED _ -> "[已忽略]"

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
            | Backtest _ -> "启动回测（只接受 TOML 配置）。"
            | Live _ -> "启动实盘（只接受 TOML 配置）。"
            | Build -> "仅构建（Algorithm、Lean.Launcher、插件）。"
            | Hello -> "打印探测信息。"

// ======================================================
// JSON 工具（仅用于写出 Lean 读取的 config.json）
// ======================================================
module Json =
    let options =
        let o = JsonSerializerOptions(WriteIndented = true)
        o.DefaultIgnoreCondition <- JsonIgnoreCondition.WhenWritingNull
        o

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
        // 环境变量占位展开
        let text = expandEnv text
        // Toml -> 动态模型
        let model = Toml.ToModel(text)

        // 递归把 TomlTable/TomlArray 转成普通 .NET 值
        let rec tomlToObj (t: obj) : obj =
            match t with
            | :? TomlTable as tbl ->
                let d = System.Collections.Generic.Dictionary<string, obj>()

                for KeyValue(k, v) in tbl do
                    d.[k] <- tomlToObj v

                d :> obj
            | :? TomlArray as arr ->
                let l = System.Collections.Generic.List<obj>()

                for v in arr do
                    l.Add(tomlToObj v)

                l :> obj
            // 叶子：Tomlyn 已经给了原生 CLR 类型
            | :? string
            | :? bool
            | :? int64
            | :? double
            | :? decimal
            | :? DateTime
            | :? DateTimeOffset -> t
            | null -> null
            | _ -> t

        toJsonElement (tomlToObj model)

    let fromTomlFile (path: string) : JsonElement =
        let text = File.ReadAllText path
        fromTomlText text

// ======================================================
// 路径解析 & 进程工具
// ======================================================
module Paths =
    let norm (p: string) = Path.GetFullPath p

    let ensureDir (p: string) =
        Directory.CreateDirectory(p) |> ignore
        p

    let guessRepoRoot () = Directory.GetCurrentDirectory() |> norm

    /// 解析 -T 传入的参数：
    /// - 如果包含路径分隔符或以 .toml 结尾，则按“路径”解析；
    /// - 否则按“文件名”，在 当前工作目录 下寻找 `<name>.toml`
    let resolveTomlPath (input: string) =
        let hasSep =
            input.Contains(Path.DirectorySeparatorChar)
            || input.Contains(Path.AltDirectorySeparatorChar)

        let isToml = input.EndsWith(".toml", StringComparison.OrdinalIgnoreCase)

        if hasSep || isToml then
            let p = if isToml then input else input + ".toml"

            if File.Exists p then
                Path.GetFullPath p
            else
                failwithf "未找到 TOML 配置文件：%s" p
        else
            // 仅名字：从 当前工作目录 查找
            let p = Path.Combine(Directory.GetCurrentDirectory(), input + ".toml")

            if File.Exists p then
                Path.GetFullPath p
            else
                failwithf "未在当前工作目录找到 TOML 配置文件：%s" p

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

// ======================================================
// 构建/探测/拷贝插件 & Lean 启动
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

    let findLeanLauncherDll (leanDir: string) (cliDll: string option) =
        match cliDll with
        | Some p when File.Exists p -> p
        | _ ->
            let root = Path.Combine(leanDir, "Launcher", "bin")

            let rec findFirst (root: string) (pattern: string) =
                if Directory.Exists root then
                    Directory.GetFiles(root, pattern, SearchOption.AllDirectories) |> Array.tryHead
                else
                    None

            match findFirst root "QuantConnect.Lean.Launcher.dll" with
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

module Runner =
    open Proc

    let runLean (launcherDll: string) (runDir: string) =
        Log.Information("启动 Lean：{Dll}", launcherDll)
        let ec = run runDir "dotnet" $"\"{launcherDll}\""

        if ec <> 0 then
            failwithf "Lean 运行失败，退出码 %d" ec

        Log.Information("运行完成，产出目录：{RunDir}", runDir)

// ======================================================
// Main（不再对 TOML 做任何补丁/合并）
// ======================================================
module Main =
    open Paths
    open Build
    open Runner
    open ConfigParse

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

                let launcherDll = findLeanLauncherDll leanDir (results.TryGetResult LeanLauncherDll)

                // 解析 TOML 路径（支持只给名字）
                let tomlArg = sub.GetResult BacktestArgs.ConfigToml
                let tomlPath = resolveTomlPath tomlArg

                // Run 目录与 plugins
                let stamp = DateTime.UtcNow.ToString "yyyyMMdd-HHmmss"
                let runDir = Path.Combine(outRoot, "backtest", stamp) |> ensureDir
                let runPlugins = Path.Combine(runDir, pluginsRel) |> ensureDir
                collectPlugins runPlugins pluginBins pluginProjects

                // TOML -> JSON（不做任何合并/补丁），写出 config.json
                let je = fromTomlFile tomlPath
                Json.write (Path.Combine(runDir, "config.json")) je
                Log.Information("配置已写入（由 TOML 原样转换）：{Path}", Path.Combine(runDir, "config.json"))

                runLean launcherDll runDir
                0

            // ---------------- LIVE -----------------
            | Some(Live sub) ->
                if not skipBuild then
                    buildAll repo algoDir leanDir pluginProjects

                let launcherDll = findLeanLauncherDll leanDir (results.TryGetResult LeanLauncherDll)

                let tomlArg = sub.GetResult ConfigToml
                let tomlPath = resolveTomlPath tomlArg

                let stamp = DateTime.UtcNow.ToString "yyyyMMdd-HHmmss"
                let runDir = Path.Combine(outRoot, "live", stamp) |> ensureDir
                let runPlugins = Path.Combine(runDir, pluginsRel) |> ensureDir
                collectPlugins runPlugins pluginBins pluginProjects

                let je = fromTomlFile tomlPath
                Json.write (Path.Combine(runDir, "config.json")) je
                Log.Information("配置已写入（由 TOML 原样转换）：{Path}", Path.Combine(runDir, "config.json"))

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
                Console.WriteLine("  回测（传完整路径）：")
                Console.WriteLine("    dotnet run --project Launcher -- backtest -T ./Launcher/backtesting.toml")
                Console.WriteLine("  回测（仅传文件名，从当前工作目录查找同名 .toml）：")
                Console.WriteLine("    dotnet run --project Launcher -- backtest -T backtesting")
                Console.WriteLine("  实盘（同理）：")
                Console.WriteLine("    dotnet run --project Launcher -- live -T live")
                0
            | _ -> 0
        with ex ->
            Log.Fatal(ex, "Launcher 失败")
            2
