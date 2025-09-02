namespace Launcher

open System
open System.IO
open System.Diagnostics
open System.Text.Json
open Argu
open Serilog

// ==============================
// CLI：仅需要 TOML
// ==============================
type BacktestArgs =
    | [<Mandatory; AltCommandLine("-T")>] ConfigToml of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | ConfigToml _ -> "TOML 配置文件路径，或只写文件名（不含路径），将从当前工作目录查找同名 .toml。（注意：TOML 必须包含根级键 algorithm-location）"

type LiveArgs =
    | [<Mandatory; AltCommandLine("-T")>] ConfigToml of string

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | ConfigToml _ -> "TOML 配置文件路径，或只写文件名（不含路径），将从当前工作目录查找同名 .toml。（注意：TOML 必须包含根级键 algorithm-location）"

[<CliPrefix(CliPrefix.None)>]
type CLIArgs =
    | Backtest of ParseResults<BacktestArgs>
    | Live of ParseResults<LiveArgs>
    | Hello

    interface IArgParserTemplate with
        member x.Usage =
            match x with
            | Backtest _ -> "启动回测（仅需 TOML；TOML 必须含 algorithm-location）。"
            | Live _ -> "启动实盘（仅需 TOML；TOML 必须含 algorithm-location）。"
            | Hello -> "打印探测信息。"

// ==============================
// JSON：仅用于写 Lean 可读的 config.json
// ==============================
module Json =
    let write (path: string) (je: JsonElement) =
        use fs = File.Create path
        use w = new Utf8JsonWriter(fs, JsonWriterOptions(Indented = true))
        je.WriteTo(w)
        w.Flush()

    /// 断言根级存在非空字符串 algorithm-location；缺少即抛错
    let assertHasAlgorithmLocation (src: JsonElement) : unit =
        if src.ValueKind <> JsonValueKind.Object then
            failwith "配置根节点不是对象（应为 TOML 的顶层表）。"

        let mutable prop = Unchecked.defaultof<JsonElement>

        if not (src.TryGetProperty("algorithm-location", &prop)) then
            failwith "配置缺少必需键：algorithm-location（请在 TOML 根级指定算法 DLL/脚本路径）。"

        if prop.ValueKind <> JsonValueKind.String then
            failwith "algorithm-location 必须是字符串。"

        let v = prop.GetString()

        if String.IsNullOrWhiteSpace v then
            failwith "algorithm-location 不能为空字符串。"

// ==============================
// TOML -> JsonElement（支持 ${ENV} 展开）
// ==============================
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

    let fromTomlFile (path: string) : JsonElement =
        let text = File.ReadAllText path
        fromTomlText text

// ==============================
// 路径&构建&进程（全部按约定）
// ==============================
module Paths =
    let repoRoot () =
        Directory.GetCurrentDirectory() |> Path.GetFullPath

    let leanDir repo = Path.Combine(repo, "lean")
    let algorithmDir repo = Path.Combine(repo, "Algorithm")

    let ensureDir (p: string) =
        Directory.CreateDirectory(p) |> ignore
        p

    /// - 含分隔符或以 .toml 结尾 => 当成路径；
    /// - 否则从 当前工作目录 寻找 `<name>.toml`
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

module Build =
    open Proc
    open Paths

    let buildProject (repo: string) (projPath: string) =
        Log.Information("构建项目：{Proj}", projPath)
        let ec = run repo "dotnet" $"build \"{projPath}\" -c Release"

        if ec <> 0 then
            failwithf "构建失败（%s），退出码 %d" projPath ec

    let buildAll (repo: string) =
        // Algorithm
        let algoFsproj = Path.Combine(algorithmDir repo, "Algorithm.fsproj")
        let algoCsproj = Path.Combine(algorithmDir repo, "Algorithm.csproj")

        let algoProj =
            if File.Exists algoFsproj then algoFsproj
            elif File.Exists algoCsproj then algoCsproj
            else algorithmDir repo

        buildProject repo algoProj

        // Lean.Launcher
        let leanLauncherProj = Path.Combine(leanDir repo, "Launcher")
        buildProject repo leanLauncherProj

    let findLeanLauncherDll (repo: string) =
        let root = Path.Combine(leanDir repo, "Launcher", "bin")

        let files =
            if Directory.Exists root then
                Directory.GetFiles(root, "QuantConnect.Lean.Launcher.dll", SearchOption.AllDirectories)
            else
                [||]

        if files.Length = 0 then
            failwith "未找到 QuantConnect.Lean.Launcher.dll，请确认已构建 Lean 子模块的 Launcher。"

        files[0]

module Runner =
    open Proc

    let runLean (launcherDll: string) (runDir: string) =
        Log.Information("启动 Lean：{Dll}", launcherDll)
        let ec = run runDir "dotnet" $"\"{launcherDll}\""

        if ec <> 0 then
            failwithf "Lean 运行失败，退出码 %d" ec

        Log.Information("运行完成，产出目录：{RunDir}", runDir)

module DataLink =
    open System.Runtime.InteropServices

    /// 在 runDir 下创建 'data' 指向 repo/lean/Data 的符号链接/目录联结
    let ensureDataLink (repoRoot: string) (runDir: string) =
        let target = Path.Combine(repoRoot, "lean", "Data")
        let link = Path.Combine(runDir, "data")

        if Directory.Exists(link) || File.Exists(link) then
            Log.Information("data 目录已存在：{Link}", link)
        else
            if not (Directory.Exists target) then
                failwithf "未找到 Lean Data 目录：%s" target

            try
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
                    // Windows：尽量建目录联结（不需要管理员权限）
                    let psi = new System.Diagnostics.ProcessStartInfo()
                    psi.FileName <- "cmd.exe"
                    psi.Arguments <- $"/c mklink /J \"{link}\" \"{target}\""
                    psi.CreateNoWindow <- true
                    psi.UseShellExecute <- false
                    let p = System.Diagnostics.Process.Start(psi)
                    p.WaitForExit()

                    if p.ExitCode <> 0 then
                        failwithf "创建 Windows 目录联结失败（mklink /J）。"
                else
                    // Unix/macOS：用符号链接
                    Directory.CreateSymbolicLink(link, target) |> ignore

                Log.Information("已创建 data 链接：{Link} -> {Target}", link, target)
            with ex ->
                Log.Warning(ex, "创建 data 链接失败，将尝试复制最小元数据。")
                // 兜底：至少复制 symbol-properties（体积很小）
                let spSrc =
                    Path.Combine(target, "symbol-properties", "symbol-properties-database.csv")

                let spDstDir = Path.Combine(runDir, "data", "symbol-properties")
                Directory.CreateDirectory(spDstDir) |> ignore
                File.Copy(spSrc, Path.Combine(spDstDir, "symbol-properties-database.csv"), true)
                Log.Information("已复制 symbol-properties 兜底文件。建议仍使用链接以获得完整元数据。")

// ==============================
// Main：只需要 TOML；其它全按约定自动推断
// ==============================
module Main =
    [<EntryPoint>]
    let main argv =
        Log.Logger <-
            LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File("launcher.log", rollingInterval = RollingInterval.Day, shared = true)
                .CreateLogger()

        // 仅保留 backtest/live 两个子命令
        let parser = ArgumentParser.Create<CLIArgs>(programName = "launcher")

        try
            let results = parser.Parse argv
            let repo = Paths.repoRoot ()

            let run (tomlArg: string) (modeName: string) =
                // 1) 构建（按约定项目构建）
                Build.buildAll repo

                // 2) 解析 TOML 路径
                let tomlPath = Paths.resolveTomlPath tomlArg

                // 3) 运行目录
                let outRoot = Path.Combine(repo, "runs")
                Directory.CreateDirectory outRoot |> ignore
                let stamp = DateTime.UtcNow.ToString "yyyyMMdd-HHmmss"
                let runDir = Path.Combine(outRoot, modeName, stamp) |> Paths.ensureDir
                let _plugins = Path.Combine(runDir, "plugins") |> Paths.ensureDir // 空目录也创建，便于 TOML 中的 ./plugins

                // 新增：确保 data 链接存在（让 ./data 指到 <repo>/lean/Data）
                DataLink.ensureDataLink repo runDir

                // 4) TOML -> JSON；**强制校验 algorithm-location 必须存在**
                let cfg = ConfigParse.fromTomlFile tomlPath
                Json.assertHasAlgorithmLocation cfg

                // 5) 写出 config.json & 启动
                let cfgPath = Path.Combine(runDir, "config.json")
                Json.write cfgPath cfg

                let leanLauncherDll = Build.findLeanLauncherDll repo
                Runner.runLean leanLauncherDll runDir
                0

            match results.TryGetSubCommand() with
            | Some(CLIArgs.Backtest sub) ->
                let tomlArg = sub.GetResult BacktestArgs.ConfigToml
                run tomlArg "backtest"

            | Some(CLIArgs.Live sub) ->
                let tomlArg = sub.GetResult LiveArgs.ConfigToml
                run tomlArg "live"

            | Some CLIArgs.Hello
            | None ->
                let repo = Paths.repoRoot ()
                Console.WriteLine("Repo Root : {0}", repo)
                Console.WriteLine("Lean Dir  : {0}", Paths.leanDir repo)
                Console.WriteLine("Algo Dir  : {0}", Paths.algorithmDir repo)
                Console.WriteLine()
                Console.WriteLine("用法：")
                Console.WriteLine("  回测（传完整路径或仅文件名）：")
                Console.WriteLine("    dotnet run --project Launcher -- backtest -T ./Launcher/backtesting.toml")
                Console.WriteLine("    dotnet run --project Launcher -- backtest -T backtesting")
                Console.WriteLine("  实盘：")
                Console.WriteLine("    dotnet run --project Launcher -- live -T live")
                Console.WriteLine()
                Console.WriteLine("提示：TOML 必须包含根级键 algorithm-location（算法 DLL/脚本路径）。")
                0

        with ex ->
            Log.Fatal(ex, "Launcher 失败")
            2
