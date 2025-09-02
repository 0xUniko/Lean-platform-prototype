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
open System.Threading
open QuantConnect
open QuantConnect.Configuration
open QuantConnect.Lean.Engine
open QuantConnect.Logging
open QuantConnect.Packets
open QuantConnect.Python
open QuantConnect.Util

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

    [<EntryPoint>]
    let main (args: string array) =
        if OS.IsWindows then
            Console.OutputEncoding <- Text.Encoding.UTF8

        if args.Length > 0 then
            Config.MergeCommandLineArgumentsWithConfiguration(LeanArgumentParser.ParseArguments args)

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
