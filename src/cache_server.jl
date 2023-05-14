module CacheServer

using HTTP
using HTTP: forceclose
using DataStructures: CircularBuffer

using ..Utils: ThreadSafe, print_exceptions
import ..Nostr

HOST = Ref("0.0.0.0")
PORT = Ref(8800)
server_task = Ref{Any}(nothing) |> ThreadSafe

exceptions_cnt = Ref(0) |> ThreadSafe
exceptions = CircularBuffer(500) |> ThreadSafe

PRINT_EXCEPTIONS = Ref(false)

function start()
    lock(server_task) do server_task
        @assert server_task[] |> isnothing
        server_task[] = _server()
    end
    nothing
end

function stop()
    lock(server_task) do server_task
        @assert !(server_task[] |> isnothing)
        forceclose(server_task[])
        server_task[] = nothing
    end
end

CacheServerHandlers() = try
    Main.eval(:(CacheServerHandlers))
catch _
    Main.eval(:(PrimalServer.CacheServerHandlers))
end

function _server()
    WebSockets.listen!(HOST[], PORT[]) do ws
        fetch(Threads.@spawn handle_connection(ws))
    end
end

function handle_connection(ws)
    mod = CacheServerHandlers()
    try
        Base.invokelatest(mod.on_connect, ws)
        for msg in ws
            try Base.invokelatest(mod.on_client_message, ws, msg)
            catch ex 
                PRINT_EXCEPTIONS[] && print_exceptions()
                push!(exceptions, (; t = time(), ws, msg, ex))
                exceptions_cnt[] += 1
            end
        end
    catch ex
        push!(exceptions, (; t = time(), ws, ex))
    end
    try Base.invokelatest(mod.on_disconnect, ws) catch _ end
    try WebSockets.close(ws) catch _ end
end

end

