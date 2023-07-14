module CacheServerHandlers

using HTTP.WebSockets
import JSON
using DataStructures: CircularBuffer

import ..Utils
using ..Utils: ThreadSafe, Throttle
import ..Nostr
import ..MetricsLogger

PRINT_EXCEPTIONS = Ref(false)

Tsubid = String
Tfilters = Vector{Any}
struct Conn
    ws::WebSocket
    subs::Dict{Tsubid, Tfilters}
end
conns = Dict{WebSocket, ThreadSafe{Conn}}() |> ThreadSafe

exceptions = CircularBuffer(200) |> ThreadSafe

sendcnt = Ref(0) |> ThreadSafe

max_request_duration = Ref(0.0) |> ThreadSafe
requests_per_period = Ref(0) |> ThreadSafe

function ext_on_connect(ws) end
function ext_on_disconnect(ws) end
function ext_periodic() end
function ext_funcall(funcall, kwargs, kwargs_extra, ws) end

function on_connect(ws)
    conns[ws] = ThreadSafe(Conn(ws, Dict{Tsubid, Tfilters}()))
    ext_on_connect(ws)
end

function on_disconnect(ws)
    delete!(conns, ws)
    ext_on_disconnect(ws)
end

function on_client_message(ws, msg)
    conn = conns[ws]
    d = JSON.parse(msg)
    try
        if d[1] == "REQ"
            subid = d[2]
            filters = d[3:end]
            lock(conn) do conn
                conn.subs[subid] = filters
                initial_filter_handler(conn.ws, subid, filters)
            end
        elseif d[1] == "CLOSE"
            subid = d[2]
            lock(conn) do conn; delete!(conn.subs, subid); end
        end
    catch _
        PRINT_EXCEPTIONS[] && Utils.print_exceptions()
        rethrow()
    end
end

function send(ws::WebSocket, s::String)
    WebSockets.send(ws, s)
end
function send(conn::ThreadSafe{Conn}, s::String)
    lock(conn) do conn
        lock(sendcnt) do sendcnt; sendcnt[] += 1; end
        WebSockets.send(conn.ws, s)
        lock(sendcnt) do sendcnt; sendcnt[] -= 1; end
    end
end

est() = Main.eval(:(cache_storage))
App() = Main.eval(:(App))

function initial_filter_handler(ws::WebSocket, subid, filters)
    ws_id = ws.id

    function sendres(res::Vector)
        for d in res
            send(ws, JSON.json(["EVENT", subid, d]))
        end
        send(ws, JSON.json(["EOSE", subid]))
    end
    function send_error(s::String)
        send(ws, JSON.json(["NOTICE", subid, s]))
        send(ws, JSON.json(["EOSE", subid]))
    end

    function app_funcall(funcall::Symbol, kwargs; kwargs_extra=Pair{Symbol, Any}[])
        ext_funcall(funcall, kwargs, kwargs_extra, ws)
        MetricsLogger.log(r->begin
                              lock(max_request_duration) do max_request_duration
                                  max_request_duration[] = max(max_request_duration[], r.time)
                              end
                              lock(requests_per_period) do requests_per_period
                                  requests_per_period[] += 1
                              end
                              (; funcall, kwargs, ws=string(ws_id), subid)
                          end) do
            fetch(Threads.@spawn Base.invokelatest(getproperty(App(), funcall), est(); kwargs..., kwargs_extra...))
        end |> sendres
    end

    try
        for filt in filters
            if haskey(filt, "cache")
                local filt = filt["cache"]
                funcall = Symbol(filt[1])
                if !(funcall in [:net_stats, :notifications, :notification_counts, :directmsg_count])
                    @assert funcall in App().exposed_functions
                    kwargs = [Symbol(k)=>v for (k, v) in get(filt, 2, Dict())]
                    app_funcall(funcall, kwargs)
                end

            elseif haskey(filt, "ids")
                eids = []
                for s in filt["ids"]
                    try push!(eids, Nostr.EventId(s)) catch _ end
                end
                app_funcall(:events, [:event_ids=>eids])

            elseif haskey(filt, "since") || haskey(filt, "until")
                kwargs = []
                for a in ["since", "until", "limit"]
                    haskey(filt, a) && push!(kwargs, Symbol(a)=>filt[a])
                end
                sendres([haskey(filt, "idsonly") ? (; id=e.id, kind=e.kind, created_at=e.created_at) : e
                         for e in App().events(est(); kwargs...)])
            end
        end
    catch ex
        PRINT_EXCEPTIONS[] && Utils.print_exceptions()
        ex isa TaskFailedException && (ex = ex.task.result)
        send_error(ex isa ErrorException ? ex.msg : "error")
    end
end

function close_connections()
    println("closing all websocket connections")
    lock(conns) do conns
        @sync for conn in collect(values(conns))
            @async try lock(conn) do conn; close(conn.ws); end catch _ end
        end
    end
end

function broadcast_network_stats(d)
    for conn in collect(values(conns))
        for (subid, filters) in lock(conn) do conn; conn.subs; end
            for filt in filters
                if haskey(filt, "cache")
                    local filt = filt["cache"]
                    if "net_stats" in filt
                        @async send(conn, JSON.json(["EVENT", subid, d]))
                        @goto next
                    end
                end
            end
            @label next
        end
    end
end

netstats_task = Ref{Any}(nothing)
netstats_running = Ref(true)
NETSTATS_RATE = Ref(5.0)

periodic_log_stats = Throttle(; period=60.0)
periodic_directmsg_counts = Throttle(; period=1.0)

function netstats_start()
    @assert netstats_task[] |> isnothing
    netstats_running[] = true

    netstats_task[] = 
    errormonitor(@async while netstats_running[]
                     try
                         d = Base.invokelatest(App().network_stats, est())
                         broadcast_network_stats(d)

                         periodic_log_stats() do
                             lock(est().commons.stats) do cache_storage_stats
                                 MetricsLogger.log((; t=time(), cache_storage_stats))
                             end
                         end

                         periodic_directmsg_counts() do
                             MetricsLogger.log(r->(; funcall=:broadcast_directmsg_count)) do
                                 broadcast_directmsg_count()
                             end
                         end

                         ext_periodic()
                     catch ex
                         push!(exceptions, (:netstats, ex))
                     end
                     sleep(1/NETSTATS_RATE[])
                 end)
end

function netstats_stop()
    @assert !(netstats_task[] |> isnothing)
    netstats_running[] = false
    wait(netstats_task[])
    netstats_task[] = nothing
end

function broadcast_directmsg_count()
    for conn in collect(values(conns))
        for (subid, filters) in lock(conn) do conn; conn.subs; end
            for filt in filters
                try
                    if haskey(filt, "cache")
                        filt = filt["cache"]
                        if length(filt) >= 2 && filt[1] == "directmsg_count"
                            pubkey = Nostr.PubKeyId(filt[2]["pubkey"])
                            for d in Base.invokelatest(App().get_directmsg_count, est(); receiver=pubkey)
                                @async send(conn, JSON.json(["EVENT", subid, d]))
                            end
                            @goto next
                        end
                    end
                catch ex
                    push!(exceptions, (:directmsg_counts, filt, ex))
                end
            end
            @label next
        end
    end
end

function broadcast(e::Nostr.Event)
    for conn in collect(values(conns))
        for (subid, filters) in lock(conn) do conn; conn.subs; end
            for filt in filters
                if length(filt) == 1
                    if haskey(filt, "since")
                        @async send(conn, JSON.json(["EVENT", subid, e]))
                    elseif get(filt, "idsonly", nothing) == [""]
                        @async send(conn, JSON.json(["EVENT", subid, (; id=e.id, kind=e.kind, created_at=e.created_at)]))
                    end
                end
            end
        end
    end
end

end
