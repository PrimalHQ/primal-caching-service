module Fetching

import HTTP, JSON, UUIDs
using Printf, Dates
import YAML
import URIs
using DataStructures: CircularBuffer, SortedDict

import ..Utils
using ..Utils

SAVE_MESSAGES = Ref(true)
PROXY_URI = Ref{Union{String,Nothing}}(nothing)
EVENTS_DATA_DIR = Ref{Any}(nothing)

TIMEOUT = Ref(30)
BASE_DELAY = Ref(1)
MAX_DELAY = Ref(300)

HTTP.ConnectionPool.default_connection_limit[] = 10000

Base.@kwdef mutable struct Fetcher <: Utils.Tasked
    active=true
    task=nothing
    threadid=-1

    relay_url::String
    proxy::Union{String, Nothing}
    timeout=TIMEOUT[]
    latest_timestamp::Int=0
    filename=nothing
    filename_date=Date(1,1,1)

    delay=0
    waiting_delay=false

    message_count=0
    exception_count=0
    chars_received=0
    chars_sent=0

    latest_messages=CircularBuffer(200)
    latest_exceptions=CircularBuffer(200)

    ws=Ref{Any}(nothing)
    send_lock=ReentrantLock()
end

relays = Set{String}()

fetchers = Dict{String, Fetcher}()

message_processors = Utils.ThreadSafe(SortedDict{Symbol, Function}())
message_processors_errors = Utils.ThreadSafe(CircularBuffer(200))

randsubid() = join(rand(union('a':'z', '0':'9'), 30))

function mk_produce(funcname, cb; exceptions=nothing, exception_count=nothing, kwargs...)
    ccb = (m=nothing; kwargs2...) -> cb([time(), (; func=funcname, kwargs..., kwargs2...), m])
    # function err(ex, subfunc; kwargs2...)
    #     bio = IOBuffer()
    #     print_exceptions(bio)
    #     ccb(string(ex); event=:exception, subfunc, printout=String(take!(bio)), kwargs2...)
    # end
    function err(ex, subfunc; kwargs2...)
        !isnothing(exception_count) && (exception_count[] += 1)
        !isnothing(exceptions) && push!(exceptions, ex)
        ccb(string(ex); event=:exception, subfunc, printout="disabled to see performance without print_exceptions", kwargs2...)
    end
    function errscope(body, subfunc; rethrow_exc=true, kwargs2...)
        try body()
        catch ex
            err(ex, subfunc; kwargs2...)
            rethrow_exc && rethrow()
        end
    end
    (ccb, err, errscope)
end

incr(fe::Fetcher, prop::Symbol; by=1) = setproperty!(fe, prop, getproperty(fe, prop) + by)

function nostr_fetch_events(
        on_msg::Function, fe::Fetcher; 
        since=Ref(trunc(Int, time())))

    runid = string(UUIDs.uuid4())

    produce, err, errscope = mk_produce(nameof(var"#self#"), 
                                        function (m)
                                            push!(fe.latest_messages, m)
                                            on_msg(m)
                                        end; 
                                        exceptions=fe.latest_exceptions, 
                                        exception_count=@refprop(fe.exception_count),
                                        runid, fe.relay_url, fe.timeout, fe.proxy)
    incstats(prop::Symbol; by=1) = incr(fe, prop; by)

    produce(; event=:started)

    fe.delay = BASE_DELAY[]
    while fe.active
        msg_cnt = Ref(0)
        try
            @sync HTTP.WebSockets.open(fe.relay_url; connect_timeout=fe.timeout, readtimeout=fe.timeout, proxy=fe.proxy) do ws
                close_ws = Ref(false)
                fe.ws[] = ws
                @async (while fe.active && !close_ws[]; sleep(1.0); end; try close(ws) catch _ end)
                try
                    produce((; since=since[]); event=:connected)
                    for qparams in [(;),
                                    (; authors=[(@sprintf "%02x" i) for i in 0:255])]
                        subid = randsubid()
                        q = ["REQ", subid, (; since=since[], qparams...)]
                        produce((; query=q); event=:request)
                        msg = JSON.json(q)
                        incstats(:chars_sent, by=length(msg))
                        lock(fe.send_lock) do; HTTP.WebSockets.send(ws, msg); end
                    end
                    for s in ws
                        msg_cnt[] += 1
                        incstats(:chars_received, by=length(s))
                        d = errscope(:parsing) do; JSON.parse(s); end
                        incstats(:message_count)
                        errscope(:downstream; rethrow_exc=false) do; produce(d; event=:event); end
                        errscope(:since_calc) do
                            if length(d) >= 3 && d[1] == "EVENT"
                                if haskey(d[3], "created_at")
                                    since[] = min(trunc(Int, time()), max(since[], d[3]["created_at"]-1))
                                end
                            end
                        end
                    end
                catch ex
                    err(ex, :websocket)
                end
                close_ws[] = true
            end
        catch ex
            err(ex, :websocket)
        end
        fe.ws[] = nothing

        fe.active || break
        fe.delay = msg_cnt[] > 0 ? BASE_DELAY[] : min(fe.delay*2, MAX_DELAY[])
        produce(; event=:sleep, fe.delay, msg_cnt=msg_cnt[])

        fe.waiting_delay = true
        try
            active_sleep(fe.delay, @refprop(fe.active))
        finally
            fe.waiting_delay = false
        end
    end
end

function relay_root_dir(relay_url)
    okchars = union('a':'z', 'A':'Z', '0':'9')
    @sprintf "%s/%s" EVENTS_DATA_DIR[] map(c->(c in okchars) ? c : '_', relay_url)
end

function message_log_filename(relay_url::String, date::Date)
    dir = @sprintf "%s/%d/%02d" relay_root_dir(relay_url) year(date) month(date)
    isdir(dir) || mkpath(dir)
    @sprintf "%s/%02d.log" dir day(date)
end

function update_fetchers!(relay_urls; wait_to_stop=false, since=0)
    for relay_url in relay_urls
        if !haskey(fetchers, relay_url)
            latest_timestamp_fn = relay_root_dir(relay_url) * "/latest_timestamp"
            if isfile(latest_timestamp_fn)
                try since = parse(Int, load(latest_timestamp_fn)) catch _ end
            end

            proxy = PROXY_URI[]

            fetchers[relay_url] = fetcher = 
            register!(Fetcher(; relay_url, proxy, latest_timestamp = since))

            fetcher.task = Threads.@spawn begin 
                fetcher.threadid = Threads.threadid()
                save_latest_timestamp = Throttle(period=5)
                flog = Ref{Any}(nothing)
                nostr_fetch_events(fetcher; since=@refprop(fetcher.latest_timestamp)) do msg
                    msgjson = JSON.json(msg)

                    if SAVE_MESSAGES[]
                        date = today()
                        if date != fetcher.filename_date
                            fetcher.filename_date = date
                            fetcher.filename = message_log_filename(relay_url, date)
                            isnothing(flog[]) || close(flog[])
                            flog[] = open(fetcher.filename, "a")
                            println("fetching messages from $relay_url to file $(fetcher.filename)")
                        end

                        println(flog[], msgjson)
                        flush(flog[])
                    end

                    lock(message_processors) do mprocs
                        for processor in values(mprocs)
                            try Base.invokelatest(processor, msgjson)
                            catch ex
                                push!(message_processors_errors, (msgjson, ex))
                                #rethrow()
                            end
                        end
                    end

                    save_latest_timestamp() do
                        safe_save(latest_timestamp_fn, "$(fetcher.latest_timestamp)\n")
                    end
                end
                isnothing(flog[]) || close(flog[])
            end
        end
    end

    for (relay_url, fetcher) in collect(fetchers)
        if !(relay_url in relay_urls)
            println("stopping fetcher for $relay_url")
            fetcher.active = false
            wait_to_stop && wait(fetcher.task)
            unregister!(fetcher)
            delete!(fetchers, relay_url)
        end
    end
end

function mon(dt=1.0)
    running = Utils.PressEnterToStop()
    prevs = nothing
    while running[]
        tots = [sum(getproperty(f, sym) for f in values(fetchers); init=0)
                for sym in [:message_count
                            :exception_count
                            :chars_received
                            :chars_sent]]'
        prevs == nothing && (prevs = tots)
        bio = IOBuffer()
        Utils.clear_screen(bio)
        Utils.move_cursor(1, 1)
        println(bio, "msgcnts: ", [f.message_count[] for f in values(fetchers)]')
        println(bio, 
                count([f.message_count[] > 0 for f in values(fetchers)]'), "/", 
                count([f.waiting_delay for f in values(fetchers)]'), "/", 
                length(fetchers),
                " (providing|waiting|total) relays")
        println(bio, "(msgcnt|exccnt|chrrecv|chrsent)/s: ", tots .- prevs)
        println(bio)
        # show(bio, MIME"text/plain"(), GlobalCounters.get())
        # println(bio)
        print(String(take!(bio)))
        flush(stdout)
        prevs = tots
        sleep(dt)
    end
end

function update_relays_from_nostr_watch()
    gh_relays = sort(YAML.load(String(HTTP.get("https://raw.githubusercontent.com/dskvr/nostr-watch/develop/relays.yaml").body))["relays"])
    open("relays.txt", "w+") do f; for url in gh_relays; println(f, url); end; end
end

function sanitize_uri(uri::AbstractString)
    u = URIs.URI(strip(uri))
    host = lowercase(strip(u.host, [' ']))
    path = rstrip(URIs.normpath(u.path), ['/'])
    port = (u.scheme == "ws" && u.port == "80") || (u.scheme == "wss" && u.port == "443") ? URIs.absent : u.port
    URIs.URI(u; host, path, port)
end

function sanitize_valid_relay_url(url)
    (isempty(url) || endswith(url, ".onion")) && return 
    url = split(strip(url), '\n')[1]
    local u = sanitize_uri(url) 
    URIs.isvalid(u) || return 
    occursin(':', u.host) && return 
    startswith(u.host, '[') && return 
    u.host in ["localhost", "127.0.0.1"] && return 
    u
end

function load_relays(dir=pwd())
    empty!(relays)
    for url in readlines("$dir/relays-active.txt") # these have sent us an event at least once in the past
        push!(relays, url)
    end
    for url in vcat([readlines("$dir/$fn")
                     for fn in ["relays.txt",
                                "relays-paid.txt",
                                "relays-mined-from-events.txt",
                                "relays-mined-from-contact-lists.txt",
                               ]]...)
        if !isnothing(local u = try sanitize_valid_relay_url(url) catch _ end)
            push!(relays, string(u))
        end
    end
end

function start(; since=0)
    @show length(relays)
    update_fetchers!(relays; since)
end

function stop()
    update_fetchers!([])
end

function fetch(filter)
    msg = JSON.json(["REQ", randsubid(), filter])
    for fe in values(fetchers)
        if !isnothing(fe.ws[])
            lock(fe.send_lock) do
                try HTTP.WebSockets.send(fe.ws[], msg) catch _ end
                incr(fe, :chars_sent; by=length(msg))
            end
        end
    end
end

end

