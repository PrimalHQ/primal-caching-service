#module DB

using DataStructures: Accumulator, CircularBuffer, SortedDict
using UnicodePlots: barplot
using BenchmarkTools: prettytime
using Printf: @sprintf
import StaticArrays

using .Threads: @threads
using Lazy: @forward
import Dates

import ..Utils
using ..Utils: ThreadSafe, Throttle, GCTask, stop

import ..Nostr
import ..Bech32
import ..Fetching

include("sqlite.jl")

hashfunc(::Type{Nostr.EventId}) = eid->eid.hash[32]
hashfunc(::Type{Nostr.PubKeyId}) = pk->pk.pk[32]
hashfunc(::Type{Tuple{Nostr.PubKeyId, Nostr.EventId}}) = p->p[1].pk[32]

MAX_MESSAGE_SIZE = Ref(100_000) |> ThreadSafe
kindints = map(Int, instances(Nostr.Kind))

term_lines = try parse(Int, strip(read(`tput lines`, String))) catch _ 15 end
threadprogress = Dict{Int, Any}() |> ThreadSafe
PROGRESS_COMPLETE = -1
progress_periodic_update = Throttle(; period=0.2)
function term_progress(fn, cnt, stats, progstr, tstart)
    #return
    tid = Threads.threadid()
    function render()
        bio = IOBuffer()
        Utils.clear_screen(bio)

        m = 1
        for (tid, (fn, cnt)) in sort(collect(threadprogress), by=kv->kv[1])
            Utils.move_cursor(bio, max(term_lines-tid-m, 1), 1)
            print(bio, @sprintf "%2d %9s  %s" tid (cnt == PROGRESS_COMPLETE ? "" : string(cnt)) fn)
        end

        Utils.move_cursor(bio, term_lines-m, 1)
        print(bio, "$(trunc(Int, time()-tstart))s $(progstr) ",
              (@sprintf "gclive: %.1fGB" Base.gc_live_bytes()/1024^3),
              " #msg: $(stats[:msg]) #any: $(stats[:any]) #exceptions: $(stats[:exceptions]) #eventhooks: $(stats[:eventhooks])-$(stats[:eventhooksexecuted]) ")

        Utils.move_cursor(bio, term_lines, 1)

        print(String(take!(bio)))
        flush(stdout)
    end
    lock(threadprogress) do threadprogress
        threadprogress[tid] = (cnt == PROGRESS_COMPLETE ? "" : fn, cnt)
        cnt == PROGRESS_COMPLETE ? render() : progress_periodic_update(render)
    end
end

SHOW_PROGRESS = Ref(true)
PRINT_EXCEPTIONS = Ref(false)

function incr(v::Ref; by=1)
    by == 0 && return
    v[] += by
end
function incr(v::ThreadSafe; by=1)
    by == 0 && return
    lock(v) do v
        incr(v; by)
    end
end
decr(v; by=1) = incr(v; by=-by)

scope_times = Dict{Symbol, Float64}()
function scope(body::Function, sym::Symbol)
    r = @timed body()
    incr(scope_times, sym; by=r.time)
    r.value
end

stat_names = Set([:msg,
                  :verifyerr,
                  :seen,
                  :seenearly,
                  :parseerr,
                  :exceptions,
                  :any,
                  :tags,
                  :pubnotes,
                  :directmsgs,
                  :eventdeletions,
                  :reactions,
                  :replies,
                  :mentions,
                  :reposts,
                  :pubkeys,
                  :users,
                  :zaps,
                  :satszapped,
                  :eventhooks,
                  :eventhooksexecuted,
                  :scheduledhooks,
                  :scheduledhooksexecuted,
                 ])

function load_stats(stats_filename::String)
    stats = Dict{Symbol, Int}()
    isfile(stats_filename) && open(stats_filename) do f
        for (k, v) in JSON.parse(read(f, String))
            stats[Symbol(k)] = v 
        end
    end
    stats
end

Base.@kwdef struct StorageCommons
    directory::String

    dbargs = (; )

    tstart = Ref(time())

    exceptions = CircularBuffer(500) |> ThreadSafe

    latest_file_positions = SqliteDict(String, Int, "$directory/latest_file_positions"; dbargs...) |> ThreadSafe

    stats_filename = "$directory/stats.json"
    stats = load_stats(stats_filename) |> ThreadSafe
    stats_saving_periodically = Throttle(; period=0.2)

    message_processors = SortedDict{Symbol, Function}() |> ThreadSafe

    gc_task = Ref{Union{Nothing, GCTask}}(nothing)

    ext::Any = nothing
end

function load_stats(commons::StorageCommons)
    lock(commons.stats) do stats
        merge!(stats, load_stats(commons.stats_filename))
    end
end

function save_stats(commons::StorageCommons)
    lock(commons.stats) do stats
        open(commons.stats_filename * ".tmp", "w+") do f
            write(f, JSON.json(stats))
        end
        mv(commons.stats_filename * ".tmp", commons.stats_filename; force=true)
    end
end

function import_msg_into_storage(msg::String, commons::StorageCommons)
    commons.stats_saving_periodically() do 
        save_stats(commons)
    end
end

abstract type EventStorage end

function incr(est::EventStorage, prop::Symbol; by=1)
    incr(est.commons.stats, prop; by)
end

function catch_exception(body::Function, est::EventStorage, args...)
    try
        body()
    catch ex
        incr(est, :exceptions)
        push!(est.commons.exceptions, (Dates.now(), ex, args...))
        PRINT_EXCEPTIONS[] && print_exceptions()
    end
end

struct OpenedFile
    io::IOStream
    t_last_write::Ref{Float64}
end

Base.@kwdef struct DeduplicatedEventStorage <: EventStorage
    directory::String

    dbargs = (; )
    commons = StorageCommons(; directory, dbargs)

    deduped_events = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/deduped_events"; commons.dbargs...,
                                      init_queries=["create table if not exists kv (
                                                       event_id blob not null,
                                                       created_at int not null,
                                                       processed_at int not null,
                                                       file_position int not null,
                                                       times_seen int not null
                                                     )",
                                                    "create index if not exists kv_event_id on kv (event_id asc)",
                                                    "create index if not exists kv_created_at on kv (created_at asc)",
                                                   ])

    files = Dict{String, OpenedFile}() |> ThreadSafe

    max_open_file_age = 1*60
    close_files_periodically = Throttle(; period=5.0)
end

Base.@kwdef struct CacheStorage <: EventStorage
    directory::String

    dbargs = (; )
    commons = StorageCommons(; directory, dbargs)

    verification_enabled = true

    auto_fetch_missing_events = false

    event_processors = SortedDict{Symbol, Function}() |> ThreadSafe

    tidcnts = Accumulator{Int, Int}() |> ThreadSafe

    periodic_task_running = Ref(false)
    periodic_task = Ref{Union{Nothing, Task}}(nothing)

    event_ids      = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_ids"; commons.dbargs...)
    events         = ShardedSqliteDict{Nostr.EventId, Nostr.Event}("$(commons.directory)/db/events"; commons.dbargs...)
    event_created_at = ShardedSqliteDict{Nostr.EventId, Int}("$(commons.directory)/db/event_created_at"; commons.dbargs...,
                                                             keycolumn="event_id", valuecolumn="created_at",
                                                             init_queries=["create table if not exists kv (
                                                                              event_id blob not null,
                                                                              created_at int not null
                                                                            )",
                                                                           "create index if not exists kv_event_id on kv (event_id asc)",
                                                                           "create index if not exists kv_created_at on kv (created_at desc)"])

    pubkey_events = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/pubkey_events"; commons.dbargs..., # no replies, no mentions
                                     init_queries=["create table if not exists kv (
                                                      pubkey blob not null,
                                                      event_id blob not null,
                                                      created_at int not null,
                                                      is_reply int not null
                                                    )",
                                                   "create index if not exists kv_pubkey on kv (pubkey asc)",
                                                   "create index if not exists kv_created_at on kv (created_at asc)"])
    pubkey_ids = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/pubkey_ids"; commons.dbargs...)

    pubkey_followers = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/pubkey_followers"; commons.dbargs...,
                                        init_queries=["create table if not exists kv (
                                                         pubkey blob not null,
                                                         follower_pubkey blob not null,
                                                         follower_contact_list_event_id blob not null
                                                       )",
                                                      "create index if not exists kv_pubkey on kv (pubkey asc)",
                                                      "create index if not exists kv_follower_pubkey on kv (follower_pubkey asc)",
                                                      "create index if not exists kv_follower_contact_list_event_id on kv (follower_contact_list_event_id asc)"])
    pubkey_followers_cnt = ShardedSqliteDict{Nostr.PubKeyId, Int}("$(commons.directory)/db/pubkey_followers_cnt"; commons.dbargs...,
                                                                  init_extra_indexes=["create index if not exists kv_value on kv (value desc)"])

    contact_lists = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/contact_lists"; commons.dbargs...)
    meta_data = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/meta_data"; commons.dbargs...)

    event_stats_init_queries = ["create table if not exists kv (
                                   event_id blob not null,
                                   author_pubkey blob not null,
                                   created_at int not null,
                                   likes int not null,
                                   replies int not null,
                                   mentions int not null,
                                   reposts int not null,
                                   zaps int not null,
                                   satszapped int not null,
                                   score int not null,
                                   score24h int not null
                                )",
                                "create index if not exists kv_event_id on kv (event_id asc)",
                                "create index if not exists kv_author_pubkey on kv (author_pubkey asc)",
                                "create index if not exists kv_created_at on kv (created_at desc)",
                                "create index if not exists kv_score on kv (score desc)",
                                "create index if not exists kv_score24h on kv (score24h desc)",
                                "create index if not exists kv_satszapped on kv (satszapped desc)",
                               ]
    event_stats = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_stats"; commons.dbargs...,
                                   init_queries=event_stats_init_queries)
    event_stats_by_pubkey = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/event_stats_by_pubkey"; commons.dbargs...,
                                             init_queries=event_stats_init_queries)

    event_replies = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_replies"; commons.dbargs...,
                                     init_queries=["create table if not exists kv (
                                                      event_id blob not null,
                                                      reply_event_id blob not null,
                                                      reply_created_at not null
                                                    )",
                                                   "create index if not exists kv_event_id on kv (event_id asc)",
                                                   "create index if not exists kv_reply_created_at on kv (reply_created_at asc)"])
    event_thread_parents = ShardedSqliteDict{Nostr.EventId, Nostr.EventId}("$(commons.directory)/db/event_thread_parents"; commons.dbargs...)

    event_hooks = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_hooks"; commons.dbargs...,
                                   init_queries=["create table if not exists kv (
                                                    event_id blob not null,
                                                    funcall text not null
                                                  )",
                                                 "create index if not exists kv_event_id on kv (event_id asc)"])

    scheduled_hooks = SqliteDict(Int, String, "$(commons.directory)/db/scheduled_hooks"; commons.dbargs...,
                                 init_queries=["create table if not exists kv (
                                                  execute_at not null,
                                                  funcall text not null
                                               )",
                                               "create index if not exists kv_execute_at on kv (execute_at asc)"])

    event_pubkey_actions = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_pubkey_actions"; commons.dbargs...,
                                            init_queries=["create table if not exists kv (
                                                             event_id blob not null,
                                                             pubkey blob not null,
                                                             created_at int not null,
                                                             updated_at int not null,
                                                             replied int not null,
                                                             liked int not null,
                                                             reposted int not null,
                                                             zapped int not null,
                                                             primary key (event_id, pubkey)
                                                          )",
                                                          "create index if not exists kv_event on kv (event_id asc)",
                                                          "create index if not exists kv_pubkey on kv (pubkey asc)",
                                                          "create index if not exists kv_created_at on kv (created_at desc)",
                                                          "create index if not exists kv_updated_at on kv (updated_at desc)",
                                                         ])

    event_pubkey_action_refs = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/event_pubkey_action_refs"; commons.dbargs...,
                                                init_queries=["create table if not exists kv (
                                                                 event_id blob not null,
                                                                 ref_event_id blob not null,
                                                                 ref_pubkey blob not null,
                                                                 ref_created_at int not null,
                                                                 ref_kind int not null
                                                              )",
                                                              "create index if not exists kv_event_id on kv (event_id asc)",
                                                              "create index if not exists kv_ref_event_id on kv (ref_event_id asc)",
                                                              "create index if not exists kv_ref_created_at on kv (ref_created_at desc)",
                                                              "create index if not exists kv_event_id_ref_pubkey on kv (event_id asc, ref_pubkey asc)",
                                                              "create index if not exists kv_event_id_ref_kind on kv (event_id asc, ref_kind asc)",
                                                             ])

    deleted_events = ShardedSqliteDict{Nostr.EventId, Nostr.EventId}("$(commons.directory)/db/deleted_events"; commons.dbargs...,
                                                                     keycolumn="event_id", valuecolumn="deletion_event_id")

    mute_list   = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/mute_list"; commons.dbargs...)
    mute_list_2 = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/mute_list_2"; commons.dbargs...)
    mute_lists  = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/mute_lists"; commons.dbargs...)
    allow_list  = ShardedSqliteDict{Nostr.PubKeyId, Nostr.EventId}("$(commons.directory)/db/allow_list"; commons.dbargs...)
    parameterized_replaceable_list = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/parameterized_replaceable_list"; commons.dbargs...,
                                                      table="parameterized_replaceable_list", 
                                                      init_queries=["create table if not exists parameterized_replaceable_list (
                                                                    pubkey blob not null,
                                                                    identifier text not null,
                                                                    created_at integer not null,
                                                                    event_id blob not null
                                                                    )",
                                                                    "create index if not exists parameterized_replaceable_list_pubkey on     parameterized_replaceable_list (pubkey asc)",
                                                                    "create index if not exists parameterized_replaceable_list_identifier on parameterized_replaceable_list (identifier asc)",
                                                                    "create index if not exists parameterized_replaceable_list_created_at on parameterized_replaceable_list (created_at desc)"])

    pubkey_directmsgs = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/pubkey_directmsgs"; commons.dbargs...,
                                         table="pubkey_directmsgs", keycolumn="receiver", valuecolumn="event_id",
                                         init_queries=["create table if not exists pubkey_directmsgs (
                                                          receiver blob not null,
                                                          sender blob not null,
                                                          created_at integer not null,
                                                          event_id blob not null
                                                       )",
                                                       "create index if not exists pubkey_directmsgs_receiver   on pubkey_directmsgs (receiver asc)",
                                                       "create index if not exists pubkey_directmsgs_sender     on pubkey_directmsgs (sender asc)",
                                                       "create index if not exists pubkey_directmsgs_receiver_event_id on pubkey_directmsgs (receiver asc, event_id asc)",
                                                       "create index if not exists pubkey_directmsgs_created_at on pubkey_directmsgs (created_at desc)"])

    pubkey_directmsgs_cnt = ShardedSqliteSet(Nostr.PubKeyId, "$(commons.directory)/db/pubkey_directmsgs_cnt"; commons.dbargs...,
                                             table="pubkey_directmsgs_cnt", keycolumn="receiver", valuecolumn="cnt",
                                             init_queries=["create table if not exists pubkey_directmsgs_cnt (
                                                              receiver blob not null,
                                                              sender blob,
                                                              cnt integer not null,
                                                              latest_at integer not null,
                                                              latest_event_id blob not null
                                                           )",
                                                           "create index if not exists pubkey_directmsgs_cnt_receiver on pubkey_directmsgs_cnt (receiver asc)",
                                                           "create index if not exists pubkey_directmsgs_cnt_sender   on pubkey_directmsgs_cnt (sender asc)",
                                                           "create index if not exists pubkey_directmsgs_cnt_receiver_sender on pubkey_directmsgs_cnt (receiver asc, sender asc)",
                                                          ])
    pubkey_directmsgs_cnt_lock = ReentrantLock()

    zap_receipts = ShardedSqliteSet(Nostr.EventId, "$(commons.directory)/db/zap_receipts"; commons.dbargs...,
                                   table="zap_receipts", 
                                   init_queries=["create table if not exists zap_receipts (
                                                 zap_receipt_id blob not null,
                                                 created_at int not null,
                                                 sender blob,
                                                 receiver blob,
                                                 amount_sats int not null,
                                                 event_id blob
                                                 )",
                                                 "create index if not exists zap_receipts_sender on zap_receipts (sender asc)",
                                                 "create index if not exists zap_receipts_receiver on zap_receipts (receiver asc)",
                                                 "create index if not exists zap_receipts_created_at on zap_receipts (created_at asc)",
                                                 "create index if not exists zap_receipts_amount_sats on zap_receipts (amount_sats desc)",
                                                 "create index if not exists zap_receipts_event_id on zap_receipts (event_id asc)",
                                                ])

    ext = Ref{Any}(nothing)
    dyn = Dict()
end

function close_conns(est)
    for a in propertynames(est)
        v = getproperty(est, a)
        if v isa ShardedDBDict
            println("closing $a")
            close(v)
        end
    end
end

function Base.close(est::DeduplicatedEventStorage)
    close_conns(est)
    close_conns(est.commons)
end

function Base.close(est::CacheStorage)
    close_conns(est)
    close_conns(est.commons)
    est.periodic_task_running[] = false
    wait(est.periodic_task[])
end

function Base.delete!(est::EventStorage)
    close(est)
    run(`sh -c "rm -frv $(est.commons.directory)"`)
end

function event_stats_cb(est::CacheStorage, e::Nostr.Event, prop, increment)
    exe(est.event_stats,           "update kv set $prop = $prop + ?2 where event_id = ?1", e.id, increment)
    exe(est.event_stats_by_pubkey, "update kv set $prop = $prop + ?3 where event_id = ?2", e.pubkey, e.id, increment)
end

function event_hook_execute(est::CacheStorage, e::Nostr.Event, funcall::Tuple)
    catch_exception(est, e, funcall) do
        funcname, args... = funcall
        eval(Symbol(funcname))(est, e, args...)
        incr(est, :eventhooksexecuted)
    end
end

function event_hook(est::CacheStorage, eid::Nostr.EventId, funcall::Tuple)
    if eid in est.events
        e = est.events[eid]
        event_hook_execute(est, e, Tuple(JSON.parse(JSON.json(funcall))))
    else
        exe(est.event_hooks, @sql("insert into kv (event_id, funcall) values (?1, ?2)"), eid, JSON.json(funcall))
    end
    incr(est, :eventhooks)
end

function scheduled_hook_execute(est::CacheStorage, funcall::Tuple)
    catch_exception(est, funcall) do
        funcname, args... = funcall
        eval(Symbol(funcname))(est, args...)
        incr(est, :scheduledhooksexecuted)
    end
end

function schedule_hook(est::CacheStorage, execute_at::Int, funcall::Tuple)
    if execute_at <= time()
        scheduled_hook_execute(est, funcall)
    else
        exec(est.scheduled_hooks, @sql("insert into kv (execute_at, funcall) values (?1, ?2)"), (execute_at, JSON.json(funcall)))
    end
    incr(est, :scheduledhooks)
end

function track_user_stats(body::Function, est::CacheStorage, pubkey::Nostr.PubKeyId)
    function isuser()
        pubkey in est.meta_data && pubkey in est.contact_lists && 
        est.contact_lists[pubkey] in est.events &&
        length([t for t in est.events[est.contact_lists[pubkey]].tags
                if length(t.fields) >= 2 && t.fields[1] == "p"]) > 0
    end
    isuser_pre = isuser()
    body()
    isuser_post = isuser()
    if     !isuser_pre &&  isuser_post; incr(est, :users)
    elseif  isuser_pre && !isuser_post; incr(est, :users; by=-1)
    end
end

function event_pubkey_action(est::CacheStorage, eid::Nostr.EventId, re::Nostr.Event, action::Symbol)
    exe(est.event_pubkey_actions, @sql("insert or ignore into kv values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"), 
        eid, re.pubkey, re.created_at, 0, false, false, false, false)
    exe(est.event_pubkey_actions, "update kv set $(action) = true, updated_at = ?3 where event_id = ?1 and pubkey = ?2", 
        eid, re.pubkey, re.created_at)
    exe(est.event_pubkey_action_refs, @sql("insert into kv values (?1, ?2, ?3, ?4, ?5)"), 
        eid, re.id, re.pubkey, re.created_at, re.kind)
end

function zap_sender(zap_receipt::Nostr.Event)
    for tag in zap_receipt.tags
        if length(tag.fields) >= 2 && tag.fields[1] == "description"
            return Nostr.PubKeyId(JSON.parse(tag.fields[2])["pubkey"])
        end
    end
    error("invalid zap receipt event")
end

function zap_receiver(zap_receipt::Nostr.Event)
    for tag in zap_receipt.tags
        if length(tag.fields) >= 2 && tag.fields[1] == "p"
            return Nostr.PubKeyId(tag.fields[2])
        end
    end
    error("invalid zap receipt event")
end

function parse_bolt11(b::String)
    if startswith(b, "lnbc")
        amount_digits = Char[]
        unit = nothing
        for i in 5:length(b)
            c = b[i]
            if isdigit(c)
                push!(amount_digits, c)
            else
                unit = c
                break
            end
        end
        amount_sats = parse(Int, join(amount_digits)) * 100_000_000
        if     unit == 'm'; amount_sats = amount_sats ÷ 1_000
        elseif unit == 'u'; amount_sats = amount_sats ÷ 1_000_000
        elseif unit == 'n'; amount_sats = amount_sats ÷ 1_000_000_000
        elseif unit == 'p'; amount_sats = amount_sats ÷ 1_000_000_000_000
        end
        amount_sats
    else
        nothing
    end
end

MAX_SATSZAPPED = Ref(1_000_000)

re_hashref = r"\#\[([0-9]*)\]"
re_mention = r"\bnostr:((note|npub|naddr|nevent|nprofile)1\w+)\b"

function for_mentiones(body::Function, est::CacheStorage, e::Nostr.Event; pubkeys_in_content=true)
    e.kind == Int(Nostr.TEXT_NOTE) || return
    mentiontags = Set()
    for m in eachmatch(re_hashref, e.content)
        ref = 1 + try parse(Int, m.captures[1]) catch _ continue end
        ref <= length(e.tags) || continue
        tag = e.tags[ref]
        length(tag.fields) >= 2 || continue
        push!(mentiontags, tag)
    end
    for tag in e.tags
        length(tag.fields) >= 4 && tag.fields[4] == "mention" && push!(mentiontags, tag)
    end
    for m in eachmatch(re_mention, e.content)
        s = m.captures[1]
        catch_exception(est, e, m) do
            if !isnothing(local id = try Bech32.nip19_decode_wo_tlv(s) catch _ end)
                id isa Nostr.PubKeyId || id isa Nostr.EventId || return
                id isa Nostr.PubKeyId && !pubkeys_in_content && return
                push!(mentiontags, Nostr.TagAny([id isa Nostr.PubKeyId ? "p" : "e", Nostr.hex(id)]))
            end
        end
    end
    for tag in mentiontags
        body(tag)
    end
end

function event_from_msg(m)
    t, md, d = m
    if d != nothing && length(d) >= 3 && d[1] == "EVENT"
        return Nostr.dict2event(d[3])
    end
    nothing
end

function verify(est::EventStorage, e)
    (e.created_at < time()+300) || return false

    if !Nostr.verify(e)
        incr(est, :verifyerr)
        return false
    end
    
    return true
end

function dedup_message_log_filename(root_dir::String, created_at::Int)::String
    date = Dates.unix2datetime(created_at)
    dir = @sprintf "%s/%d/%02d" root_dir Dates.year(date) Dates.month(date)
    isdir(dir) || mkpath(dir)
    @sprintf "%s/%02d.log" dir Dates.day(date)
end

function import_msg_into_storage(msg::String, est::DeduplicatedEventStorage; force=false)
    import_msg_into_storage(msg, est.commons)

    if !force
        # fast event id extraction to skip further processing early
        eid = extract_event_id(msg)
        if !isnothing(eid) && !isempty(exe(est.deduped_events, @sql("select 1 from kv where event_id = ?1 limit 1"), 
                                           eid))
            incr(est, :seenearly)
            # exe(est.deduped_events, # this is considerably slowing down import
            #     @sql("update kv set times_seen = times_seen + 1 where event_id = ?1"), 
            #     eid)
            return false
        end
        incr(est, :seen)
    end

    e = try
        event_from_msg(JSON.parse(msg))
    catch _
        incr(est, :parseerr)
        rethrow()
    end

    e isa Nostr.Event || return false
    verify(est, e) || return false

    incr(est, :any)

    fn = dedup_message_log_filename(est.commons.directory * "/messages", e.created_at)
    fout = lock(est.files) do files
        get!(files, fn) do
            OpenedFile(open(fn, "a"), Ref(time()))
            # open(fn * ".inuse") do; end
        end
    end

    exe(est.deduped_events,
        @sql("insert into kv (event_id, created_at, processed_at, file_position, times_seen) values (?1, ?2, ?3, ?4, ?5)"), 
        e.id, e.created_at, trunc(Int, time()), position(fout.io), 1)

    println(fout.io, msg)
    flush(fout.io)
    fout.t_last_write[] = time()

    est.close_files_periodically() do
        lock(est.files) do files
            closed_files = String[]
            for (fn, f) in collect(files)
                if time()-fout.t_last_write[] >= est.max_open_file_age
                    close(f.io)
                    # rm(fn * ".inuse")
                    push!(closed_files, fn)
                end
            end
            for fn in closed_files
                delete!(est.files, fn)
            end
        end
    end

    lock(est.commons.message_processors) do message_processors
        for func in values(message_processors)
            catch_exception(est, func, msg) do
                Base.invokelatest(func, est, msg)
            end
        end
    end

    true
end

function prune(est::DeduplicatedEventStorage, keep_after::Int)
    exec(est.deduped_events, @sql("delete from kv where created_at < ?1"), (keep_after,))
end

fetch_requests = Dict() |> ThreadSafe
fetch_requests_periodic = Throttle(; period=2.0)
FETCH_REQUEST_DURATION = Ref(60)

function fetch(filter)
    lock(fetch_requests) do fetch_requests
        haskey(fetch_requests, filter) && return
        Fetching.fetch(filter)
        fetch_requests[filter] = time()
        fetch_requests_periodic() do
            flushed = Set()
            for (k, t) in collect(fetch_requests)
                time() - t > FETCH_REQUEST_DURATION[] && push!(flushed, k)
            end
            for k in flushed
                delete!(fetch_requests, k)
            end
        end
    end
end

function fetch_user_metadata(est::CacheStorage, pubkey::Nostr.PubKeyId)
    kinds = []
    pubkey in est.meta_data || push!(kinds, Int(Nostr.SET_METADATA))
    pubkey in est.contact_lists || push!(kinds, Int(Nostr.CONTACT_LIST))
    isempty(kinds) || fetch((; kinds, authors=[pubkey]))
end

function fetch_event(est::CacheStorage, eid::Nostr.EventId)
    eid in est.event_ids || fetch((; ids=[eid]))
end

function fetch_missing_events(est::CacheStorage, e::Nostr.Event)
    est.auto_fetch_missing_events && catch_exception(est, msg) do
        for tag in e.tags
            if length(tag.fields) >= 2
                if tag.fields[1] == "e"
                    if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                        fetch_event(est, eid)
                    end
                elseif tag.fields[1] == "p"
                    if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                        fetch_user_metadata(est, pk)
                    end
                end
            end
        end
    end
end

event_stats_fields = "event_id,
                      author_pubkey,
                      created_at,
                      likes, 
                      replies, 
                      mentions, 
                      reposts, 
                      zaps, 
                      satszapped, 
                      score, 
                      score24h"
event_stats_fields_cnt = length(split(event_stats_fields, ','))
event_stats_insert_q           = sql("insert into kv ($event_stats_fields) values (?1, ?2,  ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)")
event_stats_by_pubkey_insert_q = sql("insert into kv ($event_stats_fields) values (?2, ?1,  ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)")

already_imported_check_lock = ReentrantLock()
parameterized_replaceable_list_lock = ReentrantLock()

function import_msg_into_storage(msg::String, est::CacheStorage; force=false)
    import_msg_into_storage(msg, est.commons)

    lock(est.tidcnts) do tidcnts; tidcnts[Threads.threadid()] += 1; end

    e = try
        event_from_msg(JSON.parse(msg))
    catch _
        incr(est, :parseerr)
        rethrow()
    end

    e isa Nostr.Event || return false
    e.kind in kindints || return false
    est.verification_enabled && !verify(est, e) && return false

    should_import = lock(already_imported_check_lock) do
        if e.id in est.event_ids || !ext_preimport_check(est, e)
            false
        else
            push!(est.event_ids, e.id)
            true
        end
    end
    (force || should_import) || return false

    lock(est.event_processors) do event_processors
        for func in values(event_processors)
            catch_exception(est, func, e) do
                Base.invokelatest(func, e)
            end
        end
    end

    incr(est, :any)

    est.events[e.id] = e
    est.event_created_at[e.id] = e.created_at


    if !(e.pubkey in est.pubkey_ids)
        push!(est.pubkey_ids, e.pubkey)
        incr(est, :pubkeys)
        exe(est.pubkey_followers_cnt, @sql("insert or ignore into kv (key, value) values (?1,  ?2)"), e.pubkey, 0)
        est.auto_fetch_missing_events && fetch_user_metadata(est, e.pubkey)
        ext_pubkey(est, e)
    end


    ext_preimport(est, e)

    begin
        local is_pubkey_event = false
        local is_reply = false
        if e.kind == Int(Nostr.TEXT_NOTE)
            is_pubkey_event = true
            for tag in e.tags
                if length(tag.fields) >= 2 && tag.fields[1] == "e"
                    is_reply = !(length(tag.fields) >= 4 && tag.fields[4] == "mention")
                    break
                end
            end
        elseif e.kind == Int(Nostr.REPOST)
            is_pubkey_event = true
        end
        is_pubkey_event && exe(est.pubkey_events, @sql("insert into kv (pubkey, event_id, created_at, is_reply) values (?1, ?2, ?3, ?4)"),
                               e.pubkey, e.id, e.created_at, is_reply)
    end


    incr(est, :tags; by=length(e.tags))

    catch_exception(est, msg) do
        if     e.kind == Int(Nostr.SET_METADATA)
            track_user_stats(est, e.pubkey) do
                k = e.pubkey
                if !haskey(est.meta_data, k) || (e.created_at > est.events[est.meta_data[k]].created_at)
                    est.meta_data[k] = e.id
                    ext_metadata_changed(est, e)
                end
            end
        elseif e.kind == Int(Nostr.CONTACT_LIST)
            track_user_stats(est, e.pubkey) do
                if !haskey(est.contact_lists, e.pubkey) || (e.created_at > est.events[est.contact_lists[e.pubkey]].created_at)
                    old_follows = Set{Nostr.PubKeyId}()

                    if haskey(est.contact_lists, e.pubkey)
                        for tag in est.events[est.contact_lists[e.pubkey]].tags
                            if length(tag.fields) >= 2 && tag.fields[1] == "p"
                                follow_pubkey = try Nostr.PubKeyId(tag.fields[2]) catch _ continue end
                                push!(old_follows, follow_pubkey)
                            end
                        end
                    end

                    est.contact_lists[e.pubkey] = e.id

                    new_follows = Set{Nostr.PubKeyId}()
                    for tag in e.tags
                        if length(tag.fields) >= 2 && tag.fields[1] == "p"
                            follow_pubkey = try Nostr.PubKeyId(tag.fields[2]) catch _ continue end
                            push!(new_follows, follow_pubkey)
                        end
                    end

                    for follow_pubkey in new_follows
                        follow_pubkey in old_follows && continue
                        exe(est.pubkey_followers, @sql("insert into kv (pubkey, follower_pubkey, follower_contact_list_event_id) values (?1, ?2, ?3)"),
                            follow_pubkey, e.pubkey, e.id)
                        exe(est.pubkey_followers_cnt, @sql("update kv set value = value + 1 where key = ?1"),
                            follow_pubkey)
                        ext_new_follow(est, e, follow_pubkey)
                    end
                    for follow_pubkey in old_follows
                        follow_pubkey in new_follows && continue
                        exe(est.pubkey_followers, @sql("delete from kv where pubkey = ?1 and follower_pubkey = ?2 and follower_contact_list_event_id = ?3"),
                            follow_pubkey, e.pubkey, est.contact_lists[e.pubkey])
                        exe(est.pubkey_followers_cnt, @sql("update kv set value = value - 1 where key = ?1"),
                            follow_pubkey)
                        ext_user_unfollowed(est, e, follow_pubkey)
                    end
                end
            end
            # try
            #     for relay_url in collect(keys(JSON.parse(e.content)))
            #         register_relay(est, relay_url)
            #     end
            # catch _ end
        elseif e.kind == Int(Nostr.REACTION)
            incr(est, :reactions)
            for tag in e.tags
                if tag.fields[1] == "e"
                    if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                        c = e.content
                        if isempty(c) || c[1] in "🤙+❤️"
                            ext_is_hidden(est, e.id) || event_hook(est, eid, (:event_stats_cb, :likes, +1))
                            event_pubkey_action(est, eid, e, :liked)
                            ext_reaction(est, e, eid)
                        end
                    end
                end
            end

            fetch_missing_events(est, e)

        elseif e.kind == Int(Nostr.TEXT_NOTE)
            incr(est, :pubnotes)

            ext_text_note(est, e)

            for (tbl, q, key_vals) in [(est.event_stats,           event_stats_insert_q,           (e.id,     e.pubkey)),
                                       (est.event_stats_by_pubkey, event_stats_by_pubkey_insert_q, (e.pubkey,     e.id))]
                args = [key_vals..., e.created_at]
                append!(args, [0 for _ in 1:(event_stats_fields_cnt-length(args))])
                exe(tbl, q, args...)
            end

            parent_eid = nothing
            for tag in e.tags
                if length(tag.fields) >= 4
                    if tag.fields[1] == "e" && tag.fields[4] in ["root", "reply"]
                        if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                            parent_eid = eid
                        end
                    end
                end
            end
            if isnothing(parent_eid)
                for tag in e.tags
                    if length(tag.fields) >= 2
                        if tag.fields[1] == "e"
                            if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                                parent_eid = eid
                            end
                        end
                    end
                end
            end
            if !isnothing(parent_eid)
                incr(est, :replies)
                ext_is_hidden(est, e.id) || event_hook(est, parent_eid, (:event_stats_cb, :replies, +1))
                exe(est.event_replies, @sql("insert into kv (event_id, reply_event_id, reply_created_at) values (?1, ?2, ?3)"),
                    parent_eid, e.id, e.created_at)
                event_pubkey_action(est, parent_eid, e, :replied)
                est.event_thread_parents[e.id] = parent_eid
                ext_reply(est, e, parent_eid)
            end

            fetch_missing_events(est, e)

        elseif e.kind == Int(Nostr.DIRECT_MESSAGE)
            import_directmsg(est, e)

        elseif e.kind == Int(Nostr.EVENT_DELETION)
            for tag in e.tags
                if length(tag.fields) >= 2 && tag.fields[1] == "e"
                    if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                        if eid in est.events
                            de = est.events[eid]
                            if de.pubkey == e.pubkey
                                incr(est, :eventdeletions)
                                est.deleted_events[eid] = e.id
                                ext_event_deletion(est, e, eid)
                            end
                        end
                    end
                end
            end
        elseif e.kind == Int(Nostr.REPOST)
            incr(est, :reposts)
            for tag in e.tags
                if tag.fields[1] == "e"
                    if !isnothing(local eid = try Nostr.EventId(tag.fields[2]) catch _ end)
                        ext_is_hidden(est, e.id) || event_hook(est, eid, (:event_stats_cb, :reposts, +1))
                        event_pubkey_action(est, eid, e, :reposted)
                        ext_repost(est, e, eid)
                        break
                    end
                end
            end

            fetch_missing_events(est, e)

        elseif e.kind == Int(Nostr.ZAP_RECEIPT)
            # TODO zap auth
            amount_sats = 0
            parent_eid = nothing
            zapped_pk = nothing
            description = nothing
            for tag in e.tags
                if length(tag.fields) >= 2
                    if tag.fields[1] == "e"
                        parent_eid = try Nostr.EventId(tag.fields[2]) catch _ end
                    elseif tag.fields[1] == "p"
                        zapped_pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end
                    elseif tag.fields[1] == "bolt11"
                        b = tag.fields[2]
                        if !isnothing(local amount = parse_bolt11(b))
                            if amount <= MAX_SATSZAPPED[]
                                amount_sats = amount
                            end
                        end
                    elseif tag.fields[1] == "description"
                        description = try JSON.parse(tag.fields[2]) catch _ nothing end
                    end
                end
            end
            if amount_sats > 0 && !isnothing(description)
                incr(est, :zaps)
                incr(est, :satszapped; by=amount_sats)
                if !isnothing(parent_eid)
                    event_hook(est, parent_eid, (:event_stats_cb, :zaps, +1))
                    event_pubkey_action(est, parent_eid, 
                                        Nostr.Event(e.id, zap_sender(e), e.created_at, e.kind, 
                                                    e.tags, e.content, e.sig),
                                        :zapped)
                    ext_zap(est, e, parent_eid, amount_sats)
                end
                if !isnothing(zapped_pk)
                    ext_pubkey_zap(est, e, zapped_pk, amount_sats)
                end
            end
        elseif e.kind == Int(Nostr.MUTE_LIST)
            est.mute_list[e.pubkey] = e.id
        elseif e.kind == Int(Nostr.CATEGORIZED_PEOPLE)
            for tag in e.tags
                if length(tag.fields) >= 2
                    if tag.fields[1] == "d" && tag.fields[2] == "mute"
                        est.mute_list_2[e.pubkey] = e.id
                        break
                    elseif tag.fields[1] == "d" && tag.fields[2] == "mutelists"
                        est.mute_lists[e.pubkey] = e.id
                        break
                    elseif tag.fields[1] == "d" && tag.fields[2] == "allowlist"
                        est.allow_list[e.pubkey] = e.id
                        break
                    else tag.fields[1] == "d"
                        identifier = tag.fields[2]
                        lock(parameterized_replaceable_list_lock) do
                            DB.exe(est.parameterized_replaceable_list, @sql("delete from parameterized_replaceable_list where pubkey = ?1 and identifier = ?2"), e.pubkey, identifier)
                            DB.exe(est.parameterized_replaceable_list, @sql("insert into parameterized_replaceable_list values (?1,?2,?3,?4)"), e.pubkey, identifier, e.created_at, e.id)
                        end
                        break
                    end
                end
            end
        end
    end

    for (funcall,) in exe(est.event_hooks, @sql("select funcall from kv where event_id = ?1"), e.id)
        event_hook_execute(est, e, Tuple(JSON.parse(funcall)))
    end
    exe(est.event_hooks, @sql("delete from kv where event_id = ?1"), e.id)

    return true
end

function import_directmsg(est::CacheStorage, e::Nostr.Event)
    incr(est, :directmsgs)
    for tag in e.tags
        if length(tag.fields) >= 2 && tag.fields[1] == "p"
            if !isnothing(local receiver = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                hidden = ext_is_hidden(est, e.pubkey) || try Base.invokelatest(Main.App.is_hidden, est, receiver, :content, e.pubkey) catch _ false end
                # @show (receiver, e.pubkey, hidden)
                if !hidden
                    lock(est.pubkey_directmsgs_cnt_lock) do
                        isempty(exe(est.pubkey_directmsgs, @sql("select 1 from pubkey_directmsgs indexed by pubkey_directmsgs_receiver_event_id where receiver = ?1 and event_id = ?2 limit 1"), receiver, e.id)) &&
                        exe(est.pubkey_directmsgs, @sql("insert into pubkey_directmsgs values (?1, ?2, ?3, ?4)"), receiver, e.pubkey, e.created_at, e.id)

                        for sender in [nothing, e.pubkey]
                            if isempty(exe(est.pubkey_directmsgs_cnt, @sql("select 1 from pubkey_directmsgs_cnt indexed by pubkey_directmsgs_cnt_receiver_sender where receiver is ?1 and sender is ?2 limit 1"), receiver, sender))
                                exe(est.pubkey_directmsgs_cnt, @sql("insert into pubkey_directmsgs_cnt values (?1, ?2, ?3, ?4, ?5)"), receiver, sender, 0, e.created_at, e.id)
                            end
                            exe(est.pubkey_directmsgs_cnt, @sql("update pubkey_directmsgs_cnt indexed by pubkey_directmsgs_cnt_receiver_sender set cnt = cnt + ?3 where receiver is ?1 and sender is ?2"), receiver, sender, +1)
                            prev_latest_at = exe(est.pubkey_directmsgs_cnt, @sql("select latest_at from pubkey_directmsgs_cnt indexed by pubkey_directmsgs_cnt_receiver_sender where receiver is ?1 and sender is ?2 limit 1"), receiver, sender)[1][1]
                            if e.created_at >= prev_latest_at
                                exe(est.pubkey_directmsgs_cnt, @sql("update pubkey_directmsgs_cnt indexed by pubkey_directmsgs_cnt_receiver_sender set latest_at = ?3, latest_event_id = ?4 where receiver is ?1 and sender is ?2"), receiver, sender, e.created_at, e.id)
                            end
                        end
                    end
                end

                break
            end
        end
    end
end

function run_scheduled_hooks(est::CacheStorage)
    for (rowid, funcall) in exec(est.scheduled_hooks, @sql("select rowid, funcall from kv where execute_at <= ?1"), (trunc(Int, time()),))
        scheduled_hook_execute(est, Tuple(JSON.parse(funcall)))
        exec(est.scheduled_hooks, @sql("delete from kv where rowid = ?1"), (rowid,))
    end
end

function prune(deduped_storage::DeduplicatedEventStorage, cache_storage::CacheStorage, keep_after::Int)
    # WIP
    @threads for dbconn in deduped_storage.events.dbconns
        eids = [Nostr.EventId(eid) for eid in exe(dbconn, @sql("select event_id from kv where created_at < ?1"), (keep_after,))]
        for eid in eids
            delete!(cache_storage.event_ids, eid)
            delete!(cache_storage.events, eid)
            exec(cache_storage.pubkey_followers, @sql("delete from kv where follower_contact_list_event_id = ?1"), (eid,))
            exec(cache_storage.event_hooks, @sql("delete from kv where event_id = ?1"), (eid,))
        end

        # cache_storage.pubkey_ids?
        #
        for c in [cache_storage.event_stats, cache_storage.event_stats_by_pubkey]
            exec(c, @sql("delete from kv where created_at < ?1"), (keep_after,))
        end
        exec(cache_storage.event_replies, @sql("delete from kv where reply_created_at < ?1"), (keep_after,))

        # cache_storage.contact_lists ?
        # cache_storage.meta_data ?

        # cache_storage.event_thread_parents ?
    end
end

function dump_to_directory(ssd::ShardedDBDict{K, V}, target_dir) where {K, V}
    for (i, dbconn) in enumerate(ssd.dbconns)
        lock(dbconn) do dbconn
            dbfn = @sprintf "%s/%02x.sqlite" target_dir i
            println("writing to ", dbfn)
            isfile(dbfn) && rm(dbfn)
            exe(dbconn, "vacuum into '$dbfn'")
            sleep(0.1)
        end
    end
end

function dump_to_directory(est::EventStorage, target_dir)
    # TBD
    for a in propertynames(est)
        v = getproperty(est, a)
        if v isa ShardedSqliteDict
            println("dumping $a")
            #dump_to_directory(v, target_dir)
        end
    end
end

function vacuum(est::EventStorage)
    for a in propertynames(est)
        v = getproperty(est, a)
        if v isa ShardedSqliteDict
            println("vacuuming $a")
            for dbconn in v.dbconns
                exe(dbconn, "vacuum")
            end
        end
    end
end

function vacuum_some_tables(est::CacheStorage)
    for ssd in [
                est.event_hooks,
                est.meta_data,
                est.contact_lists,
               ]
        @time for dbconn in ssd.dbconns
            DB.exe(dbconn, "vacuum")
        end
    end
end

print_exceptions_lock = ReentrantLock()
chunked_reading_lock = ReentrantLock()

function print_exceptions()
    lock(print_exceptions_lock) do
        @show Threads.threadid()
        Utils.print_exceptions()
    end
end

function init(commons::StorageCommons, running)
    isdir(commons.directory) || mkpath(commons.directory)

    for prop in stat_names
        haskey(commons.stats, prop) || (commons.stats[prop] = 0)
    end
    load_stats(commons)

    commons.tstart[] = time()

    commons.gc_task[] = GCTask(; period=15)
    nothing
end

function init(est::DeduplicatedEventStorage, running=Ref(true))
    init(est.commons, running)
end

function init(est::CacheStorage, running=Ref(true))
    init(est.commons, running)

    ext_init(est)

    est.periodic_task_running[] = true
    est.periodic_task[] = errormonitor(@async while est.periodic_task_running[]
                                           catch_exception(est, :periodic) do
                                               periodic(est)
                                           end
                                           Utils.active_sleep(60.0, est.periodic_task_running)
                                       end)
end

function periodic(est::CacheStorage)
    run_scheduled_hooks(est)
end

function import_to_storage(
        est::EventStorage, src_dirs::Vector{String};
        running=Ref(true)|>ThreadSafe, files_newer_than::Real=0,
        max_threads=Threads.nthreads())
    init(est, running)

    empty!(threadprogress)

    fns = []
    for src_dir in src_dirs
        for (dir, _, fs) in walkdir(src_dir)
            for fn in fs
                endswith(fn, ".log") || continue
                ffn = "$dir/$fn"
                !isnothing(files_newer_than) && stat(ffn).mtime < files_newer_than && continue
                push!(fns, ffn)
            end
        end
    end
    fns = sort(fns)

    fnpos = []
    chunksize = 100_000_000
    for fn in fns
        last_pos = get(est.commons.latest_file_positions, fn, 0)
        for p in 0:chunksize:filesize(fn)
            end_pos = min(p+chunksize, filesize(fn))
            end_pos > last_pos && push!(fnpos, (; fn, start_pos=p, end_pos))
        end
    end

    fnpos = sort(fnpos, by=fnp->fnp.start_pos)

    thrcnt = Ref(0) |> ThreadSafe
    fnposcnt = Ref(0) |> ThreadSafe

    @time @sync for (fnidx, fnp) in collect(enumerate(fnpos))
        while running[] && thrcnt[] >= max_threads; sleep(0.05); end
        running[] || break
        incr(thrcnt)
        Threads.@spawn begin
            catch_exception(est, fnp) do
                # running[] || continue
                try
                    file_pos = max(fnp.start_pos - 2*MAX_MESSAGE_SIZE[], 0)
                    open(fnp.fn) do flog
                        seek(flog, file_pos); file_pos > 0 && readline(flog)
                        msgcnt_ = Ref(0)
                        chunk_done = false
                        while running[]
                            (chunk_done = (eof(flog) || position(flog) >= fnp.end_pos)) && break

                            SHOW_PROGRESS[] && (msgcnt_[] % 1000 == 0) && term_progress((@sprintf "[%5d] %s" fnidx fnp.fn), msgcnt_[], est.commons.stats, "$(fnposcnt[])/$(length(fnpos))", est.commons.tstart[])
                            msgcnt_[] += 1

                            msg = readline(flog)
                            # isempty(msg) && break

                            length(msg) > MAX_MESSAGE_SIZE[] && continue

                            catch_exception(est, msg) do
                                import_msg_into_storage(msg, est)
                            end
                        end
                        incr(est, :msg; by=msgcnt_[])
                        chunk_done && lock(est.commons.latest_file_positions) do latest_file_positions
                            latest_file_positions[fnp.fn] = max(position(flog), get(latest_file_positions, fnp.fn, 0))
                        end
                    end
                finally
                    SHOW_PROGRESS[] && term_progress(fnp.fn, PROGRESS_COMPLETE, est.commons.stats, "$(fnposcnt[])/$(length(fnpos))", est.commons.tstart[])
                    incr(fnposcnt)
                end
            end
            decr(thrcnt)
        end
    end
    while thrcnt[] > 0; sleep(0.1); end

    save_stats(est.commons)

    complete(est)

    report_result(est)
end

function extract_event_id(msg::String)
    pat = "\"id\":\""
    r1 = findnext(pat, msg, 1)
    if !isnothing(r1)
        r2 = findnext(pat, msg, r1[end]+1)
        if isnothing(r2) # check if we see only one id key
            eid = try
                eidhex = msg[r1[end]+1:findnext("\"", msg, r1[end]+1)[1]-1]
                Nostr.EventId(eidhex)
            catch _ end
            if !isnothing(eid)
                return eid
            end
        end
    end
    nothing
end

function complete(commons::StorageCommons)
    stop(commons.gc_task[])
end

function complete(est::DeduplicatedEventStorage)
    complete(est.commons)

    lock(est.files) do files
        for (fn, f) in collect(files)
            close(f.io)
        end
        empty!(files)
    end
end

function complete(est::CacheStorage)
    complete(est.commons)

    ext_complete(est)
end

function report_result(commons::StorageCommons)
    println("#msgbytes: $(sum(values(commons.latest_file_positions))/(1024^3)) GB")
    println("stats:")
    for (k, v) in sort(collect(pairs(getstats(commons.stats))))
        println(@sprintf "  %20s => %d" k v)
    end
end
@forward EventStorage.commons report_result
function report_result(est::CacheStorage)
    report_result(est.commons)
    println("dicts:")
    println(barplot(map(string, [Nostr.SET_METADATA, Nostr.CONTACT_LIST]), map(length, [est.meta_data, est.contact_lists])))
end

function getstats(stats)
    lock(stats) do stats
        (; (prop => get(stats, prop, 0) for prop in stat_names)...)
    end
end

function mon(est::EventStorage)
    local running = Ref(true)
    tsk = @async begin; readline(); running[] = false; println("stopping"); end
    local pc = nothing
    while running[]
        c = get(est.commons.stats, :any, 0)
        isnothing(pc) && (pc = c)
        print("  ", c-pc, " msg/s         \r")
        pc = c
        sleep(1)
    end
end

function ext_preimport_check(est::CacheStorage, e) true end
function ext_pubkey(est::CacheStorage, e) end
function ext_preimport(est::CacheStorage, e) end
function ext_metadata_changed(est::CacheStorage, e) end
function ext_new_follow(est::CacheStorage, e, follow_pubkey) end
function ext_user_unfollowed(est::CacheStorage, e, follow_pubkey) end
function ext_reaction(est::CacheStorage, e, eid) end
function ext_text_note(est::CacheStorage, e) end
function ext_reply(est::CacheStorage, e, parent_eid) end
function ext_event_deletion(est::CacheStorage, e, eid) end
function ext_repost(est::CacheStorage, e, eid) end
function ext_zap(est::CacheStorage, e, parent_eid, amount_sats) end
function ext_pubkey_zap(est::CacheStorage, e, zapped_pk, amount_sats) end
function ext_init(est::CacheStorage) end
function ext_complete(est::CacheStorage) end
function ext_is_hidden(est::CacheStorage, eid) false end
function ext_is_human(est::CacheStorage, pubkey) true end

