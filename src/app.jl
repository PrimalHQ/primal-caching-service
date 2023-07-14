module App

import JSON
using .Threads: @threads
using DataStructures: OrderedSet

import ..DB
import ..Nostr
using ..Utils: ThreadSafe

exposed_functions = Set([:feed,
                         :thread_view,
                         :network_stats,
                         :user_infos,
                         :events,
                         :user_profile,
                         :get_directmsg_contacts,
                         :reset_directmsg_count,
                         :reset_directmsg_counts,
                         :get_directmsgs,
                        ])

EVENT_STATS=10_000_100
NET_STATS=10_000_101
USER_PROFILE=10_000_105
REFERENCED_EVENT=10_000_107
RANGE=10_000_113
EVENT_ACTIONS=10_000_115
DIRECTMSG_COUNT=10_000_117
DIRECTMSG_COUNTS=10_000_118

cast(value, type) = value isa type ? value : type(value)
castmaybe(value, type) = isnothing(value) ? value : cast(value, type)

function range(res::Vector, order_by)
    if isempty(res)
        [(; kind=Int(RANGE), content=JSON.json((; order_by)))]
    else
        [(; kind=Int(RANGE), content=JSON.json((; since=res[end][2], until=res[1][2], order_by)))]
    end
end

function follows(est::DB.CacheStorage, pubkey::Nostr.PubKeyId)::Vector{Nostr.PubKeyId}
    if pubkey in est.contact_lists 
        res = Nostr.PubKeyId[]
        for t in est.events[est.contact_lists[pubkey]].tags
            if length(t.fields) >= 2 && t.fields[1] == "p"
                try push!(res, Nostr.PubKeyId(t.fields[2])) catch _ end
            end
        end
        res
    else
        []
    end
end

function event_stats(est::DB.CacheStorage, eid::Nostr.EventId)
    r = DB.exe(est.event_stats, DB.@sql("select likes, replies, mentions, reposts, zaps, satszapped, score, score24h from kv where event_id = ?"), eid)
    if isempty(r)
        @debug "event_stats: ignoring missing event $(eid)"
        []
    else
        es = zip([:likes, :replies, :mentions, :reposts, :zaps, :satszapped, :score, :score24h], r[1])
        [(; 
             kind=Int(EVENT_STATS),
             content=JSON.json((; event_id=eid, es...)))]
    end
end

function event_actions(est::DB.CacheStorage, eid::Nostr.EventId, user_pubkey::Nostr.PubKeyId)
    r = DB.exe(est.event_pubkey_actions, DB.@sql("select replied, liked, reposted, zapped from kv where event_id = ?1 and pubkey = ?2"), eid, user_pubkey)
    if isempty(r)
        []
    else
        ea = zip([:replied, :liked, :reposted, :zapped], map(Bool, r[1]))
        [(; 
          kind=Int(EVENT_ACTIONS),
          content=JSON.json((; event_id=eid, ea...)))]
    end
end

function response_messages_for_posts(
        est::DB.CacheStorage, eids::Vector{Nostr.EventId}; 
        res_meta_data=Dict(), user_pubkey=nothing,
    )
    res = OrderedSet() |> ThreadSafe

    pks = Set{Nostr.PubKeyId}() |> ThreadSafe
    res_meta_data = res_meta_data |> ThreadSafe

    mute_list = Set{Nostr.PubKeyId}()
    if !isnothing(user_pubkey) && user_pubkey in est.mute_list
        for tag in est.events[est.mute_list[user_pubkey]].tags
            if length(tag.fields) >= 2 && tag.fields[1] == "p"
                if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                    push!(mute_list, pk)
                end
            end
        end
    end

    function handle_event(body::Function, eid::Nostr.EventId; wrapfun::Function=identity)
        ext_is_hidden(est, eid) && return
        eid in est.deleted_events && return

        e = est.events[eid]
        e.pubkey in mute_list && return
        ext_is_hidden(est, e.pubkey) && return
        push!(res, wrapfun(e))
        union!(res, event_stats(est, e.id))
        isnothing(user_pubkey) || union!(res, event_actions(est, e.id, user_pubkey))
        push!(pks, e.pubkey)
        union!(res, ext_event_response(est, e))

        extra_tags = Nostr.Tag[]
        DB.for_mentiones(est, e) do tag
            push!(extra_tags, tag)
        end
        all_tags = vcat(e.tags, extra_tags)
        # @show length(all_tags)
        for tag in all_tags
            tag = tag.fields
            try
                if length(tag) >= 2
                    if tag[1] == "e"
                        body(Nostr.EventId(tag[2]))
                    elseif tag[1] == "p"
                        push!(pks, Nostr.PubKeyId(tag[2]))
                    end
                end
            catch _ end
        end
    end

    for eid in eids
        handle_event(eid) do subeid
            handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e))) do subeid
                handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e))) do _
                end
            end
        end

        ## if e.kind == Int(Nostr.REPOST)
        ##     try
        ##         ee = Nostr.Event(JSON.parse(e.content))
        ##         union!(res, event_stats(est, ee.id))
        ##         push!(pks, ee.pubkey)
        ##     catch _ end
        ## end
    end

    for pk in pks.wrapped
        if !haskey(res_meta_data, pk) && pk in est.meta_data
            res_meta_data[pk] = est.events[est.meta_data[pk]]
        end
    end

    res = collect(res)

    for md in values(res_meta_data)
        push!(res, md)
        union!(res, ext_event_response(est, md))
    end

    res
end

function feed(
        est::DB.CacheStorage;
        pubkey=nothing, notes::Union{Symbol,String}=:follows, include_replies=false,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
    )
    limit <= 1000 || error("limit too big")
    notes = Symbol(notes)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    posts = [] |> ThreadSafe
    if pubkey isa Nothing
        # @threads for dbconn in est.pubkey_events
        #     append!(posts, map(Tuple, DB.exe(dbconn, DB.@sql("select event_id, created_at from kv 
        #                                                       where created_at >= ? and created_at <= ? and (is_reply = 0 or is_reply = ?)
        #                                                       order by created_at desc limit ? offset ?"),
        #                                      (since, until, Int(include_replies), limit, offset))))
        # end
    else
        pubkeys = 
        if     notes == :follows;  follows(est, pubkey)
        elseif notes == :authored; [pubkey]
        else;                      error("unsupported type of notes")
        end
        @threads for p in pubkeys
            append!(posts, map(Tuple, DB.exe(est.pubkey_events, DB.@sql("select event_id, created_at from kv 
                                                                        where pubkey = ? and created_at >= ? and created_at <= ? and (is_reply = 0 or is_reply = ?)
                                                                        order by created_at desc limit ? offset ?"),
                                             p, since, until, Int(include_replies), limit, offset)))
        end
    end
    posts = sort(posts.wrapped, by=p->-p[2])[1:min(limit, length(posts))]

    eids = [Nostr.EventId(eid) for (eid, _) in posts]
    res = response_messages_for_posts(est, eids; user_pubkey)

    vcat(res, range(posts, :created_at))
end

function thread_view(est::DB.CacheStorage; event_id, user_pubkey=nothing, kwargs...)
    event_id = cast(event_id, Nostr.EventId)

    est.auto_fetch_missing_events && DB.fetch_event(est, event_id)

    res = []

    hidden = ext_is_hidden(est, event_id) || event_id in est.deleted_events

    hidden || append!(res, thread_view_replies(est; event_id, user_pubkey, kwargs...))
    append!(res, thread_view_parents(est; event_id, user_pubkey))

    res
end

function thread_view_replies(est::DB.CacheStorage;
        event_id,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
    )
    limit <= 1000 || error("limit too big")
    event_id = cast(event_id, Nostr.EventId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    posts = Tuple{Nostr.EventId, Int}[]
    for (reid, created_at) in DB.exe(est.event_replies, DB.@sql("select reply_event_id, reply_created_at from kv
                                                                 where event_id = ? and reply_created_at >= ? and reply_created_at <= ?
                                                                 order by reply_created_at desc limit ? offset ?"),
                                     event_id, since, until, limit, offset)
        push!(posts, (Nostr.EventId(reid), created_at))
    end
    posts = sort(posts, by=p->-p[2])[1:min(limit, length(posts))]
    
    reids = [reid for (reid, _) in posts]
    response_messages_for_posts(est, reids; user_pubkey)
end

function thread_view_parents(est::DB.CacheStorage; event_id, user_pubkey=nothing)
    event_id = cast(event_id, Nostr.EventId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    posts = Tuple{Nostr.EventId, Int}[]
    peid = event_id
    while true
        if peid in est.events
            push!(posts, (peid, est.events[peid].created_at))
        else
            @debug "missing thread parent event $peid not found in storage"
        end
        if peid in est.event_thread_parents
            peid = est.event_thread_parents[peid]
        else
            break
        end
    end

    posts = sort(posts, by=p->p[2])

    reids = [reid for (reid, _) in posts]
    response_messages_for_posts(est, reids; user_pubkey)
end

function network_stats(est::DB.CacheStorage)
    lock(est.commons.stats) do stats
        (;
         kind=Int(NET_STATS),
         content=JSON.json((;
                            [k => get(stats, k, 0)
                             for k in [:users,
                                       :pubkeys,
                                       :pubnotes,
                                       :reactions,
                                       :reposts,
                                       :any,
                                       :zaps,
                                       :satszapped,
                                      ]]...)))
    end
end

function contact_list(est::DB.CacheStorage; pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    haskey(est.contact_lists, pubkey) ? [est.events[est.contact_lists[pubkey]]] : []
end

function user_infos(est::DB.CacheStorage; pubkeys::Vector)
    pubkeys = [pk isa Nostr.PubKeyId ? pk : Nostr.PubKeyId(pk) for pk in pubkeys]

    res_meta_data = Dict() |> ThreadSafe
    @threads for pk in pubkeys 
        if pk in est.meta_data
            res_meta_data[pk] = est.events[est.meta_data[pk]]
        end
    end
    res = []
    append!(res, collect(values(res_meta_data)))
    ext_user_infos(est, res, collect(values(res_meta_data)))
    res
end

function events(
        est::DB.CacheStorage; 
        event_ids::Vector, extended_response::Bool=false, user_pubkey=nothing,
    )
    event_ids = [eid isa Nostr.EventId ? eid : Nostr.EventId(eid) for eid in event_ids]
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    if !extended_response
        res = [] |> ThreadSafe
        @threads for eid in event_ids 
            eid in est.events && push!(res, est.events[eid])
        end
        res.wrapped
    else
        response_messages_for_posts(est, event_ids; user_pubkey)
    end
end

function user_profile(est::DB.CacheStorage; pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    est.auto_fetch_missing_events && DB.fetch_user_metadata(est, pubkey)

    res = [] |> ThreadSafe

    pubkey in est.meta_data && push!(res, est.events[est.meta_data[pubkey]])

    note_count = DB.exe(est.pubkey_events, DB.@sql("select count(1) from kv where pubkey = ?"), pubkey)[1][1]

    time_joined_r = DB.exe(est.pubkey_events, DB.@sql("select created_at from kv
                                                       where pubkey = ?
                                                       order by created_at asc limit 1"), pubkey)

    time_joined = isempty(time_joined_r) ? nothing : time_joined_r[1][1];

    push!(res, (;
                kind=Int(USER_PROFILE),
                pubkey,
                content=JSON.json((;
                                   follows_count=length(follows(est, pubkey)),
                                   followers_count=get(est.pubkey_followers_cnt, pubkey, 0),
                                   note_count,
                                   time_joined,
                                  ))))
    res.wrapped
end

function get_directmsg_count(est::DB.CacheStorage; receiver, sender=nothing)
    receiver = cast(receiver, Nostr.PubKeyId)
    sender = castmaybe(sender, Nostr.PubKeyId)
    cnt = 0
    for (c,) in DB.exe(est.pubkey_directmsgs_cnt,
                       DB.@sql("select cnt from pubkey_directmsgs_cnt
                                indexed by pubkey_directmsgs_cnt_receiver_sender
                                where receiver is ?1 and sender is ?2 limit 1"),
                       receiver, sender)
        cnt = c
        break
    end
    [(; kind=Int(DIRECTMSG_COUNT), cnt)]
end

function get_directmsg_contacts(
        est::DB.CacheStorage; 
        user_pubkey, relation::Union{String,Symbol}=:any
    )
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    relation = cast(relation, Symbol)

    fs = Set(follows(est, user_pubkey))

    d = Dict()
    evts = []
    mds = []
    mdextra = []
    for (peer, cnt, latest_at, latest_event_id) in 
        vcat(DB.exe(est.pubkey_directmsgs_cnt,
                    DB.@sql("select sender, cnt, latest_at, latest_event_id
                            from pubkey_directmsgs_cnt
                            where receiver is ?1 and sender is not null
                            order by latest_at desc"), user_pubkey),
             DB.exe(est.pubkey_directmsgs_cnt,
                    DB.@sql("select receiver, 0, latest_at, latest_event_id
                            from pubkey_directmsgs_cnt
                            where sender is ?1
                            order by latest_at desc"), user_pubkey))

        peer = Nostr.PubKeyId(peer)
        if relation != :any
            if relation == :follows; peer in fs || continue
            elseif relation == :other; peer in fs && continue
            else; error("invalid relation")
            end
        end
        
        latest_event_id = Nostr.EventId(latest_event_id)
        k = Nostr.hex(peer) 
        if !haskey(d, k)
            d[k] = Dict([:cnt=>cnt, :latest_at=>latest_at, :latest_event_id=>latest_event_id])
        end
        if d[k][:latest_at] < latest_at
            d[k][:latest_at] = latest_at
            d[k][:latest_event_id] = latest_event_id
        end
        if d[k][:cnt] < cnt
            d[k][:cnt] = cnt
        end
        if latest_event_id in est.events
            push!(evts, est.events[latest_event_id])
        end
        if peer in est.meta_data
            mdeid = est.meta_data[peer]
            if mdeid in est.events
                md = est.events[mdeid]
                push!(mds, md)
                union!(mdextra, ext_event_response(est, md))
            end
        end
    end
    [(; kind=Int(DIRECTMSG_COUNTS), content=JSON.json(d)), evts..., mds..., mdextra...]
end

reset_directmsg_count_lock = ReentrantLock()

function reset_directmsg_count(est::DB.CacheStorage; event_from_user::Dict, sender, replicated=false)
    DB.PG_DISABLE[] && return []

    replicated || replicate_request(:reset_directmsg_count; event_from_user, sender)

    e = Nostr.Event(event_from_user)
    e.created_at > time() - 300 || error("event is too old")
    Nostr.verify(e) || error("verification failed")

    receiver = e.pubkey
    sender = cast(sender, Nostr.PubKeyId)

    lock(reset_directmsg_count_lock) do
        r = DB.exe(est.pubkey_directmsgs_cnt,
                   DB.@sql("select cnt from pubkey_directmsgs_cnt 
                           indexed by pubkey_directmsgs_cnt_receiver_sender
                           where receiver is ?1 and sender is ?2"),
                   receiver, sender)
        if !isempty(r)
            for s in [sender, nothing]
                DB.exe(est.pubkey_directmsgs_cnt,
                       DB.@sql("update pubkey_directmsgs_cnt 
                               indexed by pubkey_directmsgs_cnt_receiver_sender
                               set cnt = max(0, cnt - ?3)
                               where receiver is ?1 and sender is ?2"),
                       receiver, s, r[1][1])
            end
        end
    end
    []
end

function reset_directmsg_counts(est::DB.CacheStorage; event_from_user::Dict, replicated=false)
    DB.PG_DISABLE[] && return []

    replicated || replicate_request(:reset_directmsg_counts; event_from_user)

    e = Nostr.Event(event_from_user)
    e.created_at > time() - 300 || error("event is too old")
    Nostr.verify(e) || error("verification failed")

    receiver = e.pubkey

    lock(reset_directmsg_count_lock) do
        DB.exe(est.pubkey_directmsgs_cnt,
               DB.@sql("update pubkey_directmsgs_cnt 
                       set cnt = 0
                       where receiver is ?1"),
               receiver)
    end
    []
end

function get_directmsgs(
        est::DB.CacheStorage; 
        receiver, sender, 
        since::Int=0, until::Int=trunc(Int, time()), limit::Int=20, offset::Int=0
    )
    receiver = cast(receiver, Nostr.PubKeyId)
    sender = cast(sender, Nostr.PubKeyId)
    msgs = []
    for (eid, created_at) in DB.exe(est.pubkey_directmsgs,
                                    DB.@sql("select event_id, created_at from pubkey_directmsgs where 
                                            ((receiver is ?1 and sender is ?2) or (receiver is ?2 and sender is ?1)) and
                                            created_at >= ?3 and created_at <= ?4 
                                            order by created_at desc limit ?5 offset ?6"),
                                    receiver, sender, since, until, limit, offset)
        push!(msgs, (est.events[Nostr.EventId(eid)], created_at))
    end
    vcat([e for (e, _) in msgs], range(msgs, :created_at))
end

REPLICATE_TO_SERVERS = []

function replicate_request(reqname::Union{String, Symbol}; kwargs...)
    msg = JSON.json(["REQ", "replicated_request", (; cache=[reqname, (; kwargs..., replicated=true)])])
    for (addr, port) in REPLICATE_TO_SERVERS
        errormonitor(@async HTTP.WebSockets.open("ws://$addr:$port"; connect_timeout=2, readtimeout=2) do ws
            HTTP.WebSockets.send(ws, msg)
            # println("replicated: ", msg)
        end)
    end
end

function ext_user_infos(est::DB.CacheStorage, res, res_meta_data) end
function ext_is_hidden(est::DB.CacheStorage, eid::Nostr.EventId); false; end
function ext_event_response(est::DB.CacheStorage, e::Nostr.Event); []; end

end
