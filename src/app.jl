module App

import JSON
using .Threads: @threads

import ..DB
import ..Nostr
using ..Utils: ThreadSafe

exposed_functions = Set([:feed,
                         :thread_view,
                         :thread_view_replies,
                         :thread_view_parents,
                         :network_stats,
                         :user_infos,
                         :events,
                         :user_profile,
                        ])

EVENT_STATS=10_000_100
NET_STATS=10_000_101
USER_PROFILE=10_000_105
REFERENCED_EVENT=10_000_107

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

function push_event_stats(est::DB.CacheStorage, res, e::Nostr.Event)
    r = DB.exe(est.event_stats, DB.@sql("select likes, replies, mentions, reposts, zaps, satszapped, score, score24h from kv where event_id = ?"), e.id)
    if isempty(r); @debug "push_event_stats: ignoring missing event $(e.id)"; return; end
    es = zip([:likes, :replies, :mentions, :reposts, :zaps, :satszapped, :score, :score24h], r[1])
    d = (; 
         kind=Int(EVENT_STATS),
         content=JSON.json((; event_id=e.id, es...)))
    push!(res, d)
end

function response_messages_for_posts(est::DB.CacheStorage, eids::Vector{Nostr.EventId}; res_meta_data=Dict())
    res = [] |> ThreadSafe

    pks = Set{Nostr.PubKeyId}() |> ThreadSafe
    res_meta_data = res_meta_data |> ThreadSafe

    function handle_event(body::Function, eid::Nostr.EventId; wrapfun::Function=identity)
        e = est.events[eid]
        push!(res, wrapfun(e))
        push_event_stats(est, res, e)
        push!(pks, e.pubkey)

        extra_tags = Nostr.Tag[]
        DB.for_mentiones(est, e) do tag
            push!(extra_tags, tag)
        end
        for tag in vcat(e.tags, extra_tags)
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

    @threads for eid in eids
        handle_event(eid) do subeid
            handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e))) do subeid
                handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e))) do _
                end
            end
        end

        ## if e.kind == Int(Nostr.REPOST)
        ##     try
        ##         ee = Nostr.dict2event(JSON.parse(e.content))
        ##         push_event_stats(est, res, ee)
        ##         push!(pks, ee.pubkey)
        ##     catch _ end
        ## end
    end

    for pk in pks.wrapped
        if !haskey(res_meta_data, pk) && pk in est.meta_data
            res_meta_data[pk] = est.events[est.meta_data[pk]]
        end
    end

    res = res.wrapped

    append!(res, collect(values(res_meta_data)))

    res
end

function feed(
        est::DB.CacheStorage;
        pubkey::Any=nothing, notes::Union{Symbol,String}=:follows, include_replies=false,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
    )
    limit <= 1000 || error("limit too big")
    notes = Symbol(notes)
    pubkey isa Nothing || pubkey isa Nostr.PubKeyId || (pubkey = Nostr.PubKeyId(pubkey))

    posts = [] |> ThreadSafe
    if pubkey isa Nothing
        @threads for dbconn in est.pubkey_events
            append!(posts, map(Tuple, DB.exe(dbconn, DB.@sql("select event_id, created_at from kv 
                                                              where created_at >= ? and created_at <= ? and (is_reply = 0 or is_reply = ?)
                                                              order by created_at desc limit ? offset ?"),
                                             (since, until, Int(include_replies), limit, offset))))
        end
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
    response_messages_for_posts(est, eids)
end

function thread_view(est::DB.CacheStorage; event_id, kwargs...)
    vcat(thread_view_replies(est; event_id, kwargs...), thread_view_parents(est; event_id))
end

function thread_view_replies(est::DB.CacheStorage;
        event_id,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0)
    limit <= 1000 || error("limit too big")
    event_id isa Nostr.EventId || (event_id = Nostr.EventId(event_id))

    posts = Tuple{Nostr.EventId, Int}[]
    for (reid, created_at) in DB.exe(est.event_replies, DB.@sql("select reply_event_id, reply_created_at from kv
                                                                 where event_id = ? and reply_created_at >= ? and reply_created_at <= ?
                                                                 order by reply_created_at desc limit ? offset ?"),
                                     event_id, since, until, limit, offset)
        push!(posts, (Nostr.EventId(reid), created_at))
    end
    posts = sort(posts, by=p->-p[2])[1:min(limit, length(posts))]
    
    reids = [reid for (reid, _) in posts]
    response_messages_for_posts(est, reids)
end

function thread_view_parents(est::DB.CacheStorage; event_id)
    event_id isa Nostr.EventId || (event_id = Nostr.EventId(event_id))

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
    response_messages_for_posts(est, reids)
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

function contact_list(est::DB.CacheStorage; pubkey::Any=nothing)
    pubkey isa Nostr.PubKeyId || (pubkey = Nostr.PubKeyId(pubkey))
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
    ext_user_infos(est, res, res_meta_data)
    res
end

function events(est::DB.CacheStorage; event_ids::Vector, extended_response::Bool=false)
    event_ids = [eid isa Nostr.EventId ? eid : Nostr.EventId(eid) for eid in event_ids]

    if !extended_response
        res = [] |> ThreadSafe
        @threads for eid in event_ids 
            eid in est.events && push!(res, est.events[eid])
        end
        res.wrapped
    else
        response_messages_for_posts(est, event_ids)
    end
end

function user_profile(est::DB.CacheStorage; pubkey)
    pubkey isa Nostr.PubKeyId || (pubkey = Nostr.PubKeyId(pubkey))

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

function ext_user_infos(est::DB.CacheStorage, res, res_meta_data) end

end
