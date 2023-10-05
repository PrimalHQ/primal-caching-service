module App

import JSON
using .Threads: @threads
using DataStructures: OrderedSet, CircularBuffer

import ..DB
import ..Nostr
using ..Utils: ThreadSafe

exposed_functions = Set([:feed,
                         :thread_view,
                         :network_stats,
                         :contact_list,
                         :is_user_following,
                         :user_infos,
                         :user_followers,
                         :events,
                         :event_actions,
                         :user_profile,
                         :get_directmsg_contacts,
                         :reset_directmsg_count,
                         :reset_directmsg_counts,
                         :get_directmsgs,
                         :mutelist,
                         :mutelists,
                         :allowlist,
                         :parameterized_replaceable_list,
                         :search_filterlist,
                         :import_events,
                         :zaps_feed,
                         :user_zaps,
                         :user_zaps_by_satszapped,
                        ])

exposed_async_functions = Set([:net_stats, 
                               :directmsg_count,
                              ])

EVENT_STATS=10_000_100
NET_STATS=10_000_101
USER_PROFILE=10_000_105
REFERENCED_EVENT=10_000_107
RANGE=10_000_113
EVENT_ACTIONS_COUNT=10_000_115
DIRECTMSG_COUNT=10_000_117
DIRECTMSG_COUNTS=10_000_118
EVENT_IDS=10_000_122
PARTIAL_RESPONSE=10_000_123
IS_USER_FOLLOWING=10_000_125
EVENT_IMPORT_STATUS=10_000_127
ZAP_EVENT=10_000_129
FILTERING_REASON=10_000_131
USER_FOLLOWER_COUNTS=10_000_133

cast(value, type) = value isa type ? value : type(value)
castmaybe(value, type) = (isnothing(value) || ismissing(value)) ? value : cast(value, type)

PRINT_EXCEPTIONS = Ref(false)
exceptions = CircularBuffer(200)

function catch_exception(body::Function, args...; rethrow_exception=false)
    try
        body()
    catch ex
        push!(exceptions, (time(), ex, args...))
        PRINT_EXCEPTIONS[] && Utils.print_exceptions()
        rethrow_exception && rethrow()
        nothing
    end
end

function range(res::Vector, order_by; by=r->r[2])
    if isempty(res)
        [(; kind=Int(RANGE), content=JSON.json((; order_by)))]
    else
        [(; kind=Int(RANGE), content=JSON.json((; since=by(res[end]), until=by(res[1]), order_by)))]
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

function event_actions_cnt(est::DB.CacheStorage, eid::Nostr.EventId, user_pubkey::Nostr.PubKeyId)
    r = DB.exe(est.event_pubkey_actions, DB.@sql("select replied, liked, reposted, zapped from kv where event_id = ?1 and pubkey = ?2"), eid, user_pubkey)
    if isempty(r)
        []
    else
        ea = zip([:replied, :liked, :reposted, :zapped], map(Bool, r[1]))
        [(; 
          kind=Int(EVENT_ACTIONS_COUNT),
          content=JSON.json((; event_id=eid, ea...)))]
    end
end

function event_actions(est::DB.CacheStorage; event_id, user_pubkey, kind::Int, limit=100)
    limit <= 1000 || error("limit too big")
    event_id = cast(event_id, Nostr.EventId)
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    pks = [Nostr.PubKeyId(pk) 
           for (pk,) in DB.exe(est.event_pubkey_action_refs,
                               DB.@sql("select ref_pubkey from kv 
                                        where event_id = ?1 and ref_kind = ?2 
                                        order by ref_created_at desc
                                        limit ?3"),
                               event_id, kind, limit)]
    user_infos(est; pubkeys=pks)
end

THash = Vector{UInt8}
parsed_mutelists = Dict{Nostr.PubKeyId, Tuple{Nostr.EventId, Any}}() |> ThreadSafe
compiled_content_moderation_rules = Dict{Union{Nostr.PubKeyId,Nothing}, Tuple{THash, Any}}() |> ThreadSafe

function compile_content_moderation_rules(est::DB.CacheStorage, pubkey)
    cmr = (; 
           pubkeys = Dict{Nostr.PubKeyId, NamedTuple{(:parent, :scopes), Tuple{Nostr.PubKeyId, Set{Symbol}}}}(),
           groups = Dict{Symbol, NamedTuple{(:scopes,), Tuple{Set{Symbol}}}}(),
           pubkeys_allowed = Set{Nostr.PubKeyId}(),
          )
    r = catch_exception(:compile_content_moderation_rules, (; pubkey)) do
        settings = ext_user_get_settings(est, pubkey)
        (isnothing(settings) || !settings["applyContentModeration"]) && return cmr

        eids = Set{Nostr.EventId}()

        push!(eids, settings["id"])

        if !isnothing(pubkey)
            ml = (; 
                  pubkeys = Dict{Nostr.PubKeyId, Set{Symbol}}(),
                  groups = Dict{Symbol, Set{Symbol}}())

            if pubkey in est.mute_lists
                local mlseid = est.mute_lists[pubkey]
                push!(eids, mlseid)

                pml = get(parsed_mutelists, pubkey, nothing)
                if isnothing(pml) || pml[1] != mlseid
                    for tag in est.events[mlseid].tags
                        if length(tag.fields) >= 2
                            if tag.fields[1] == "p"
                                scopes = if length(tag.fields) >= 5
                                    Set([Symbol(s) for s in JSON.parse(tag.fields[5])])
                                else
                                    Set{Symbol}([:content, :trending])
                                end
                                if !isempty(scopes)
                                    ml.pubkeys[Nostr.PubKeyId(tag.fields[2])] = scopes
                                end
                                # elseif tag.fields[1] == "group"
                                #     ml.groups[Symbol(tag.fields[2])] = if length(tag.fields) >= 3
                                #         Set([Symbol(s) for s in JSON.parse(tag.fields[3])])
                                #     else
                                #         Set{Symbol}()
                                #     end
                            end
                        end
                    end
                    parsed_mutelists[pubkey] = (mlseid, ml)
                else
                    ml = pml[2]
                end
            end

            pks = Set{Nostr.PubKeyId}()

            push!(eids, settings["id"])

            pubkey in est.mute_lists && push!(eids, est.mute_lists[pubkey])
            pubkey in est.allow_list && push!(eids, est.allow_list[pubkey])

            for pk in [pubkey; collect(keys(ml.pubkeys))]
                for tbl in [est.mute_list, est.mute_list_2, est.allow_list]
                    pk in tbl && push!(eids, tbl[pk])
                end
            end
        end

        eids = sort(collect(eids))

        h = SHA.sha256(vcat([0x00], [eid.hash for eid in eids]...))

        cmr_ = get(compiled_content_moderation_rules, pubkey, nothing)
        !isnothing(cmr_) && cmr_[1] == h && return cmr_[2]

        catch_exception(:compile_content_moderation_rules_calc, (; pubkey)) do
            !isnothing(pubkey) && for ppk in [pubkey, collect(keys(ml.pubkeys))...]
                for tbl in [est.mute_list, est.mute_list_2]
                    if ppk in tbl
                        for tag in est.events[tbl[ppk]].tags
                            if length(tag.fields) >= 2 && tag.fields[1] == "p"
                                if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                                    cmr.pubkeys[pk] = (; parent=ppk, 
                                                       scopes=haskey(ml.pubkeys, ppk) ? ml.pubkeys[ppk] : Set{Symbol}())
                                end
                            end
                        end
                    end
                end
            end

            for cm in settings["contentModeration"]
                scopes = Set([Symbol(s) for s in cm["scopes"]])
                if !isempty(scopes)
                    cmr.groups[Symbol(cm["name"])] = (; scopes)
                end
            end

            !isnothing(pubkey) && if pubkey in est.allow_list
                for tag in est.events[est.allow_list[pubkey]].tags
                    if length(tag.fields) >= 2 && tag.fields[1] == "p"
                        if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                            push!(cmr.pubkeys_allowed, pk)
                        end
                    end
                end
            end
        end

        compiled_content_moderation_rules[pubkey] = (h, cmr)

        ext_invalidate_cached_content_moderation(est, pubkey)

        cmr
    end
    r isa NamedTuple ? r : cmr
end

function is_hidden(est::DB.CacheStorage, user_pubkey, scope::Symbol, pubkey::Nostr.PubKeyId)
    cmr = compile_content_moderation_rules(est, user_pubkey)
    # cmr isa Bool && @show (user_pubkey, cmr)
    pubkey in cmr.pubkeys_allowed && return false
    if haskey(cmr.pubkeys, pubkey)
        scopes = cmr.pubkeys[pubkey].scopes
        isempty(scopes) ? true : scope in scopes
    else
        ext_is_hidden_by_group(est, user_pubkey, scope, pubkey)
    end
end
function is_hidden(est::DB.CacheStorage, user_pubkey, scope::Symbol, eid::Nostr.EventId)
    eid in est.events && is_hidden(est, user_pubkey, scope, est.events[eid].pubkey) && return true
    ext_is_hidden_by_group(est, user_pubkey, scope, eid)
end

function response_messages_for_posts(
        est::DB.CacheStorage, eids::Vector{Nostr.EventId}; 
        res_meta_data=Dict(), user_pubkey=nothing,
    )
    res = OrderedSet() |> ThreadSafe

    pks = Set{Nostr.PubKeyId}() |> ThreadSafe
    res_meta_data = res_meta_data |> ThreadSafe

    function handle_event(body::Function, eid::Nostr.EventId; wrapfun::Function=identity)
        ext_is_hidden(est, eid) && return
        eid in est.deleted_events && return

        eid in est.events || return
        e = est.events[eid]
        is_hidden(est, user_pubkey, :content, e.pubkey) && return
        ext_is_hidden(est, e.pubkey) && return
        push!(res, wrapfun(e))
        union!(res, event_stats(est, e.id))
        isnothing(user_pubkey) || union!(res, event_actions_cnt(est, e.id, user_pubkey))
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
        time_exceeded=()->false,
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
    elseif notes == :replies
        append!(posts, map(Tuple, DB.exe(est.pubkey_events, DB.@sql("select event_id, created_at from kv 
                                                                    where pubkey = ? and created_at >= ? and created_at <= ? and is_reply = 1
                                                                    order by created_at desc limit ? offset ?"),
                                         pubkey, since, until, limit, offset)))
    else
        pubkeys = 
        if     notes == :follows;  follows(est, pubkey)
        elseif notes == :authored; [pubkey]
        else;                      error("unsupported type of notes")
        end
        @threads for p in pubkeys
            time_exceeded() && break
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
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    est.auto_fetch_missing_events && DB.fetch_event(est, event_id)

    res = []

    hidden = is_hidden(est, user_pubkey, :content, event_id) || ext_is_hidden(est, event_id) || event_id in est.deleted_events

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

function user_scores(est::DB.CacheStorage, res_meta_data)
    d = Dict([(Nostr.hex(e.pubkey), get(est.pubkey_followers_cnt, e.pubkey, 0))
              for e in collect(res_meta_data)])
    isempty(d) ? [] : [(; kind=Int(USER_SCORES), content=JSON.json(d))]
end

function contact_list(est::DB.CacheStorage; pubkey, extended_response=true)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    res = []

    if pubkey in est.contact_lists
        eid = est.contact_lists[pubkey]
        eid in est.events && push!(res, est.events[eid])
    end

    extended_response && append!(res, user_infos(est; pubkeys=follows(est, pubkey)))

    res
end

function is_user_following(est::DB.CacheStorage; pubkey, user_pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    [(; 
      kind=Int(IS_USER_FOLLOWING),
      content=JSON.json(user_pubkey in follows(est, pubkey)))]
end

function user_infos(est::DB.CacheStorage; pubkeys::Vector)
    pubkeys = [pk isa Nostr.PubKeyId ? pk : Nostr.PubKeyId(pk) for pk in pubkeys]

    res_meta_data = Dict() |> ThreadSafe
    @threads for pk in pubkeys 
        if pk in est.meta_data
            eid = est.meta_data[pk]
            eid in est.events && (res_meta_data[pk] = est.events[eid])
        end
    end
    res_meta_data_arr = []
    for pk in pubkeys
        haskey(res_meta_data, pk) && push!(res_meta_data_arr, res_meta_data[pk])
    end
    res = [res_meta_data_arr..., user_scores(est, res_meta_data_arr)..., 
           (; kind=Int(USER_FOLLOWER_COUNTS),
            content=JSON.json(Dict([Nostr.hex(pk)=>get(est.pubkey_followers_cnt, pk, 0) for pk in pubkeys]))
           )]
    ext_user_infos(est, res, res_meta_data_arr)
    res
end

function user_followers(est::DB.CacheStorage; pubkey, limit=200)
    limit <= 1000 || error("limit too big")
    pubkey = cast(pubkey, Nostr.PubKeyId)
    pks = Set{Nostr.PubKeyId}()
    for pk in follows(est, pubkey)
        if !isempty(DB.exe(est.pubkey_followers, 
                           DB.@sql("select 1 from kv 
                                   where pubkey = ?1 and follower_pubkey = ?2
                                   limit 1"), pubkey, pk))
            push!(pks, pk)
        end
    end
    for r in DB.exe(est.pubkey_followers, 
                    DB.@sql("select follower_pubkey from kv 
                            where pubkey = ?1 
                            order by follower_pubkey
                            limit ?2"), pubkey, limit)
        length(pks) < limit || break
        pk = Nostr.PubKeyId(r[1])
        pk in pks || push!(pks, pk)
    end

    user_infos(est; pubkeys=collect(pks))
end

function events(
        est::DB.CacheStorage; 
        event_ids::Vector=[], extended_response::Bool=false, user_pubkey=nothing,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        idsonly=false,
    )
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    if isempty(event_ids)
        event_ids = [r[1] for r in DB.exec(est.event_created_at, 
                                           DB.@sql("select event_id from kv 
                                                   where created_at >= ?1 and created_at <= ?2 
                                                   order by created_at asc 
                                                   limit ?3 offset ?4"),
                                           (since, until, limit, offset))]
    end

    event_ids = [cast(eid, Nostr.EventId) for eid in event_ids]

    if idsonly
        [(; kind=Int(EVENT_IDS), ids=event_ids)]
    elseif !extended_response
        res = [] |> ThreadSafe
        @threads for eid in event_ids 
            eid in est.events && push!(res, est.events[eid])
        end
        sort(res.wrapped; by=e->e.created_at)
    else
        response_messages_for_posts(est, event_ids; user_pubkey)
    end
end

function user_profile(est::DB.CacheStorage; pubkey, user_pubkey=nothing)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    est.auto_fetch_missing_events && DB.fetch_user_metadata(est, pubkey)

    res = [] |> ThreadSafe

    pubkey in est.meta_data && push!(res, est.events[est.meta_data[pubkey]])

    note_count  = DB.exe(est.pubkey_events, DB.@sql("select count(1) from kv where pubkey = ? and is_reply = false"), pubkey)[1][1]
    reply_count = DB.exe(est.pubkey_events, DB.@sql("select count(1) from kv where pubkey = ? and is_reply = true"), pubkey)[1][1]

    time_joined_r = DB.exe(est.pubkey_events, DB.@sql("select created_at from kv
                                                       where pubkey = ?
                                                       order by created_at asc limit 1"), pubkey)

    time_joined = isempty(time_joined_r) ? nothing : time_joined_r[1][1];

    relay_count = 0
    if pubkey in est.contact_lists
        clid = est.contact_lists[pubkey]
        if clid in est.events
            try relay_count = length(keys(JSON.parse(est.events[clid].content))) catch _ end
        end
    end

    push!(res, (;
                kind=Int(USER_PROFILE),
                content=JSON.json((;
                                   pubkey,
                                   follows_count=length(follows(est, pubkey)),
                                   followers_count=get(est.pubkey_followers_cnt, pubkey, 0),
                                   note_count,
                                   reply_count,
                                   time_joined,
                                   relay_count,
                                   ext_user_profile(est, pubkey)...,
                                  ))))

    !isnothing(user_pubkey) && is_hidden(est, user_pubkey, :content, pubkey) && append!(res, search_filterlist(est; pubkey, user_pubkey))

    res.wrapped
end

function parse_event_from_user(event_from_user::Dict)
    e = Nostr.Event(event_from_user)
    e.created_at > time() - 300 || error("event is too old")
    e.created_at < time() + 300 || error("event from the future")
    Nostr.verify(e) || error("verification failed")
    e
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

        is_hidden(est, user_pubkey, :content, peer) && continue

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

    e = parse_event_from_user(event_from_user)

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

    e = parse_event_from_user(event_from_user)

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
    res = []
    for (eid, created_at) in DB.exe(est.pubkey_directmsgs,
                                    DB.@sql("select event_id, created_at from pubkey_directmsgs where 
                                            ((receiver is ?1 and sender is ?2) or (receiver is ?2 and sender is ?1)) and
                                            created_at >= ?3 and created_at <= ?4 
                                            order by created_at desc limit ?5 offset ?6"),
                                    receiver, sender, since, until, limit, offset)
        e = est.events[Nostr.EventId(eid)]
        push!(res, e)
        push!(msgs, (e, created_at))
    end

    res_meta_data = Dict()
    for pk in [receiver, sender]
        if !haskey(res_meta_data, pk) && pk in est.meta_data
            mdid = est.meta_data[pk]
            if mdid in est.events
                res_meta_data[pk] = est.events[mdid]
            end
        end
    end

    res_meta_data = collect(values(res_meta_data))
    append!(res, res_meta_data)
    ext_user_infos(est, res, res_meta_data)

    [res..., range(msgs, :created_at)]
end

function response_messages_for_list(est::DB.CacheStorage, tables, pubkey, extended_response=true)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    res = []
    push_event(eid) = eid in est.events && push!(res, est.events[eid])
    for tbl in tables
        pubkey in tbl && push_event(tbl[pubkey])
    end

    if extended_response
        res_meta_data = Dict()
        for e in res
            for tag in e.tags
                if length(tag.fields) == 2 && tag.fields[1] == "p"
                    if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                        if !haskey(res_meta_data, pk) && pk in est.meta_data
                            mdid = est.meta_data[pk]
                            if mdid in est.events
                                res_meta_data[pk] = est.events[mdid]
                            end
                        end
                    end
                end
            end
        end
        res_meta_data = collect(values(res_meta_data))
        append!(res, res_meta_data)
        append!(res, user_scores(est, res_meta_data))
        ext_user_infos(est, res, res_meta_data)
    end

    res
end

function mutelist(est::DB.CacheStorage; pubkey, extended_response=true)
    response_messages_for_list(est, [est.mute_list, est.mute_list_2], pubkey, extended_response)
end
function mutelists(est::DB.CacheStorage; pubkey, extended_response=true)
    response_messages_for_list(est, [est.mute_lists], pubkey, extended_response)
end
function allowlist(est::DB.CacheStorage; pubkey, extended_response=true)
    response_messages_for_list(est, [est.allow_list], pubkey, extended_response)
end

function parameterized_replaceable_list(est::DB.CacheStorage; pubkey, identifier::String, extended_response=true)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    res = []
    for (eid,) in DB.exe(est.parameterized_replaceable_list, DB.@sql("select event_id from parameterized_replaceable_list where pubkey = ?1 and identifier = ?2"), pubkey, identifier)
        eid = Nostr.EventId(eid)
        push!(res, est.events[eid])
    end

    if extended_response
        res_meta_data = Dict()
        for e in res
            for tag in e.tags
                if length(tag.fields) == 2 && tag.fields[1] == "p"
                    if !isnothing(local pk = try Nostr.PubKeyId(tag.fields[2]) catch _ end)
                        if !haskey(res_meta_data, pk) && pk in est.meta_data
                            mdid = est.meta_data[pk]
                            if mdid in est.events
                                res_meta_data[pk] = est.events[mdid]
                            end
                        end
                    end
                end
            end
        end
        res_meta_data = collect(values(res_meta_data))
        append!(res, res_meta_data)
        append!(res, user_scores(est, res_meta_data))
        ext_user_infos(est, res, res_meta_data)
    end

    res
end

function search_filterlist(est::DB.CacheStorage; pubkey, user_pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    cmr = compile_content_moderation_rules(est, user_pubkey)
    res = nothing
    if pubkey in cmr.pubkeys_allowed
        res = (; action=:allow, pubkey=user_pubkey)
        [est.events[est.meta_data[user_pubkey]], 
         (; kind=Int(FILTERING_REASON), content=JSON.json(res))]
    elseif haskey(cmr.pubkeys, pubkey)
        res = (; action=:block, pubkey=cmr.pubkeys[pubkey].parent)
        [est.events[est.meta_data[res.pubkey]], 
         (; kind=Int(FILTERING_REASON), content=JSON.json(res))]
    else
        for (grpname, g) in cmr.groups
            if grpname == :primal_spam && pubkey in Filterlist.access_pubkey_blocked_spam
                res = (; action=:block, group=grpname)
                break
            elseif grpname == :primal_nsfw && any([is_hidden_on_primal_nsfw(est, user_pubkey, scope, pubkey) for scope in [:content, :trending]])
                res = (; action=:block, group=grpname)
                break
            end
        end
    end
    isnothing(res) ? [] : [ (; kind=Int(FILTERING_REASON), content=JSON.json(res))]
end

function import_events(est::DB.CacheStorage; events::Vector=[])
    cnt = Ref(0)
    errcnt = Ref(0)
    for e in events
        try
            msg = JSON.json([time(), nothing, ["EVENT", "", e]])
            if DB.import_msg_into_storage(msg, est)
                cnt[] += 1
            end
        catch _ 
            errcnt[] += 1
        end
    end
    [(; kind=Int(EVENT_IMPORT_STATUS), content=JSON.json((; imported=cnt[], errors=errcnt[])))]
end

function response_messages_for_zaps(est, zaps; kinds=nothing, order_by=:created_at)
    res_meta_data = Dict()
    res = []
    for (zap_receipt_id, created_at, event_id, sender, receiver, amount_sats) in zaps
        for pk in [sender, receiver]
            (isnothing(pk) || ismissing(pk)) && continue
            pk = Nostr.PubKeyId(pk)
            if !haskey(res_meta_data, pk) && pk in est.meta_data
                res_meta_data[pk] = est.events[est.meta_data[pk]]
            end
        end
        zap_receipt_id = Nostr.EventId(zap_receipt_id)
        zap_receipt_id in est.events && push!(res, est.events[zap_receipt_id])
        if !ismissing(event_id)
            event_id = Nostr.EventId(event_id)
            event_id in est.events && push!(res, est.events[event_id])
        end
        push!(res, (; kind=Int(ZAP_EVENT), content=JSON.json((; 
                                                              event_id, 
                                                              created_at, 
                                                              sender=castmaybe(sender, Nostr.PubKeyId),
                                                              receiver=castmaybe(receiver, Nostr.PubKeyId),
                                                              amount_sats,
                                                              zap_receipt_id))))
    end

    res_meta_data = collect(values(res_meta_data))
    append!(res, res_meta_data)
    append!(res, user_scores(est, res_meta_data))
    ext_user_infos(est, res, res_meta_data)

    res_ = []
    by = if order_by == :created_at; r->r[2]
    elseif order_by == :amount_sats; r->r[6]
    else; error("invalid order_by")
    end
    for e in [collect(OrderedSet(res)); range(zaps, order_by; by)]
        if isnothing(kinds) || (hasproperty(e, :kind) && getproperty(e, :kind) in kinds)
            push!(res_, e)
        end
    end
    res_
end

function zaps_feed(
        est::DB.CacheStorage;
        pubkeys,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        kinds=nothing,
        time_exceeded=()->false,
    )
    limit=min(50, limit)
    limit <= 1000 || error("limit too big")
    pubkeys = [cast(pubkey, Nostr.PubKeyId) for pubkey in pubkeys]

    # pks = collect(union(pubkeys, [follows(est, pubkey) for pubkey in pubkeys]...))
    pks = pubkeys

    zaps = [] |> ThreadSafe
    @threads for p in pks
        time_exceeded() && break
        append!(zaps, map(Tuple, DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                                                   where (sender = ? or receiver = ?) and created_at >= ? and created_at <= ?
                                                                   order by created_at desc limit ? offset ?"),
                                         (p, p, since, until, limit, offset))))
    end

    zaps = sort(zaps.wrapped, by=z->-z[2])[1:min(limit, length(zaps))]

    response_messages_for_zaps(est, zaps; kinds)
end

function user_zaps(
        est::DB.CacheStorage;
        sender=nothing, receiver=nothing,
        kinds=nothing,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
    )
    limit=min(50, limit)
    limit <= 1000 || error("limit too big")
    sender = castmaybe(sender, Nostr.PubKeyId)
    receiver = castmaybe(receiver, Nostr.PubKeyId)

    zaps = if !isnothing(sender)
        map(Tuple, DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                                     where sender = ? and created_at >= ? and created_at <= ?
                                                     order by created_at desc limit ? offset ?"),
                           (sender, since, until, limit, offset)))
    elseif !isnothing(receiver)
        map(Tuple, DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                                     where receiver = ? and created_at >= ? and created_at <= ?
                                                     order by created_at desc limit ? offset ?"),
                           (receiver, since, until, limit, offset)))
    else
        error("either sender or receiver argument has to be specified")
    end

    zaps = sort(zaps, by=z->-z[2])[1:min(limit, length(zaps))]

    response_messages_for_zaps(est, zaps; kinds)
end

function user_zaps_by_satszapped(
        est::DB.CacheStorage;
        receiver=nothing,
        limit::Int=20, since::Int=0, until=nothing, offset::Int=0,
    )
    limit <= 1000 || error("limit too big")
    receiver = cast(receiver, Nostr.PubKeyId)

    isnothing(until) && (until = 1<<61)
    zaps = map(Tuple, DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                                        where receiver = ? and amount_sats >= ? and amount_sats <= ?
                                                        order by amount_sats desc limit ? offset ?"),
                              (receiver, since, until, limit, offset)))

    zaps = sort(zaps, by=z->-z[6])[1:min(limit, length(zaps))]

    response_messages_for_zaps(est, zaps; order_by=:amount_sats)
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
function ext_user_profile(est::DB.CacheStorage, pubkey); (;); end
function ext_is_hidden(est::DB.CacheStorage, eid::Nostr.EventId); false; end
function ext_is_hidden_by_group(est::DB.CacheStorage, user_pubkey, scope::Symbol, pubkey::Nostr.PubKeyId); false; end
function ext_is_hidden_by_group(est::DB.CacheStorage, user_pubkey, scope::Symbol, eid::Nostr.EventId); false; end
function ext_event_response(est::DB.CacheStorage, e::Nostr.Event); []; end
function ext_user_get_settings(est::DB.CacheStorage, pubkey); end
function ext_invalidate_cached_content_moderation(est::DB.CacheStorage, user_pubkey::Union{Nothing,Nostr.PubKeyId}); end

end
