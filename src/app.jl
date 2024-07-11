module App

import JSON
using .Threads: @threads
using DataStructures: OrderedSet, CircularBuffer, Accumulator
using ReadWriteLocks: read_lock
import Sockets
import Dates

import ..DB
import ..Nostr
using ..Utils: ThreadSafe, Throttle

exposed_functions = Set([:feed,
                         :feed_2,
                         :thread_view,
                         :network_stats,
                         :contact_list,
                         :is_user_following,
                         :user_infos,
                         :user_followers,
                         :mutual_follows,
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
                         :parametrized_replaceable_event,
                         :parametrized_replaceable_events,
                         :search_filterlist,
                         :import_events,
                         :zaps_feed,
                         :user_zaps,
                         :user_zaps_by_satszapped,
                         :event_zaps_by_satszapped,
                         :server_name,
                         :nostr_stats,
                         :is_hidden_by_content_moderation,
                         :user_of_ln_address,
                         :get_user_relays,
                         :get_bookmarks,
                         :get_highlights,
                         :long_form_content_feed,
                         :long_form_content_thread_view,
                         :get_recommended_reads,
                         :get_reads_topics,
                         :get_featured_authors,
                         :creator_paid_tiers,
                        ])

exposed_async_functions = Set([:net_stats, 
                               :directmsg_count,
                               :directmsg_count_2,
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
DIRECTMSG_COUNT_2=10_000_134
NOSTR_STATS=10_000_136
IS_HIDDEN_BY_CONTENT_MODERATION=10_000_137
USER_PUBKEY=10_000_138
USER_RELAYS=10_000_139
EVENT_RELAYS=10_000_141
LONG_FORM_METADATA=10_000_144
RECOMMENDED_READS=10_000_145
READS_TOPICS=10_000_146
CREATOR_PAID_TIERS=10_000_147
FEATURED_AUTHORS=10_000_148

cast(value, type) = value isa type ? value : type(value)
castmaybe(value, type) = (isnothing(value) || ismissing(value)) ? value : cast(value, type)

PRINT_EXCEPTIONS = Ref(false)
exceptions = CircularBuffer(200)

DAG_OUTPUTS_DB = Ref{Any}(nothing) |> ThreadSafe

function catch_exception(body::Function, args...; rethrow_exception=false, kwargs...)
    try
        body()
    catch ex
        push!(exceptions, (; t=Dates.now(), ex, args, kwargs...))
        PRINT_EXCEPTIONS[] && Utils.print_exceptions()
        rethrow_exception && rethrow()
        nothing
    end
end

function range(res::Vector, order_by; by=r->r[2])
    if isempty(res)
        [(; kind=Int(RANGE), content=JSON.json((; order_by)))]
    else
        since = min(by(res[1]), by(res[end]))
        until = max(by(res[1]), by(res[end]))
        [(; kind=Int(RANGE), content=JSON.json((; since, until, order_by)))]
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
        # res
        first(res, 1600)
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

function event_actions(est::DB.CacheStorage; 
        event_id=nothing,
        pubkey=nothing, identifier=nothing,
        kind::Int, 
        limit=100, offset=0,
    )
    limit <= 1000 || error("limit too big")
    event_id = castmaybe(event_id, Nostr.EventId)
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)

    res = []
    pks = Set{Nostr.PubKeyId}()

    r = if !isnothing(event_id)
        DB.exe(est.event_pubkey_action_refs, DB.@sql("
                        select ref_event_id, ref_pubkey from kv 
                        where event_id = ?1 and ref_kind = ?2 
                        order by ref_created_at desc
                        limit ?3 offset ?4"),
               event_id, kind, limit, offset)
                        
    elseif !isnothing(pubkey) && !isnothing(identifier) 
        Postgres.pex(DAG_OUTPUTS_DB[], pgparams() do P "
                        SELECT
                            ref_event_id, 
                            ref_pubkey
                        FROM
                            prod.reads_versions,
                            prod.event_pubkey_action_refs
                        WHERE 
                            reads_versions.pubkey = $(@P pubkey) AND 
                            reads_versions.identifier = $(@P identifier) AND 
                            reads_versions.eid = event_pubkey_action_refs.event_id AND
                            event_pubkey_action_refs.ref_kind = $(@P kind)
                        ORDER BY
                            event_pubkey_action_refs.ref_created_at DESC
                        LIMIT $(@P limit) OFFSET $(@P offset)
                    " end...)
    else
        []
    end

    for (reid, pk) in r
        push!(pks, Nostr.PubKeyId(pk))
        reid = Nostr.EventId(reid)
        reid in est.events && push!(res, est.events[reid])
    end

    [res; user_infos(est; pubkeys=collect(pks))]
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

RELAY_URL_MAP = Dict{String, String}()

response_messages_for_posts_cache_periodic = Throttle(; period=300.0)
response_messages_for_posts_cache = Dict{Tuple{Nostr.EventId, Union{Nothing, Nostr.PubKeyId}}, Any}() |> ThreadSafe
response_messages_for_posts_res_meta_data_cache = Dict{Nostr.EventId, Any}() |> ThreadSafe
response_messages_for_posts_mds_cache = Dict{Nostr.EventId, Any}() |> ThreadSafe

ng_any_user = Ref(true)
ng_whitelist = Set{Nostr.PubKeyId}() |> ThreadSafe

function response_messages_for_posts(
        est::DB.CacheStorage, eids::Vector{Nostr.EventId}; 
        res_meta_data=Dict(), user_pubkey=nothing,
        time_exceeded=()->false,
    )
    if ng_any_user[] || user_pubkey in ng_whitelist
        response_messages_for_posts_2(est, eids; res_meta_data, user_pubkey, time_exceeded)
    else
        response_messages_for_posts_1(est, eids; res_meta_data, user_pubkey, time_exceeded)
    end
end

function response_messages_for_posts_1(
        est::DB.CacheStorage, eids::Vector{Nostr.EventId}; 
        res_meta_data=Dict(), user_pubkey=nothing,
        time_exceeded=()->false,
    )
    res = OrderedSet() |> ThreadSafe

    pks = Set{Nostr.PubKeyId}() |> ThreadSafe
    res_meta_data = res_meta_data |> ThreadSafe

    event_relays = Dict{Nostr.EventId, String}()

    function handle_event(body::Function, eid::Nostr.EventId; wrapfun::Function=identity)
        ext_is_hidden(est, eid) && return
        eid in est.deleted_events && return

        eid in est.events || return
        e = est.events[eid]

        is_hidden(est, user_pubkey, :content, e.pubkey) && return
        ext_is_hidden(est, e.pubkey) && return

        e.kind == Int(Nostr.REPOST) && try 
            hide = false
            for t in e.tags
                if t.fields[1] == "p"
                    pk = Nostr.PubKeyId(t.fields[2])
                    hide |= is_hidden(est, user_pubkey, :content, pk) || ext_is_hidden(est, pk) 
                end
            end
            hide
        catch _ false end && return

        push!(res, wrapfun(e))
        union!(res, e.kind == Int(Nostr.LONG_FORM_CONTENT) ? 
               ext_long_form_event_stats(est, e.id) : 
               event_stats(est, e.id))
        isnothing(user_pubkey) || union!(res, event_actions_cnt(est, e.id, user_pubkey))
        push!(pks, e.pubkey)
        union!(res, ext_event_response(est, e))
        haskey(est.dyn[:event_relay], eid) && (event_relays[eid] = est.dyn[:event_relay][eid])

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
            yield()
            time_exceeded() && return
            handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e))) do subeid
                yield()
                time_exceeded() && return
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

    !isempty(event_relays) && union!(res, [(; kind=Int(EVENT_RELAYS), content=JSON.json(Dict([Nostr.hex(k)=>get(RELAY_URL_MAP, v, v) for (k, v) in event_relays])))])

    res = collect(res)

    for md in values(res_meta_data)
        push!(res, md)
        union!(res, ext_event_response(est, md))
    end

    res
end

function response_messages_for_posts_2(
        est::DB.CacheStorage, eids::Vector{Nostr.EventId}; 
        res_meta_data=Dict(), user_pubkey=nothing,
        time_exceeded=()->false,
    )
    response_messages_for_posts_cache_periodic() do
        empty!(response_messages_for_posts_cache)
        empty!(response_messages_for_posts_res_meta_data_cache)
        empty!(response_messages_for_posts_mds_cache)
    end

    function mkres()
        (;
         res = OrderedSet(),
         pks = Set{Nostr.PubKeyId}(),
         event_relays = Dict{Nostr.EventId, String}())
    end

    function handle_event(
            body::Function, eid::Nostr.EventId; 
            wrapfun::Function=identity, 
            res::OrderedSet, pks::Set{Nostr.PubKeyId}, event_relays::Dict{Nostr.EventId, String},
        )
        ext_is_hidden(est, eid) && return
        eid in est.deleted_events && return

        eid in est.events || return
        e = est.events[eid]

        is_hidden(est, user_pubkey, :content, e.pubkey) && return
        ext_is_hidden(est, e.pubkey) && return

        e.kind == Int(Nostr.REPOST) && try 
            hide = false
            for t in e.tags
                if t.fields[1] == "p"
                    pk = Nostr.PubKeyId(t.fields[2])
                    hide |= is_hidden(est, user_pubkey, :content, pk) || ext_is_hidden(est, pk) 
                end
            end
            hide
        catch _ false end && return

        if e.kind == Int(Nostr.LONG_FORM_CONTENT)
            union!(res, [(; kind=Int(LONG_FORM_METADATA), content=JSON.json((; event_id=e.id, words=length(split(e.content)))))])
        end

        push!(res, wrapfun(e))
        union!(res, e.kind == Int(Nostr.LONG_FORM_CONTENT) ? 
               ext_long_form_event_stats(est, e.id) : 
               event_stats(est, e.id))
        isnothing(user_pubkey) || union!(res, event_actions_cnt(est, e.id, user_pubkey))
        push!(pks, e.pubkey)
        union!(res, ext_event_response(est, e))
        haskey(est.dyn[:event_relay], eid) && (event_relays[eid] = est.dyn[:event_relay][eid])

        union!(res, [e for e in event_zaps_by_satszapped(est; event_id=eid, limit=5, user_pubkey)
                     if e.kind != Int(RANGE) && e.kind != Int(Nostr.TEXT_NOTE)])

        extra_tags = Nostr.Tag[]
        DB.for_mentiones(est, e) do tag
            push!(extra_tags, tag)
        end
        all_tags = vcat(e.tags, extra_tags)
        for tag in all_tags
            tag = tag.fields
            if length(tag) >= 2
                if tag[1] == "e"
                    try body(Nostr.EventId(tag[2])) catch _ end
                elseif tag[1] == "p"
                    try push!(pks, Nostr.PubKeyId(tag[2])) catch _ end
                end
            end
        end
    end

    r2 = mkres()

    event_relays = r2.event_relays

    # @time "eids"
    for eid in eids
        r = get(response_messages_for_posts_cache, (eid, user_pubkey), nothing)

        if isnothing(r)
            r = mkres()

            # @time "handle_event" 
            handle_event(eid; r.res, r.pks, r.event_relays) do subeid
                yield()
                time_exceeded() && return
                handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e)), r.res, r.pks, r.event_relays) do subeid
                    yield()
                    time_exceeded() && return
                    handle_event(subeid; wrapfun=e->(; kind=Int(REFERENCED_EVENT), content=JSON.json(e)), r.res, r.pks, r.event_relays) do _
                    end
                end
            end

            response_messages_for_posts_cache[(eid, user_pubkey)] = r
        end

        union!(r2.res, r.res)
        union!(r2.pks, r.pks)
        event_relays = union(event_relays, r.event_relays)
    end

    # @show [e.kind for e in collect(r2.res) if occursin("3c88a6fe308d78302599f050cfcbf3dd9c60f11413d690e5a48d8b593392be12", JSON.json(e))]

    res_meta_data = res_meta_data |> ThreadSafe
    # @time "pks" 
    for pk in r2.pks
        if !haskey(res_meta_data, pk) && pk in est.meta_data
            mdid = est.meta_data[pk]
            res_meta_data[pk] = lock(response_messages_for_posts_res_meta_data_cache) do response_messages_for_posts_res_meta_data_cache
                get!(response_messages_for_posts_res_meta_data_cache, mdid) do
                    est.events[mdid]
                end
            end
        end
    end

    !isempty(event_relays) && union!(r2.res, [(; kind=Int(EVENT_RELAYS), content=JSON.json(Dict([Nostr.hex(k)=>get(RELAY_URL_MAP, v, v) for (k, v) in event_relays])))])

    res = copy(r2.res)

    #@time "mds" 
    for md in values(res_meta_data)
        push!(res, md)
        union!(res, lock(response_messages_for_posts_mds_cache) do response_messages_for_posts_mds_cache
                   get!(response_messages_for_posts_mds_cache, md.id) do
                       ext_event_response(est, md)
                   end
               end)
    end

    collect(res)
end

function feed(
        est::DB.CacheStorage;
        pubkey=nothing, notes::Union{Symbol,String}=:follows, include_replies=false,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
        time_exceeded=()->false,
        kwargs...,
    )
    feed_2(est; pubkey, notes, include_replies,
           since, until, limit, offset, order=:desc,
           user_pubkey, time_exceeded, kwargs...)
end

function feed_2(
        est::DB.CacheStorage;
        pubkey=nothing, notes::Union{Symbol,String}=:follows, include_replies=false,
        kinds=nothing,
        since::Union{Nothing,Int}=nothing, until::Union{Nothing,Int}=nothing, limit::Int=20, offset::Int=0, order::Union{Nothing,Symbol,String}=nothing,
        user_pubkey=nothing,
        time_exceeded=()->false,
    )
    limit <= 1000 || error("limit too big")

    order = 
    if !isnothing(order)
        Symbol(order)
    elseif !isnothing(until)
        :desc
    elseif !isnothing(since)
        :asc
    else
        :desc
    end
    order in [:asc, :desc] || error("invalid order")

    since = isnothing(since) ? 0 : since
    until = isnothing(until) ? trunc(Int, time()) : until

    notes = Symbol(notes)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    tdur1 = tdur2 = 0
    events_scanned = 0

    posts = [] |> ThreadSafe
    if pubkey isa Nothing
        # @threads for dbconn in est.pubkey_events
        #     append!(posts, map(Tuple, DB.exe(dbconn, DB.@sql("select event_id, created_at from kv 
        #                                                       where created_at >= ? and created_at <= ? and (is_reply = 0 or is_reply = ?)
        #                                                       order by created_at desc limit ? offset ?"),
        #                                      (since, until, Int(include_replies), limit, offset))))
        # end
    elseif notes == :replies
        append!(posts, map(Tuple, DB.exe(est.pubkey_events, 
                                         "select event_id, created_at from kv 
                                         where pubkey = ? and created_at >= ? and created_at <= ? and is_reply = 1
                                         order by created_at $order limit ? offset ?",
                           pubkey, since, until, limit, offset)))
    elseif notes == :bookmarks
        if !isempty(local r = get_bookmarks(est; pubkey))
            bm = r[1]
            eids = []
            for t in bm.tags
                if length(t.fields) >= 2 
                    if     t.fields[1] == "e" && !isnothing(local eid = try Nostr.EventId(t.fields[2]) catch _ end)
                        push!(eids, eid)
                    elseif t.fields[1] == "a"
                        try
                            kind, pk, identifier = map(string, split(t.fields[2], ':'))
                            kind = parse(Int, kind)
                            pk = Nostr.PubKeyId(pk)
                            for (eid,) in DB.exe(est.dyn[:parametrized_replaceable_events], 
                                                 DB.@sql("select event_id from parametrized_replaceable_events 
                                                         where pubkey = ? and identifier = ? and kind = ?"), 
                                                 pk, identifier, kind)
                                push!(eids, Nostr.EventId(eid))
                            end
                        catch ex
                            println(ex)
                        end
                    end
                end
            end
            bms = []
            for eid in eids
                if eid in est.events 
                    e = est.events[eid]
                    !isnothing(kinds) && !(e.kind in kinds) && continue
                    if eid in est.event_created_at
                        created_at = est.event_created_at[eid]
                        push!(bms, (collect(eid.hash), created_at))
                    end
                end
            end
            i = 0
            for (eid, created_at) in sort(bms; by=r->-r[2])
                length(posts) >= limit && break
                if since <= created_at <= until
                    i >= offset && push!(posts, (eid, created_at))
                    i += 1
                end
            end
        end
    elseif notes == :user_media_thumbnails
        if !isnothing(DAG_OUTPUTS_DB[])
            for (eid, created_at) in Postgres.pex(:p1timelimit, "
                SELECT
                    DISTINCT events.id, 
                    events.created_at
                FROM
                    prod.events,
                    prod.event_media
                WHERE 
                    events.pubkey = \$1 AND 
                    events.kind = $(Int(Nostr.TEXT_NOTE)) AND 
                    events.id = event_media.event_id AND
                    events.created_at >= \$2 and events.created_at <= \$3
                ORDER BY
                    events.created_at DESC
                LIMIT \$4 OFFSET \$5
                ", [pubkey, since, until, limit, offset])
                push!(posts, (eid, created_at))
            end
        end
    else
        pubkeys = 
        if     notes == :follows;  follows(est, pubkey)
        elseif notes == :authored; [pubkey]
        else;                      error("unsupported type of notes")
        end
        pubkeys = sort(pubkeys)

        use_slow_method = Ref(false)

        if order == :asc; use_slow_method[] = true; end

        if 1==1 && !use_slow_method[] && (ng_any_user[] || user_pubkey in ng_whitelist)
            ca = 1
            rwlock, ss = est.dyn[:recent_events]
            lock(read_lock(rwlock)) do
                if isempty(ss) || (let t = last(ss)[ca]; until < t; end)
                    use_slow_method[] = true
                else
                    pubkeyss = Set(pubkeys)
                    tdur1 = @elapsed begin
                        try
                            st1 = searchsortedfirst(ss, (until, until, false, Nostr.PubKeyId(zeros(UInt8, 32)), Nostr.EventId(zeros(UInt8, 32))))
                            st2 = DataStructures.findkey(ss, last(ss))
                            done = false
                            for e in DataStructures.inclusive(ss, st1, st2)
                                events_scanned += 1
                                events_scanned >= 100000 && break
                                if e[ca] < since; done = true; break; end
                                !include_replies && e[3] && continue
                                if e[4] in pubkeyss
                                    push!(posts, (e[5].hash, e[ca]))
                                    if length(posts) >= limit; done = true; break; end
                                end
                            end
                            if !done; use_slow_method[] = true; end
                        catch _ Utils.print_exceptions() end
                    end
                end
            end
        end

        if use_slow_method[]
            tdur2 = @elapsed @threads for p in pubkeys
                time_exceeded() && break
                yield()
                append!(posts, map(Tuple, DB.exe(est.pubkey_events, 
                                                 "select event_id, created_at from kv 
                                                 where pubkey = ? and created_at >= ? and created_at <= ? and (is_reply = 0 or is_reply = ?)
                                                 order by created_at $order limit ? offset ?",
                                                 p, since, until, Int(include_replies), limit, offset)))
            end
        end
    end

    posts = first(sort(posts.wrapped, by=p->p[2], rev=order!=:asc), limit)

    eids = [Nostr.EventId(eid) for (eid, _) in posts]
    tdur3 = @elapsed (res = response_messages_for_posts(est, eids; user_pubkey, time_exceeded))

    vcat(res, range(posts, :created_at))
end

function thread_view(est::DB.CacheStorage; event_id, user_pubkey=nothing, kwargs...)
    event_id = cast(event_id, Nostr.EventId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    est.auto_fetch_missing_events && DB.fetch_event(est, event_id)

    res = OrderedSet()

    hidden = is_hidden(est, user_pubkey, :content, event_id) || ext_is_hidden(est, event_id) || event_id in est.deleted_events

    hidden || union!(res, thread_view_replies(est; event_id, user_pubkey, kwargs...))
    union!(res, thread_view_parents(est; event_id, user_pubkey))

    collect(res)
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
    posts = first(sort(posts, by=p->-p[2]), limit)
    
    reids = [reid for (reid, _) in posts]
    [response_messages_for_posts(est, reids; user_pubkey); range(posts, :created_at)]
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

import MbedTLS
user_infos_cache_periodic = Throttle(; period=60.0)
user_infos_cache = Dict{NTuple{20, UInt8}, Any}() |> ThreadSafe

function user_infos(est::DB.CacheStorage; pubkeys::Vector)
    return user_infos_1(est; pubkeys)

    user_infos_cache_periodic() do
        empty!(user_infos_cache)
    end
    bio = IOBuffer()
    for pk in pubkeys
        write(bio, castmaybe(pk, Nostr.PubKeyId).pk)
    end
    k = NTuple{20, UInt8}(MbedTLS.digest(MbedTLS.MD_SHA1, take!(bio)))
    lock(user_infos_cache) do user_infos_cache
        get!(user_infos_cache, k) do
            user_infos_1(est; pubkeys)
        end
    end
end

function user_infos_1(est::DB.CacheStorage; pubkeys::Vector)
    pubkeys = [pk isa Nostr.PubKeyId ? pk : Nostr.PubKeyId(pk) for pk in pubkeys]

    res_meta_data = Dict() |> ThreadSafe
    @threads for pk in pubkeys 
    # for pk in pubkeys 
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

function mutual_follows(est::DB.CacheStorage; pubkey, user_pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    pks = Set{Nostr.PubKeyId}()
    for pk in follows(est, user_pubkey)
        if !isempty(DB.exe(est.pubkey_followers, 
                           DB.@sql("select 1 from kv 
                                   where pubkey = ?1 and follower_pubkey = ?2
                                   limit 1"), pubkey, pk))
            push!(pks, pk)
        end
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
        # for eid in event_ids 
            eid in est.events && push!(res, est.events[eid])
        end
        sort(res.wrapped; by=e->e.created_at)
    else
        response_messages_for_posts(est, event_ids; user_pubkey)
    end
end

blocked_user_profiles = Set{Nostr.PubKeyId}() |> ThreadSafe

function user_profile(est::DB.CacheStorage; pubkey, user_pubkey=nothing)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    pubkey in blocked_user_profiles && return []

    est.auto_fetch_missing_events && DB.fetch_user_metadata(est, pubkey)

    res = [] |> ThreadSafe

    pubkey in est.meta_data && push!(res, est.events[est.meta_data[pubkey]])

    note_count  = DB.exe(est.pubkey_events, DB.@sql("select count(1) from kv where pubkey = ? and is_reply = false"), pubkey)[1][1]
    reply_count = DB.exe(est.pubkey_events, DB.@sql("select count(1) from kv where pubkey = ? and is_reply = true"), pubkey)[1][1]
    long_form_note_count = DB.exe(est.dyn[:parametrized_replaceable_events], DB.@sql("select count(1) from parametrized_replaceable_events where pubkey = ? and kind = ?"), pubkey, Int(Nostr.LONG_FORM_CONTENT))[1][1]
    # subscribable = any(e.kind == 37001 for e in creator_paid_tiers(est; pubkey))

    time_joined_r = DB.exe(est.pubkey_events, DB.@sql("select created_at from kv
                                                       where pubkey = ?
                                                       order by created_at asc limit 1"), pubkey)

    time_joined = isempty(time_joined_r) ? nothing : time_joined_r[1][1];

    relay_count = length(Set([t[2] for t in get_user_relays(est; pubkey)[end].tags]))

    push!(res, (;
                kind=Int(USER_PROFILE),
                content=JSON.json((;
                                   pubkey,
                                   follows_count=length(follows(est, pubkey)),
                                   followers_count=get(est.pubkey_followers_cnt, pubkey, 0),
                                   note_count,
                                   long_form_note_count,
                                   reply_count,
                                   time_joined,
                                   relay_count,
                                   # subscribable,
                                   ext_user_profile(est, pubkey)...,
                                  ))))

    append!(res, ext_user_profile_media(est, pubkey))

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

function get_directmsg_count_2(est::DB.CacheStorage; receiver, sender=nothing)
    cnt = get_directmsg_count(est; receiver, sender)[1].cnt
    [(; kind=Int(DIRECTMSG_COUNT_2), content=JSON.json(cnt))]
end

function get_directmsg_contacts(
        est::DB.CacheStorage; 
        user_pubkey, relation::Union{String,Symbol}=:any,
        limit=10000, offset=0, since=0, until=trunc(Int, time()),
    )
    limit = min(10000, limit)
    user_pubkey = cast(user_pubkey, Nostr.PubKeyId)
    relation = cast(relation, Symbol)

    fs = Set(follows(est, user_pubkey))

    d = Dict{Nostr.PubKeyId, Dict{Symbol, Any}}()
    for (peer, cnt, latest_at, latest_event_id) in 
        vcat(DB.exe(est.pubkey_directmsgs_cnt,
                    DB.@sql("select sender, cnt, latest_at, latest_event_id
                            from pubkey_directmsgs_cnt
                            where receiver is ?1 and sender is not null and latest_at >= ?2 and latest_at <= ?3
                            order by latest_at desc limit ?4"), user_pubkey, since, until, 300),
             DB.exe(est.pubkey_directmsgs_cnt,
                    DB.@sql("select receiver, 0, latest_at, latest_event_id
                            from pubkey_directmsgs_cnt
                            where sender is ?1 and latest_at >= ?2 and latest_at <= ?3
                            order by latest_at desc limit ?4"), user_pubkey, since, until, 300))

        peer = Nostr.PubKeyId(peer)

        is_hidden(est, user_pubkey, :content, peer) && continue

        if relation != :any
            if     relation == :follows; peer in fs || continue
            elseif relation == :other;   peer in fs && continue
            else; error("invalid relation")
            end
        end
        
        latest_event_id = Nostr.EventId(latest_event_id)
        if !haskey(d, peer)
            d[peer] = Dict([:cnt=>cnt, :latest_at=>latest_at, :latest_event_id=>latest_event_id])
        end
        if d[peer][:latest_at] < latest_at
            d[peer][:latest_at] = latest_at
            d[peer][:latest_event_id] = latest_event_id
        end
        if d[peer][:cnt] < cnt
            d[peer][:cnt] = cnt
        end
    end

    d = sort(collect(d); by=x->-x[2][:latest_at])[offset+1:min(length(d), offset+limit)]

    evts = []
    mds = []
    mdextra = []
    for (peer, p) in d
        if p[:latest_event_id] in est.events
            push!(evts, est.events[p[:latest_event_id]])
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

    d = [Nostr.hex(peer)=>p for (peer, p) in d]

    [(; kind=Int(DIRECTMSG_COUNTS), content=JSON.json(Dict(d))), 
     evts..., mds..., mdextra..., 
     range(d, :latest_at; by=x->x[2][:latest_at])]
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

    [res..., range(msgs, :created_at)...]
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

function parametrized_replaceable_events_extended_response(est::DB.CacheStorage, eids::Vector{Nostr.EventId})
    res = []
    res_meta_data = Dict()
    for eid in eids
        eid in est.events || continue
        e = est.events[eid]
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
    res
end

function parameterized_replaceable_list(est::DB.CacheStorage; pubkey, identifier::String, extended_response=true)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    eids = [Nostr.EventId(eid) for (eid,) in DB.exe(est.parameterized_replaceable_list, 
                                                    DB.@sql("select event_id from parameterized_replaceable_list where pubkey = ?1 and identifier = ?2"), 
                                                    pubkey, identifier)]

    res = [est.events[eid] for eid in eids]
    extended_response && append!(res, parametrized_replaceable_events_extended_response(est, eids))
    res
end

function parametrized_replaceable_event(est::DB.CacheStorage; pubkey, kind::Int, identifier::String, extended_response=true, user_pubkey=nothing)
    pubkey = cast(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    eids = [Nostr.EventId(eid) 
            for (eid,) in DB.exec(est.dyn[:parametrized_replaceable_events], 
                                  DB.@sql("select event_id from parametrized_replaceable_events 
                                          where pubkey = ?1 and kind = ?2 and identifier = ?3"), 
                                  (pubkey, kind, identifier))]
    res = OrderedSet{Any}([est.events[eid] for eid in eids])
    if extended_response
        union!(res, parametrized_replaceable_events_extended_response(est, eids))
        union!(res, response_messages_for_posts(est, eids; user_pubkey))
    end
    collect(res)
end

function parametrized_replaceable_events(est::DB.CacheStorage; events::Vector, extended_response=true)
    res = []
    for r in events
        append!(res, parametrized_replaceable_event(est; pubkey=r["pubkey"], kind=r["kind"], identifier=r["identifier"], extended_response))
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
        e = Nostr.Event(e)
        try
            msg = JSON.json([time(), nothing, ["EVENT", "", e]])
            if DB.import_msg_into_storage(msg, est)
                cnt[] += 1
            end
            ext_import_event(est, e)
        catch _ 
            errcnt[] += 1
        end
    end
    [(; kind=Int(EVENT_IMPORT_STATUS), content=JSON.json((; imported=cnt[], errors=errcnt[])))]
end

function response_messages_for_zaps(est, zaps; kinds=nothing, order_by=:created_at, user_pubkey=nothing)
    res_meta_data = Dict()
    res = []
    for (zap_receipt_id, created_at, event_id, sender, receiver, amount_sats) in zaps
        hidden = false
        try
            sender = Nostr.PubKeyId(sender)
            receiver = Nostr.PubKeyId(receiver)
            hidden |= !isnothing(user_pubkey) && is_hidden(est, user_pubkey, :content, sender)
            hidden |= is_hidden(est, receiver, :content, sender)
        catch _
            Utils.print_exceptions()
        end
        hidden && continue

        for pk in [sender, receiver]
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
    # for p in pks
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

    isnothing(until) && (until = 100_000_000_000)
    zaps = map(Tuple, DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                                        where receiver = ? and amount_sats >= ? and amount_sats <= ?
                                                        order by amount_sats desc limit ? offset ?"),
                              (receiver, since, until, limit, offset)))

    zaps = sort(zaps, by=z->-z[6])[1:min(limit, length(zaps))]

    response_messages_for_zaps(est, zaps; order_by=:amount_sats)
end

function event_zaps_by_satszapped(
        est::DB.CacheStorage;
        event_id=nothing,
        pubkey=nothing, identifier=nothing,
        limit::Int=20, since::Int=0, until=nothing, offset::Int=0,
        user_pubkey=nothing,
    )
    limit <= 1000 || error("limit too big")
    event_id = castmaybe(event_id, Nostr.EventId)
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    isnothing(until) && (until = 100_000_000_000)

    r = if !isnothing(event_id)
        DB.exec(est.zap_receipts, DB.@sql("select zap_receipt_id, created_at, event_id, sender, receiver, amount_sats from zap_receipts 
                                          where event_id = ? and amount_sats >= ? and amount_sats <= ?
                                          order by amount_sats desc limit ? offset ?"),
                (event_id, since, until, limit, offset))

    elseif !isnothing(pubkey) && !isnothing(identifier) 
        Postgres.pex(DAG_OUTPUTS_DB[], pgparams() do P "
                      SELECT
                          zap_receipts.eid        as zap_receipt_id,
                          zap_receipts.created_at as created_at,
                          zap_receipts.target_eid as event_id,
                          zap_receipts.sender     as sender,
                          zap_receipts.receiver   as receiver,
                          zap_receipts.satszapped as amount_sats 
                      FROM
                          prod.reads_versions,
                          prod.zap_receipts
                      WHERE 
                          reads_versions.pubkey = $(@P pubkey) AND 
                          reads_versions.identifier = $(@P identifier) AND 
                          reads_versions.eid = zap_receipts.target_eid AND 
                          zap_receipts.satszapped >= $(@P since) AND 
                          zap_receipts.satszapped <= $(@P until)
                      ORDER BY
                          zap_receipts.satszapped DESC
                      LIMIT $(@P limit) OFFSET $(@P offset)
                  " end...)
    else
        []
    end

    zaps = map(Tuple, r)
    zaps = first(sort(zaps, by=z->-z[6]), limit)

    response_messages_for_zaps(est, zaps; order_by=:amount_sats, user_pubkey)
end

function server_name(est::DB.CacheStorage)
    [(; content=JSON.json(Int(Sockets.getipaddr().host >> 8 & 0xff)-10))]
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

function nostr_stats(est::DB.CacheStorage)
    res = [r[1:4] for r in DB.dyn_select(est, :daily_stats, :active_humans)]
    [(; kind=Int(NOSTR_STATS), content=JSON.json(res))]
end

function is_hidden_by_content_moderation(est::DB.CacheStorage; user_pubkey=nothing, scope=:content, pubkeys=[], event_ids=[])
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)
    scope = cast(scope, Symbol)
    pubkeys = [cast(pk, Nostr.PubKeyId) for pk in pubkeys]
    event_ids = [cast(eid, Nostr.EventId) for eid in event_ids]
    res = (; 
           pubkeys=Dict([Nostr.hex(pk)=>is_hidden(est, user_pubkey, scope, pk) for pk in pubkeys]),
           event_ids=Dict([Nostr.hex(eid)=>is_hidden(est, user_pubkey, scope, eid) for eid in event_ids]))
    [(; kind=Int(IS_HIDDEN_BY_CONTENT_MODERATION), content=JSON.json(res))]
end

function user_of_ln_address(est::DB.CacheStorage; ln_address::String)
    if !isempty(local r = DB.exec(est.dyn[:pubkey_ln_address], 
                                  DB.@sql("select pubkey from pubkey_ln_address where ln_address = ?1"), 
                                  (ln_address,)))
        pubkey = Nostr.PubKeyId(r[1][1])
        [(; kind=Int(USER_PUBKEY), content=JSON.json((; pubkey))), user_infos(est; pubkeys=[pubkey])...]
    else
        []
    end
end

function get_user_relays(est::DB.CacheStorage; pubkey)
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    res = []
    relays = Set()
    if haskey(est.dyn[:relay_list_metadata], pubkey)
        eid = est.dyn[:relay_list_metadata][pubkey]
        if haskey(est.events, eid)
            e = est.events[eid]
            # push!(res, e)
            for t in e.tags
                if length(t.fields) >= 2 && t.fields[1] == "r"
                    push!(relays, ["r", t.fields[2:end]...])
                end
            end
        end
    end
    if isempty(relays) && haskey(est.contact_lists, pubkey)
        eid = est.contact_lists[pubkey]
        if haskey(est.events, eid)
            e = est.events[eid]
            # push!(res, e)
            d = try JSON.parse(e.content) catch _ Dict() end
            for (url, dd) in d
                for (k, v) in dd
                    v && !isempty(url) && push!(relays, ["r", url, k])
                end
            end
        end
    end
    push!(res, (; kind=Int(USER_RELAYS), tags=sort(collect(relays))))
    res
end

function get_bookmarks(est::DB.CacheStorage; pubkey)
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    res = []
    if haskey(est.dyn[:bookmarks], pubkey)
        eid = est.dyn[:bookmarks][pubkey]
        if haskey(est.events, eid)
            e = est.events[eid]
            push!(res, e)
        end
    end
    res
end

function get_highlights(
        est::DB.CacheStorage;
        event_id=nothing,
        pubkey=nothing, identifier=nothing, kind=nothing,
        limit::Int=20, since::Int=0, until=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
    )
    event_id = castmaybe(event_id, Nostr.EventId)
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    event_from_row(r) = Nostr.Event(r[1], r[2], r[3], r[4], [Nostr.TagAny(t) for t in r[5]], ismissing(r[6]) ? "" : r[6], r[7])

    evts = if !isnothing(event_id)
        map(event_from_row, Postgres.pex(DAG_OUTPUTS_DB[], pgparams() do P "
                             SELECT
                                 events.*
                             FROM
                                 prod.basic_tags,
                                 prod.events
                             WHERE 
                                 basic_tags.kind = 9802 AND
                                 basic_tags.tag = 'e' AND
                                 basic_tags.arg1 = $(@P event_id)
                                 basic_tags.created_at >= $(@P since) AND 
                                 basic_tags.created_at <= $(@P until) AND
                                 basic_tags.eid = events.id
                             ORDER BY
                                 basic_tags.created_at DESC
                             LIMIT $(@P limit) OFFSET $(@P offset)
                         " end...))

    elseif !isnothing(kind) && !isnothing(pubkey) && !isnothing(identifier) 
        map(event_from_row, Postgres.pex(DAG_OUTPUTS_DB[], pgparams() do P "
                             SELECT
                                 events.*
                             FROM
                                 prod.a_tags,
                                 prod.events
                             WHERE 
                                 a_tags.kind = 9802 AND 
                                 a_tags.ref_kind = $(@P kind) AND 
                                 a_tags.ref_pubkey = $(@P pubkey) AND 
                                 a_tags.ref_identifier = $(@P identifier) AND 
                                 a_tags.created_at >= $(@P since) AND 
                                 a_tags.created_at <= $(@P until) AND
                                 a_tags.eid = events.id
                             ORDER BY
                                 a_tags.created_at DESC
                             LIMIT $(@P limit) OFFSET $(@P offset)
                         " end...))
    else
        []
    end

    evts = [e for e in evts
            if !(is_hidden(est, user_pubkey, :content, e.id) || 
                 ext_is_hidden(est, e.id) || 
                 e.id in est.deleted_events)]

    res = []
    res_meta_data = Dict()
    for e in evts

        push!(res, e)
        pk = e.pubkey
        if !haskey(res_meta_data, pk) && pk in est.meta_data
            res_meta_data[pk] = est.events[est.meta_data[pk]]
        end
    end

    res_meta_data = collect(values(res_meta_data))
    append!(res, res_meta_data)
    append!(res, user_scores(est, res_meta_data))
    ext_user_infos(est, res, res_meta_data)

    vcat(res, range([(e.id, e.created_at) for e in evts], :created_at))
end

function pgparams()
    r = (; params=[], wheres=[])
    (; r...,
     clear=function()
         empty!(r.params)
         empty!(r.wheres)
         nothing
     end,
     P=function(v)
         push!(r.params, v)
         "\$$(length(r.params))"
     end,
     W=function(w)
         push!(r.wheres, w)
         nothing
     end,
     fmtwheres=()->join([" and "*s for s in r.wheres]),
     )
end

function pgparams(body)
    p = pgparams()
    body(p.P), p.params
end

macro P(arg)
    :($(esc(:P))($(esc(arg))))
end

function long_form_content_feed(
        est::DB.CacheStorage;
        pubkey=nothing, notes::Union{Symbol,String}=:follows,
        topic=nothing,
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
        time_exceeded=()->false,
    )
    limit <= 1000 || error("limit too big")
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)
    notes = Symbol(notes)

    topic_where(P) = isnothing(topic) ? "" : "and topics @@ plainto_tsquery('simple', $(@P replace(topic, ' '=>'-')))"

    posts = [] |> ThreadSafe
    if pubkey isa Nothing
        for r in Postgres.pex(DAG_OUTPUTS_DB[], 
                              pgparams() do P "
                                  select latest_eid, published_at
                                  from prod.reads
                                  where 
                                      published_at >= $(@P since) and published_at <= $(@P until)
                                      $(topic_where(P))
                                  order by published_at desc limit $(@P limit) offset $(@P offset)
                              " end...)
            push!(posts, r)
        end
    else
        pubkeys = 
        if     notes == :follows;  follows(est, pubkey)
        elseif notes == :authored; [pubkey]
        else;                      error("unsupported type of notes")
        end
        pubkeys = sort(pubkeys)

        for pk in pubkeys
            time_exceeded() && break
            for r in Postgres.pex(DAG_OUTPUTS_DB[],
                                  pgparams() do P "
                                      select latest_eid, published_at
                                      from prod.reads
                                      where 
                                          published_at >= $(@P since) and published_at <= $(@P until)
                                          and pubkey = $(@P pk) 
                                          $(topic_where(P))
                                      order by published_at desc limit $(@P limit) offset $(@P offset)
                                  " end...)
                push!(posts, r)
            end
        end
    end

    posts = first(sort(posts.wrapped, by=p->-p[2]), limit)

    eids = [Nostr.EventId(eid) for (eid, _) in posts]
    res = response_messages_for_posts_2(est, eids; user_pubkey, time_exceeded)

    append!(res, parametrized_replaceable_events_extended_response(est, eids))

    vcat(res, range(posts, :created_at))
end

function long_form_content_thread_view(
        est::DB.CacheStorage; 
        pubkey, kind::Int, identifier::String, 
        limit::Int=20, since::Int=0, until::Int=trunc(Int, time()), offset::Int=0,
        user_pubkey=nothing,
    )
    limit <= 1000 || error("limit too big")
    pubkey = castmaybe(pubkey, Nostr.PubKeyId)
    user_pubkey = castmaybe(user_pubkey, Nostr.PubKeyId)

    event_id = parametrized_replaceable_event(est; pubkey, kind, identifier, extended_response=false)[1].id

    res = OrderedSet()

    hidden = is_hidden(est, user_pubkey, :content, event_id) || ext_is_hidden(est, event_id) || event_id in est.deleted_events
    hidden && return []

    union!(res, thread_view_parents(est; event_id, user_pubkey))

    posts = Tuple{Nostr.EventId, Int}[]

    for (reid, created_at) in 
        Postgres.pex(DAG_OUTPUTS_DB[], pgparams() do P "
                         WITH a AS (
                             SELECT
                                 events.id,
                                 events.created_at
                             FROM
                                 prod.reads_versions,
                                 prod.event_replies,
                                 prod.events
                             WHERE 
                                 reads_versions.pubkey = $(@P pubkey) AND 
                                 reads_versions.identifier = $(@P identifier) AND 
                                 reads_versions.eid = event_replies.event_id AND 
                                 event_replies.reply_event_id = events.id AND 
                                 events.created_at >= $(@P since) AND 
                                 events.created_at <= $(@P until)
                         ), b AS (
                             SELECT
                                 a_tags.eid,
                                 a_tags.created_at
                             FROM
                                 prod.a_tags
                             WHERE 
                                 a_tags.kind = $(@P Int(Nostr.TEXT_NOTE)) AND 
                                 a_tags.ref_kind = $(@P Int(Nostr.LONG_FORM_CONTENT)) AND 
                                 a_tags.ref_pubkey = $(@P pubkey) AND 
                                 a_tags.ref_identifier = $(@P identifier) AND 
                                 a_tags.created_at >= $(@P since) AND 
                                 a_tags.created_at <= $(@P until)
                         )
                         (SELECT * FROM a) UNION (SELECT * FROM b)
                         ORDER BY
                             created_at DESC
                         LIMIT $(@P limit) OFFSET $(@P offset)
                     " end...)
        push!(posts, (Nostr.EventId(reid), created_at))
    end
    
    reids = [reid for (reid, _) in posts]
    union!(res, [response_messages_for_posts(est, reids; user_pubkey); range(posts, :created_at)])

    collect(res)
end

RECOMMENDED_READS_FILE = Ref("recommended-reads.json")

function get_recommended_reads(est::DB.CacheStorage)
    [(; kind=Int(RECOMMENDED_READS), 
      content=JSON.json(try JSON.parse(read(RECOMMENDED_READS_FILE[], String))
                        catch _; (;) end))]
end

READS_TOPICS_FILE = Ref("reads-topics.json")

function get_reads_topics(est::DB.CacheStorage)
    [(; kind=Int(READS_TOPICS), 
      content=JSON.json(try JSON.parse(read(READS_TOPICS_FILE[], String))
                        catch _; (;) end))]
end

FEATURED_AUTHORS_FILE = Ref("featured-authors.json")

function get_featured_authors(est::DB.CacheStorage)
    [(; kind=Int(FEATURED_AUTHORS), 
      content=JSON.json(try JSON.parse(read(FEATURED_AUTHORS_FILE[], String))
                        catch _; (;) end))]
end

function creator_paid_tiers(est::DB.CacheStorage; pubkey)
    pubkey = cast(pubkey, Nostr.PubKeyId)

    res = []

    catch_exception(:creator_paid_tiers, (; pubkey)) do
        event_from_row(r) = Nostr.Event(r[1], r[2], r[3], r[4], [Nostr.TagAny(t) for t in r[5]], ismissing(r[6]) ? "" : r[6], r[7])

        for r in Main.Postgres.pex(:p1, "select * from event where kind = 17000 and pubkey = ?1 order by created_at desc limit 1", (pubkey,))
            e = event_from_row(r)
            push!(res, e)
            for t in e.tags
                if length(t.fields) >= 2 && t.fields[1] == "e"
                    eid = Nostr.EventId(t.fields[2])
                    for r2 in Main.Postgres.pex(:p1, "select * from event where id = ?1 limit 1", (eid,))
                        push!(res, event_from_row(r2))
                    end
                end
            end
        end
    end

    res
end

function ext_user_infos(est::DB.CacheStorage, res, res_meta_data) end
function ext_user_profile(est::DB.CacheStorage, pubkey); (;); end
function ext_user_profile_media(est::DB.CacheStorage, pubkey); []; end
function ext_is_hidden(est::DB.CacheStorage, eid::Nostr.EventId); false; end
function ext_is_hidden_by_group(est::DB.CacheStorage, user_pubkey, scope::Symbol, pubkey::Nostr.PubKeyId); false; end
function ext_is_hidden_by_group(est::DB.CacheStorage, user_pubkey, scope::Symbol, eid::Nostr.EventId); false; end
function ext_event_response(est::DB.CacheStorage, e::Nostr.Event); []; end
function ext_user_get_settings(est::DB.CacheStorage, pubkey); end
function ext_invalidate_cached_content_moderation(est::DB.CacheStorage, user_pubkey::Union{Nothing,Nostr.PubKeyId}); end
function ext_import_event(est::DB.CacheStorage, e::Nostr.Event) end
function ext_long_form_event_stats(est::DB.CacheStorage, eid::Nostr.EventId); []; end

end
