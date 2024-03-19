module PerfTestDelegation

import HTTP
using HTTP.WebSockets

import ..Utils
using ..Utils: ThreadSafe

mutable struct Delegation
    ws::Union{Nothing, ThreadSafe{WebSocket}}
    task::Union{Nothing, Task}
end

TARGET_HOST = Ref{Any}(nothing) |> ThreadSafe

FUNCS = Set{Symbol}([
                     # app.jl:
                     :feed,
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
                     # :reset_directmsg_count,
                     # :reset_directmsg_counts,
                     :get_directmsgs,
                     :mutelist,
                     :mutelists,
                     :allowlist,
                     :parameterized_replaceable_list,
                     :search_filterlist,
                     # :import_events,
                     :zaps_feed,
                     :user_zaps,
                     :user_zaps_by_satszapped,
                     :server_name,
                     :nostr_stats,
                     :is_hidden_by_content_moderation,
                     :user_of_ln_address,
                     :get_user_relays,

                     # App.jl:
                     :explore_legend_counts,
                     :explore,
                         :explore_global_trending_24h,
                         :explore_global_mostzapped_4h,
                     :scored,
                     :scored_users,
                         :scored_users_24h,
                     # :set_app_settings,
                     # :get_app_settings,
                     # :get_app_settings_2,
                     :get_default_app_settings,
                     :get_default_relays,
                     :get_recommended_users,
                     :get_suggested_users,
                     :get_app_releases,
                     :user_profile_scored_content,
                     :search,
                     :relays,
                     :get_notifications,
                     # :set_notifications_seen,
                     :get_notifications_seen,
                     :user_search,
                     :feed_directive,
                     :trending_hashtags,
                         :trending_hashtags_4h,
                         :trending_hashtags_7d,
                     :trending_images,
                         :trending_images_4h,
                     # :upload,
                     # :upload_chunk,
                     # :upload_complete,
                     # :upload_cancel,
                     # :report_user,
                     # :report_note,
                     :get_filterlist,
                     :check_filterlist,
                     # :broadcast_reply,
                    ]) |> ThreadSafe

delegations = Dict{Base.UUID, Delegation}() |> ThreadSafe
received_msg_cnt = Ref(0) |> ThreadSafe

enabled() = !isnothing(TARGET_HOST[])

function start_replication(ws_id::Base.UUID)
    if enabled()
        lock(delegations) do delegations
            p = delegations[ws_id] = Delegation(nothing, nothing)
            p.task = @async replicator(ws_id)
            println("delegations: created $ws_id")
        end
    end
end

function stop_replication(ws_id::Base.UUID)
    ws = Ref{Any}(nothing)
    lock(delegations) do delegations
        if haskey(delegations, ws_id)
            p = delegations[ws_id]
            ws[] = p.ws
        end
    end
    if !isnothing(ws[])
        lock(ws[]) do ws
            try close(ws) catch _ end
        end
    end
end

function stop_all_replication()
    wss = []
    lock(delegations) do delegations
        if haskey(delegations, ws_id)
            p = delegations[ws_id]
            if !isnothing(p.ws)
                push!(wss, p.ws)
            end
        end
    end
    for ws in wss
        lock(ws[]) do ws
            try close(ws) catch _ end
        end
    end
end

function send_msg(ws_id::Base.UUID, msg)
    ws = Ref{Any}(nothing)
    lock(delegations) do delegations
        if haskey(delegations, ws_id)
            p = delegations[ws_id]
            ws[] = p.ws
        end
    end
    if !isnothing(ws[])
        lock(ws[]) do ws
            @async try HTTP.WebSockets.send(ws, msg) catch _ end
        end
    end
end

function replicator(ws_id::Base.UUID)
    addr, port = TARGET_HOST[]
    HTTP.WebSockets.open("ws://$addr:$port"; connect_timeout=10, readtimeout=60) do ws
        lock(delegations) do delegations
            delegations[ws_id].ws = ws |> ThreadSafe
        end
        try
            for msg in ws
                lock(received_msg_cnt) do cnt; cnt[] += 1; end
            end
        finally
            close(ws)
            lock(delegations) do delegations
                delete!(delegations, ws_id)
                println("delegations: deleted $ws_id")
            end
        end
    end
end

end
