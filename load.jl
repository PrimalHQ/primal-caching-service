for fn in [
           "utils.jl",
           "nostr.jl",
           "fetching.jl",
           "db.jl",
           "metrics_logger.jl",
           "app.jl",
           "cache_server_handlers.jl",
           "cache_server.jl",
          ]
    fn = "src/$fn"
    println(fn, " -> ", include(fn))
end

STORAGEPATH   = get(ENV, "PRIMALSERVER_STORAGE_PATH", "$(pwd())/var")
PROXY         = get(ENV, "PRIMALSERVER_PROXY", nothing)
FETCHER_SINCE = try parse(Int, ENV["PRIMALSERVER_FETCHER_SINCE"]) catch _ trunc(Int, time()) end
NODEIDX       = parse(Int, get(ENV, "PRIMALSERVER_NODE_IDX", "1"))
RELAYS_FILE   = get(ENV, "PRIMALSERVER_RELAYS", nothing)

#DB.PRINT_EXCEPTIONS[] = true

gctask = Utils.GCTask()

auto_fetch_missing_events = get(ENV, "PRIMALSERVER_AUTO_FETCH_MISSING_EVENTS", nothing) == "1"

cache_storage = DB.CacheStorage(;
                                directory="$(STORAGEPATH)/primalnode$(NODEIDX)/cache",
                                dbargs=(; ndbs=1, journal_mode="WAL"), 
                                auto_fetch_missing_events)

Fetching.message_processors[:cache_storage] = (msg)->DB.import_msg_into_storage(msg, cache_storage)

Fetching.EVENTS_DATA_DIR[] = "$(STORAGEPATH)/primalnode$(NODEIDX)/fetcher"
Fetching.PROXY_URI[] = PROXY
if isnothing(RELAYS_FILE)
    Fetching.load_relays()
else
    union!(Fetching.relays, [r for r in readlines(RELAYS_FILE) if !isempty(r)])
end

CacheServer.HOST[] = get(ENV, "PRIMALSERVER_HOST", "0.0.0.0")
CacheServer.PORT[] = 8800+NODEIDX

