DB.init(cache_storage)

Fetching.start(; since=FETCHER_SINCE)

MetricsLogger.start("$STORAGEPATH/metrics-cache$(NODEIDX).log")

CacheServerHandlers.netstats_start()

CacheServer.start()


