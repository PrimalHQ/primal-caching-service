@show target_dir = ARGS[1]
@show input_dirs = ARGS[2:end]

# @assert isdir(target_dir)
@assert all([isdir(d) for d in input_dirs])

for fn in [
           "utils.jl",
           "nostr.jl",
           # "bech32.jl",
           # "fetching.jl",
           "db.jl",
          ]
    fn = "src/$fn"
    println(fn, " -> ", include(fn))
end

# DB.PRINT_EXCEPTIONS[] = true

deduped_storage = DB.DeduplicatedEventStorage(directory=target_dir;
                                              dbargs=(; ndbs=256, journal_mode="WAL"))

DB.import_to_storage(deduped_storage, input_dirs;
                     running=Utils.PressEnterToStop())
