module DB

using .Threads: @threads

using ..Utils: ThreadSafe

registered_queries = Vector{String}()
sql(query::String) = query
# function sql(query::String)
#     i = findfirst(s->s == query, registered_queries)
#     if isnothing(i)
#         push!(registered_queries, query)
#         length(registered_queries)
#     else
#         i
#     end
# end
macro sql(query::String)
    sql(query)
end
macro sql(query::Expr)
    esc(query)
end

abstract type DBConn end

struct DBConversionFuncs
    to_sql::Function
    from_sql::Function
end

abstract type ShardedDBDict{K, V} end

function shard(ssd::ShardedDBDict{K, V}, k::K)::Int where {K, V}
    (ssd.hashfunc(k) % length(ssd.dbconns)) + 1
end

function Base.in(k::K, ssd::ShardedDBDict{K, V})::Bool where {K, V}
    length(exe(ssd.dbconns[shard(ssd, k)], @sql("select 1 from $(ssd.table) where $(ssd.keycolumn) = ?1 limit 1"), (ssd.keyfuncs.to_sql(k),))) > 0
end
function Base.haskey(ssd::ShardedDBDict{K, V}, k::K)::Bool where {K, V}
    k in ssd
end
function Base.get(ssd::ShardedDBDict{K, V}, k::K, default::V)::V where {K, V}
    k in ssd ? ssd[k] : default
end
function Base.getindex(ssd::ShardedDBDict{K, V}, k::K)::V where {K, V}
    r = exe(ssd.dbconns[shard(ssd, k)],
            @sql("select $(ssd.valuecolumn) from $(ssd.table) where $(ssd.keycolumn) = ?1"),
            (ssd.keyfuncs.to_sql(k),))
    isempty(r) && throw(KeyError(k))
    r[1][1] |> ssd.valuefuncs.from_sql
end
function Base.setindex!(ssd::ShardedDBDict{K, V}, v::V, k::K)::V where {K, V}
    exe(ssd.dbconns[shard(ssd, k)],
        @sql("replace into $(ssd.table) ($(ssd.keycolumn), $(ssd.valuecolumn)) values (?1, ?2)"),
        (ssd.keyfuncs.to_sql(k), ssd.valuefuncs.to_sql(v)))
    v
end
function Base.push!(ssd::ShardedDBDict{K, Bool}, k::K)::ShardedDBDict{K, Bool} where {K}
    ssd[k] = true
    ssd
end
function Base.delete!(ssd::ShardedDBDict{K, V}, k::K)::ShardedDBDict{K, V} where {K, V}
    exe(ssd.dbconns[shard(ssd, k)], @sql("delete from $(ssd.table) where $(ssd.keycolumn) = ?1"), (ssd.keyfuncs.to_sql(k),))
    ssd
end
function db_args_mapped(ty::Type, args)
    map(args) do a
        db_conversion_funcs(ty, typeof(a)).to_sql(a)
    end
end
function exe(ssd::ShardedDBDict{K, V}, query::Union{Int,String}, k::K, rest...) where {K, V}
    exe(ssd.dbconns[shard(ssd, k)], query, (ssd.keyfuncs.to_sql(k), db_args_mapped(typeof(ssd), rest)...))
end
function exec(ssd::ShardedDBDict{K, V}, query::Union{Int,String}, args=())::Vector where {K, V}
    res = [] |> ThreadSafe
    @threads for dbconn in ssd.dbconns
        append!(res, exe(dbconn, query, db_args_mapped(typeof(ssd), args)))
    end
    res.wrapped
end
function rows(ssd::ShardedDBDict{K, V})::Vector where {K, V}
    exec(ssd, @sql("select * from $(ssd.table)"))
end

function Base.length(ssd::ShardedDBDict{K, V})::Int where {K, V}
    total = Ref(0) |> ThreadSafe
    @threads for dbconn in ssd.dbconns
        r = exe(dbconn, @sql("select count(1) from $(ssd.table)"))[1][1]
        lock(total) do total; total[] += ismissing(r) ? 0 : r; end
    end
    total[]
end
function Base.empty!(ssd::ShardedDBDict{K, V}) where {K, V}
    @threads for dbconn in ssd.dbconns
        exe(dbconn, @sql("delete from $(ssd.table)"))
    end
end
function Base.collect(ssd::ShardedDBDict{K, V})::Vector{Pair{K, V}} where {K, V}
    res = [] |> ThreadSafe
    @threads for dbconn in ssd.dbconns
        append!(res, [Pair{K, V}(kv[1] |> ssd.keyfuncs.from_sql, kv[2] |> ssd.valuefuncs.from_sql)
                      for kv in exe(dbconn, @sql("select * from $(ssd.table)"))])
    end
    res.wrapped
end
function Base.values(ssd::ShardedDBDict{K, V})::Vector{V} where {K, V}
    res = [] |> ThreadSafe
    for dbconn in ssd.dbconns
        append!(res, [r[1] |> ssd.valuefuncs.from_sql
                      for r in exe(dbconn, @sql("select $(ssd.valuecolumn) from $(ssd.table)"))])
    end
    res.wrapped
end
function Base.keys(ssd::ShardedDBDict{K, V})::Vector{K} where {K, V}
    res = [] |> ThreadSafe
    for dbconn in ssd.dbconns
        append!(res, [r[1] |> ssd.keyfuncs.from_sql
                      for r in exe(dbconn, @sql("select $(ssd.keycolumn) from $(ssd.table)"))])
    end
    res.wrapped
end
function Base.close(dbconn::DBConn) 
    for db in dbconn.dbs
        close(db)
    end
end
function Base.close(ssd::ShardedDBDict{K, V}) where {K, V}
    for dbconn in ssd.dbconns
        lock(dbconn) do dbconn; close(dbconn); end
    end
end
# function Base.lock(body, ssd::ShardedDBDict{K, V}) where {K, V}
#     body(ssd)
# end
# function Base.lock(body, ssds::Vector{ShardedDBDict})
#     body(ssds...)
# end
function incr(ssd::ShardedDBDict{Symbol, Int}, prop::Symbol; by=1)
    by == 0 && return
    exe(ssd, @sql("update $(ssd.table) set $(ssd.valuecolumn) = $(ssd.valuecolumn) + ?2 where $(ssd.keycolumn) = ?1"), prop, by)
end
function incr(d::ThreadSafe{Dict{Symbol, Int}}, prop::Symbol; by=1)
    by == 0 && return
    lock(d) do d
        d[prop] = get(d, prop, 0) + by
    end
end

include("cache_storage.jl")

end

