module PerfStats

using ..Utils: ThreadSafe

tstart = Ref(0.0) |> ThreadSafe

collections = Dict() |> ThreadSafe

function with_collection(body::Function, d::Symbol)
    c = lock(collections) do collections
        get!(collections, d) do; (; d=Dict(), tstart=Ref(0.0)) |> ThreadSafe; end
    end
    body(c)
end

function reset!()
   empty!(collections)
   nothing
end

function reset!(d::Symbol)
    with_collection(d) do c
        lock(c) do c
            c.tstart[] = time()
            empty!(c.d)
        end
    end
end

function report(d::Symbol; by=x->x.avgduration)
    with_collection(d) do c
        lock(c) do c
            totalduration = sum([v.duration for (k, v) in collect(c.d)])
            sort([k=>(; v..., 
                      durationperc=v.duration/totalduration, 
                      wallperc=v.duration/(Threads.nthreads()*(time()-tstart[])),
                      avgduration=v.duration/v.count) 
                  for (k, v) in collect(c.d)]; by=x->-by(x[2]))
        end
    end
end

function record!(body::Function, d::Symbol, key)
    with_collection(d) do c
        t = @timed body()
        lock(c) do c
            r = get!(c.d, key) do; (; count=0, duration=0.0); end
            c.d[key] = (; count=r.count+1, duration=r.duration+t.time)
        end
        t.value
    end
end

end
