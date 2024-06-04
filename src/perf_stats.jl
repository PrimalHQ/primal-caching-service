module PerfStats

import Dates

using ..Utils: ThreadSafe
import ..Clocks

tstart = Ref(0.0) |> ThreadSafe

collections = Dict() |> ThreadSafe

function get_collection(d::Symbol)
    c = lock(collections) do collections
        get!(collections, d) do; (; d=Dict(), tstart=Ref(time())) |> ThreadSafe; end
    end
end

function reset!()
   empty!(collections)
   nothing
end

function reset!(d::Symbol)
    lock(get_collection(d)) do c
        c.tstart[] = time()
        empty!(c.d)
    end
end

function report(d::Symbol; by=x->x.avgduration)
    lock(get_collection(d)) do c
        totalduration = sum([v.duration for (k, v) in collect(c.d)])
        sort([k=>(; v..., 
                  durationperc=v.duration/totalduration, 
                  wallperc=v.duration/(Threads.nthreads()*(time()-tstart[])),
                  avgduration=v.duration/v.count,
                  avgcpuusage=v.cpuusage/v.count,
                  avgallocs=v.allocs/v.count,
                  avgallocbytes=v.allocbytes/v.count,
                 ) 
              for (k, v) in collect(c.d)]; by=x->-by(x[2]))
    end
end

function record!(body::Function, d::Symbol, key)
    sticky = Base.current_task().sticky

    tid1 = Threads.threadid()
    cput1 = sticky ? Clocks.clock_gettime(Clocks.CLOCK_THREAD_CPUTIME_ID) : 0.0

    t = @timed body()

    tid2 = Threads.threadid()
    cput2 = sticky ? Clocks.clock_gettime(Clocks.CLOCK_THREAD_CPUTIME_ID) : 0.0

    cpuusage = tid1 == tid2 ? (cput2-cput1)/1e9 : 0.0

    lock(get_collection(d)) do c
        r = get!(c.d, key) do
            (; 
             count=0, 
             duration=0.0, 
             cpuusage=0.0, 
             maxduration=0.0, 
             maxcpuusage=0.0,
             allocbytes=0,
             allocs=0,
            )
        end
        c.d[key] = (; 
                    count=r.count+1, 
                    duration=r.duration+t.time,
                    cpuusage=r.cpuusage+cpuusage,
                    maxduration=max(r.maxduration, t.time),
                    maxcpuusage=max(r.maxcpuusage, cpuusage),
                    allocbytes=r.allocbytes+t.bytes,
                    allocs=r.allocs+Base.gc_alloc_count(t.gcstats),
                   )
    end

    # t.time > 1 && println((string(Dates.now()), t.time, key))

    t.value
end

end
