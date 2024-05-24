module PerfStats

using ..Utils: ThreadSafe
import ..Clocks

tstart = Ref(0.0) |> ThreadSafe

collections = Dict() |> ThreadSafe

function with_collection(body::Function, d::Symbol)
    c = lock(collections) do collections
        get!(collections, d) do; (; d=Dict(), tstart=Ref(time())) |> ThreadSafe; end
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
                      avgduration=v.duration/v.count,
                      avgcpuusage=v.cpuusage/v.count) 
                  for (k, v) in collect(c.d)]; by=x->-by(x[2]))
        end
    end
end

function record!(body::Function, d::Symbol, key)
    with_collection(d) do c
        sticky = Base.current_task().sticky

        tid1 = Threads.threadid()
        cput1 = sticky ? Clocks.clock_gettime(Clocks.CLOCK_THREAD_CPUTIME_ID) : 0.0

        t = @timed body()

        tid2 = Threads.threadid()
        cput2 = sticky ? Clocks.clock_gettime(Clocks.CLOCK_THREAD_CPUTIME_ID) : 0.0

        cpuusage = tid1 == tid2 ? (cput2-cput1)/1e9 : 0.0

        lock(c) do c
            r = get!(c.d, key) do; (; count=0, duration=0.0, cpuusage=0.0, maxduration=0.0, maxcpuusage=0.0); end
            c.d[key] = (; 
                        count=r.count+1, 
                        duration=r.duration+t.time,
                        cpuusage=r.cpuusage+cpuusage,
                        maxduration=max(r.maxduration, t.time),
                        maxcpuusage=max(r.maxcpuusage, cpuusage))
        end

        t.value
    end
end

end
