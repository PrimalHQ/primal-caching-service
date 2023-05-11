module MetricsLogger

import JSON
using DataStructures: CircularBuffer

using ..Utils: ThreadSafe, Throttle, print_exceptions, isfull

CHANNEL_SIZE = Ref(1_000_000)

PRINT_EXCEPTIONS = Ref(true)

channel = Ref{Any}(nothing)
running = Ref(false)
logging_task = Ref{Any}(nothing)
flog = Ref{Any}(nothing)

latest_messages = CircularBuffer(500) |> ThreadSafe
latest_exceptions = CircularBuffer(200) |> ThreadSafe

function ext_start(output_filename) end
function ext_stop() end

function start(output_filename="/home/pr/var/primalserver/metrics.log")
    empty!(latest_messages)
    empty!(latest_exceptions)
    channel[] = Channel{Any}(CHANNEL_SIZE[])
    running[] = true

    logging_task[] = 
    errormonitor(@async open(output_filename, "a+") do f
                     flog[] = f
                     periodically = Throttle(; period=1.0)
                     while running[]
                         msg = take!(channel[])
                         try
                             println(f, JSON.json(msg))
                         catch ex
                             push!(latest_exceptions, (ex, msg))
                             PRINT_EXCEPTIONS[] && print_exceptions()
                         end
                         periodically() do; flush(f); end
                     end
                     flog[] = nothing
                     @debug "logging_task is stopped"
                 end)

    ext_start(output_filename)

    log(:metrics_logger_started)
    @info "started metrics logger to file $(output_filename)"
end

function stop()
    isnothing(logging_task[]) && return
    log(:metrics_logger_stopped)
    running[] = false
    wait(logging_task[])
    logging_task[] = nothing
    ext_stop()
    channel[] = nothing
    @info "stopped metrics logger"
end

function log(msg)
    running[] || return
    push!(latest_messages, msg)
    !isfull(channel[]) && put!(channel[], msg);
end
function log(body::Function, descfunc::Function)
    t = time()
    logit(r) = log((; t, filter(p->p[1]!=:value, pairs(r))..., descfunc(r)...))
    try 
        r = @timed body() 
        logit(r)
        r.value
    catch ex
        ex isa TaskFailedException && (ex = ex.task.result)
        logit((; time=time()-t, ex=string(ex isa ErrorException ? ex : typeof(ex))))
        rethrow()
    end
end
log(body::Function, event::Symbol) = log(body, r->(; event))
log(event::Symbol) = log((; t=time(), event))

end
