module Utils

using Dates

import DataStructures

dt2unix(dt::DateTime) = trunc(Int, datetime2unix(dt))
dt2unix(s::String) = dt2unix(DateTime(s))

export watch_file_exec
function watch_file_exec(fn; start_code=nothing, stop_code=nothing, running=Ref(true))
    @async begin
        mt = nothing
        while running[]
            mt_ = mtime(fn)
            if mt_ != mt
                mt = mt_
                println()
                println("loading $fn into Main module")
                !isnothing(stop_code) && try Main.eval(stop_code) catch _ end
                try
                    Main.eval(:(include($fn)))
                    println("successfully loaded $fn into Main module")
                catch _
                    print_exceptions()
                end
                !isnothing(start_code) && try Main.eval(start_code) catch _ end
            end
            sleep(0.2)
        end
        println("watch_file_exec of $fn is done")
    end
    running
end

export safe_save
function safe_save(filename::String, v)
    fn = filename * ".saving"
    open(fn, "w") do f; write(f, v); end
    mv(fn, filename; force=true)
end

export load
function load(filename::String, ty=String; default=error)
    if isfile(filename)
        open(filename) do f; read(f, ty); end
    else
        default == error ? error("$filename doesnt exist") : default
    end
end

export Throttle
mutable struct Throttle
    t
    period
    lock
    Throttle(; period=1.0, t=time()) = new(t, period, ReentrantLock())
end
function (thr::Throttle)(f::Function)
    lock(thr.lock) do
        t = time()
        if t - thr.t >= thr.period
            thr.t = t
            f()
        end
        nothing
    end
end

export active_sleep
function active_sleep(period, active=Ref(true); dt=1.0)
    while active[] && period > 0
        sleep(dt)
        period -= dt
    end
end

export wait_for
function wait_for(cond::Function; timeout=5.0, dt=1.0)
    while timeout > 0
        v = cond()
        if v == false || v == nothing
            sleep(dt)
            timeout -= dt
        else
            return v
        end
    end
    error("timeout waiting for $cond")
end

struct RefProp
    obj
    prop::Symbol
end
Base.getindex(rp::RefProp) = getproperty(rp.obj, rp.prop)
Base.setindex!(rp::RefProp, v) = setproperty!(rp.obj, rp.prop, v)

export @refprop
macro refprop(arg)
    args = arg.args
    :(RefProp($(esc(args[1])), $(args[2])))
end

" should have active and task properties "
abstract type Tasked end

Base.@kwdef mutable struct SimpleTasked <: Tasked
    active=true
    task=nothing
    threadid=-1
end

Base.wait(t::Tasked) = wait(t.task)

taskeds = Set()
export register!
function register!(obj::Tasked)
    obj in taskeds || push!(taskeds, obj)
    obj
end
export unregister!
function unregister!(obj::Tasked)
    obj in taskeds && delete!(taskeds, obj)
    obj
end
export errormonitor
function errormonitor(t::Tasked)
    t.task = errormonitor(t.task)
    t
end
export stop
function stop(t::Tasked)
    t.active = false
    wait(t)
    unregister!(t)
end
export async_task
function async_task(f::Function)
    t = SimpleTasked()
    t.task = @async f(t)
    register!(t)
end
export threaded_task
function threaded_task(f::Function)
    t = SimpleTasked()
    t.task = Threads.@spawn begin
        t.threadid = Threads.threadid()
        f(t)
    end
    register!(t)
end

export print_exceptions
function print_exceptions(io=stdout)
    for (exc, bt) in current_exceptions()
        showerror(io, exc, bt)
        println(io)
    end
end

export ThreadSafe
struct ThreadSafe{T}
    wrapped::T   # TODO rename to wrappee?
    lock::ReentrantLock
    ThreadSafe(wrapped::T) where {T} = new{T}(wrapped, ReentrantLock())
end
const THREAD_SAFE_DISABLE = false
export @thread_safety_wrapper
macro thread_safety_wrapper(func)
    @assert func.head == :.
    :(function $func(o::ThreadSafe{T}, args...; kwargs...) where {T}
          THREAD_SAFE_DISABLE || (locked_args = [])
          try
              THREAD_SAFE_DISABLE || push!(locked_args, o)
              THREAD_SAFE_DISABLE || lock(o.lock)
              THREAD_SAFE_DISABLE || for a in args
                  a isa ThreadSafe|| continue
                  push!(locked_args, a)
                  lock(a.lock)
              end
              $func(o.wrapped, args...; kwargs...)
          finally
              THREAD_SAFE_DISABLE || for a in locked_args
                  unlock(a.lock)
              end
          end
      end)
end

@thread_safety_wrapper Base.push!
@thread_safety_wrapper Base.append!
@thread_safety_wrapper Base.union!
@thread_safety_wrapper Base.merge!
@thread_safety_wrapper Base.delete!
@thread_safety_wrapper Base.copy
@thread_safety_wrapper Base.copy!
@thread_safety_wrapper Base.isempty
@thread_safety_wrapper Base.empty!
@thread_safety_wrapper Base.filter!
@thread_safety_wrapper Base.lastindex
@thread_safety_wrapper Base.get
@thread_safety_wrapper Base.get!
@thread_safety_wrapper Base.getindex
@thread_safety_wrapper Base.setindex!
@thread_safety_wrapper Base.haskey
@thread_safety_wrapper Base.length
@thread_safety_wrapper Base.sort
@thread_safety_wrapper Base.sort!
@thread_safety_wrapper Base.sizehint!
@thread_safety_wrapper Base.display
@thread_safety_wrapper Base.keys
@thread_safety_wrapper Base.values
@thread_safety_wrapper Base.collect
@thread_safety_wrapper Base.close
Base.in(v, o::ThreadSafe{T}) where {T} = lock(o.lock) do; in(v, o.wrapped); end
function Base.lock(f::Function, o::ThreadSafe{T}) where {T}
    if THREAD_SAFE_DISABLE
        o.wrapped 
    else
        lock(o.lock) do; f(o.wrapped); end
    end
end
function Base.lock(f::Function, os::Vector{ThreadSafe}) 
    locked = ThreadSafe[]
    try
        for o in os; lock(o.lock); push!(locked, o); end
        f([o.wrapped for o in os]...)
    finally
        for o in locked; unlock(o.lock); end
    end
end

import TerminalUserInterfaces
TUI = TerminalUserInterfaces
export move_cursor
move_cursor(io, row, col) = print(io, TUI.Terminals.CSI, TUI.INDICES[row], ';', TUI.INDICES[col], 'H')
move_cursor(row, col) = move_cursor(stdout, row, col)
export clear_screen
clear_screen(io) = print(io, TUI.CLEARSCREEN)
clear_screen() = clear_screen(stdout)

#ST(T) = T.types[1]
ST(T) = T

export obj2dict
function obj2dict(obj)
    Dict((k, getproperty(obj, k)) for k in propertynames(obj))
end

struct TimeLimitedRunning
    timeout
    tstart
    TimeLimitedRunning(timeout) = new(timeout, time())
end
Base.getindex(running::TimeLimitedRunning) = (time() - running.tstart) < running.timeout

struct PressEnterToStop
    running
    keytsk
    function PressEnterToStop()
        running = Ref(true) |> ThreadSafe
        keytsk = @async begin
            readline()
            running[] = false
            println("PressEnterToStop exited")
        end
        new(running, keytsk)
    end
end
Base.getindex(pets::PressEnterToStop) = pets.running[]

isfull(chan::Channel) = length(chan.data) >= chan.sz_max

function counts(collection)
    acc = DataStructures.Accumulator{Any,Int}()
    for r in collect(collection)
        push!(acc, r)
    end
    acc
end

struct GCTask
    running::ThreadSafe{Base.RefValue{Bool}}
    period::Base.RefValue{Int}
    task::Task
    function GCTask(; period=15)
        running = ThreadSafe(Ref(true))
        period = Ref(period)
        new(running, period, 
            Base.errormonitor(@async while running[]
                                  # if Base.gc_live_bytes() >= 10*1024^3
                                  GC.gc(false)
                                  ccall(:malloc_trim, Cint, (Cint,), 0)
                                  # end
                                  active_sleep(period[], running)
                              end))
    end
end

function stop(gc_task::GCTask)
    gc_task.running[] = false
    wait(gc_task.task)
end

function extension(obj, name::Symbol, args...; kwargs...)
    if name in obj.extension_hooks
        obj.extension_hooks[name](args...; kwargs...)
    end
end

end
