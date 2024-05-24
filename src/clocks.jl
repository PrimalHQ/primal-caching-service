module Clocks

CLOCK_REALTIME = 0
CLOCK_MONOTONIC = 1
CLOCK_PROCESS_CPUTIME_ID = 2
CLOCK_THREAD_CPUTIME_ID = 3
CLOCK_MONOTONIC_RAW = 4
CLOCK_REALTIME_COARSE = 5
CLOCK_MONOTONIC_COARSE = 6
CLOCK_BOOTTIME = 7
CLOCK_REALTIME_ALARM = 8
CLOCK_BOOTTIME_ALARM = 9
CLOCK_TAI = 11

mutable struct Timespec
    sec::Int
    nsec::Int
end

@inline function clock_gettime(clockid::Int)
    ts = Timespec(0, 0)
    @assert ccall(:clock_gettime, Int, (Int, Ptr{Timespec}), clockid, pointer_from_objref(ts)) == 0
    Int128(ts.sec)*1_000_000_000 + ts.nsec
end

@inline function gettime(clockid::Int)::Float64
    clock_gettime(clockid) / 1e9
end

end
