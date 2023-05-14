import HTTP
HTTP.ConnectionPool.eval(quote
function keepalive!(tcp)
    #println("keepalive! disabled")
    return
    Base.iolock_begin()
    err = ccall(:uv_tcp_keepalive, Cint, (Ptr{Nothing}, Cint, Cuint),
                tcp.handle, 1, 1)
    Base.uv_error("failed to set keepalive on tcp socket", err)
    Base.iolock_end()
    return
end
end)

