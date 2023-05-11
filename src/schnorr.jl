module Schnorr

const SECP256K1_CONTEXT_NONE = 1

mutable struct Secp256k1Ctx
    ptr::Ptr{Cvoid}
    xonlypk::Vector{UInt8}
    function Secp256k1Ctx()
        ptr = @ccall :libsecp256k1.secp256k1_context_create(SECP256K1_CONTEXT_NONE::Int32)::Ptr{Cvoid}
        ptr == 0 && error("secp256k1_context_create failed")
        finalizer(destroy!, new(ptr, zeros(UInt8, 64)))
    end
end

function destroy!(ctx::Secp256k1Ctx)
    @ccall :libsecp256k1.secp256k1_context_destroy(ctx.ptr::Ptr{Cvoid})::Cvoid
    ctx.ptr = C_NULL
    nothing
end

function verify(msg::Vector{UInt8}, pubkey::Vector{UInt8}, sig::Vector{UInt8})
    verify(Secp256k1Ctx(), msg, pubkey, sig)
end

function verify(ctx::Secp256k1Ctx, msg::Vector{UInt8}, pubkey::Vector{UInt8}, sig::Vector{UInt8})
    if (@ccall :libsecp256k1.secp256k1_xonly_pubkey_parse(ctx.ptr::Ptr{Cvoid}, pointer(ctx.xonlypk)::Ptr{UInt8}, pointer(pubkey)::Ptr{UInt8})::Int32) == 0
        error("parsing of pubkey failed")
    end
    (@ccall :libsecp256k1.secp256k1_schnorrsig_verify(ctx.ptr::Ptr{Cvoid}, pointer(sig)::Ptr{UInt8}, pointer(msg)::Ptr{UInt8}, Int32(length(msg))::Int32, pointer(ctx.xonlypk)::Ptr{UInt8})::Int32) != 0
end

begin
    local ctx = Secp256k1Ctx()
    local msg = hex2bytes("01a36e30a4117ba974f786c9135c198116d4bb100c516bc59fac48948ecb5abf")
    local pk  = hex2bytes("932fedb11d131b720362372e8248ecd79cb72e16aecba31b0650e4c2ff21ed00")
    local sig = hex2bytes("20c8a626e8b2f70a86938af2fca509a69910111dc735525e15e0ec36e020a12529e7d3116e60126c135f8345353acc9c82a00bfea394007a625d5ecccc0741cc")
    try
        @assert verify(ctx, msg, pk, sig)
    finally
        destroy!(ctx)
    end
end

end
