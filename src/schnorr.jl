module Schnorr

const SECP256K1_CONTEXT_NONE = 1

# mutable struct Secp256k1Ctx
#     ptr::Ptr{Cvoid}
#     xonlypk::Vector{UInt8}
#     function Secp256k1Ctx()
#         ptr = @ccall :libsecp256k1.secp256k1_context_create(SECP256K1_CONTEXT_NONE::Int32)::Ptr{Cvoid}
#         ptr == 0 && error("secp256k1_context_create failed")
#         finalizer(destroy!, new(ptr, zeros(UInt8, 64)))
#     end
# end

# function destroy!(ctx::Secp256k1Ctx)
#     @ccall :libsecp256k1.secp256k1_context_destroy(ctx.ptr::Ptr{Cvoid})::Cvoid
#     ctx.ptr = C_NULL
#     nothing
# end

# function verify(msg::Vector{UInt8}, pubkey::Vector{UInt8}, sig::Vector{UInt8})
#     verify(Secp256k1Ctx(), msg, pubkey, sig)
# end

# function verify(ctx::Secp256k1Ctx, msg::Vector{UInt8}, pubkey::Vector{UInt8}, sig::Vector{UInt8})
#     if (@ccall :libsecp256k1.secp256k1_xonly_pubkey_parse(ctx.ptr::Ptr{Cvoid}, pointer(ctx.xonlypk)::Ptr{UInt8}, pointer(pubkey)::Ptr{UInt8})::Int32) == 0
#         error("parsing of pubkey failed")
#     end
#     (@ccall :libsecp256k1.secp256k1_schnorrsig_verify(ctx.ptr::Ptr{Cvoid}, pointer(sig)::Ptr{UInt8}, pointer(msg)::Ptr{UInt8}, Int32(length(msg))::Int32, pointer(ctx.xonlypk)::Ptr{UInt8})::Int32) != 0
# end

function with_context(body::Function; randomize=true)
    ctx = @ccall :libsecp256k1.secp256k1_context_create(SECP256K1_CONTEXT_NONE::Int32)::Ptr{Cvoid}
    try
        GC.@preserve ctx begin
            if randomize
                rnd = rand(UInt8, 32)
                @GC.preserve rnd @assert (@ccall :libsecp256k1.secp256k1_context_randomize(ctx::Ptr{Cvoid}, pointer(rnd)::Ptr{UInt8})::Cint) != 0
            end
            body(ctx)
        end
    finally
        @ccall :libsecp256k1.secp256k1_context_destroy(ctx::Ptr{Cvoid})::Cvoid
    end
end

function generate_keypair()
    with_context() do ctx
        keypair = zeros(UInt8, 96)
        pubkey = zeros(UInt8, 64)
        serialized_pubkey = zeros(UInt8, 32)

        GC.@preserve keypair pubkey serialized_pubkey begin
            seckey = nothing
            while true
                seckey = rand(UInt8, 32)
                if (@ccall :libsecp256k1.secp256k1_keypair_create(ctx::Ptr{Cvoid}, pointer(keypair)::Ptr{UInt8}, pointer(seckey)::Ptr{UInt8})::Cint) != 0
                    break
                end
            end

            @assert (@ccall :libsecp256k1.secp256k1_keypair_xonly_pub(ctx::Ptr{Cvoid}, pointer(pubkey)::Ptr{UInt8}, C_NULL::Ptr{Cvoid}, pointer(keypair)::Ptr{UInt8})::Cint) != 0

            @assert (@ccall :libsecp256k1.secp256k1_xonly_pubkey_serialize(ctx::Ptr{Cvoid}, pointer(serialized_pubkey)::Ptr{UInt8}, pointer(pubkey)::Ptr{UInt8})::Cint) != 0

            seckey, serialized_pubkey
        end
    end
end

function generate_signature(seckey::Vector{UInt8}, msg_hash::Vector{UInt8})
    @assert length(msg_hash) == 32

    with_context() do ctx
        keypair = zeros(UInt8, 96)
        rnd = rand(UInt8, 32)
        signature = zeros(UInt8, 64)

        GC.@preserve seckey msg_hash keypair rnd signature begin
            @assert (@ccall :libsecp256k1.secp256k1_keypair_create(ctx::Ptr{Cvoid}, pointer(keypair)::Ptr{UInt8}, pointer(seckey)::Ptr{UInt8})::Cint) != 0

            @assert (@ccall :libsecp256k1.secp256k1_schnorrsig_sign(ctx::Ptr{Cvoid}, signature::Ptr{UInt8}, pointer(msg_hash)::Ptr{UInt8}, pointer(keypair)::Ptr{UInt8}, pointer(rnd)::Ptr{UInt8})::Cint) != 0

            signature
        end
    end
end

function verify(msg_hash::Vector{UInt8}, serialized_pubkey::Vector{UInt8}, signature::Vector{UInt8})
    with_context(; randomize=false) do ctx
        pubkey = zeros(UInt8, 64)
        GC.@preserve msg_hash serialized_pubkey signature pubkey begin
            if (@ccall :libsecp256k1.secp256k1_xonly_pubkey_parse(ctx::Ptr{Cvoid}, pointer(pubkey)::Ptr{UInt8}, pointer(serialized_pubkey)::Ptr{UInt8})::Int32) == 0
                error("parsing of serialized_pubkey failed")
            end
            (@ccall :libsecp256k1.secp256k1_schnorrsig_verify(ctx::Ptr{Cvoid}, pointer(signature)::Ptr{UInt8}, pointer(msg_hash)::Ptr{UInt8}, Int32(length(msg_hash))::Int32, pointer(pubkey)::Ptr{UInt8})::Int32) != 0
        end
    end
end

begin
    # local ctx = Secp256k1Ctx()
    local msg = hex2bytes("01a36e30a4117ba974f786c9135c198116d4bb100c516bc59fac48948ecb5abf")
    local pk  = hex2bytes("932fedb11d131b720362372e8248ecd79cb72e16aecba31b0650e4c2ff21ed00")
    local sig = hex2bytes("20c8a626e8b2f70a86938af2fca509a69910111dc735525e15e0ec36e020a12529e7d3116e60126c135f8345353acc9c82a00bfea394007a625d5ecccc0741cc")
    try
        # @assert verify(ctx, msg, pk, sig)
        @assert verify(msg, pk, sig)
        global benchfunc() = verify(msg, pk, sig)
    finally
        # destroy!(ctx)
    end
end

end
