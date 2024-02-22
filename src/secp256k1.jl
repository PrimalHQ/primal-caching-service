module Secp256k1

const SECP256K1_CONTEXT_NONE = 1

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

function pubkey_of_seckey(seckey::Vector{UInt8})
    @assert length(seckey) == 32

    with_context() do ctx
        keypair = zeros(UInt8, 96)
        pubkey = zeros(UInt8, 64)
        serialized_pubkey = zeros(UInt8, 32)

        GC.@preserve pubkey serialized_pubkey begin
            @assert (@ccall :libsecp256k1.secp256k1_keypair_create(ctx::Ptr{Cvoid}, pointer(keypair)::Ptr{UInt8}, pointer(seckey)::Ptr{UInt8})::Cint) != 0

            @assert (@ccall :libsecp256k1.secp256k1_keypair_xonly_pub(ctx::Ptr{Cvoid}, pointer(pubkey)::Ptr{UInt8}, C_NULL::Ptr{Cvoid}, pointer(keypair)::Ptr{UInt8})::Cint) != 0

            @assert (@ccall :libsecp256k1.secp256k1_xonly_pubkey_serialize(ctx::Ptr{Cvoid}, pointer(serialized_pubkey)::Ptr{UInt8}, pointer(pubkey)::Ptr{UInt8})::Cint) != 0

            serialized_pubkey
        end
    end
end

function generate_signature(seckey::Vector{UInt8}, msg_hash::Vector{UInt8})
    @assert length(seckey) == 32
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
    @assert length(msg_hash) == 32
    @assert length(serialized_pubkey) == 32
    @assert length(signature) == 64

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

null_hashfp = @cfunction(function (output, x32, y32, data)
                             unsafe_copyto!(output, x32, 32)
                             Cint(1)
                         end, 
                         Cint, 
                         (Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}, Ptr{Cvoid}))

function create_shared_secret(serialized_pubkey::Vector{UInt8}, seckey::Vector{UInt8})
    @assert length(serialized_pubkey) == 32
    @assert length(seckey) == 32

    with_context() do ctx
        secret = zeros(UInt8, 32)
        pubkey = zeros(UInt8, 64)

        GC.@preserve serialized_pubkey seckey secret pubkey begin
            if (@ccall :libsecp256k1.secp256k1_xonly_pubkey_parse(ctx::Ptr{Cvoid}, pointer(pubkey)::Ptr{UInt8}, pointer(serialized_pubkey)::Ptr{UInt8})::Int32) == 0
                error("parsing of serialized_pubkey failed")
            end
            @assert (@ccall :libsecp256k1.secp256k1_ecdh(ctx::Ptr{Cvoid}, pointer(secret)::Ptr{UInt8}, pointer(pubkey)::Ptr{UInt8}, pointer(seckey)::Ptr{UInt8}, null_hashfp::Ptr{Cvoid}, C_NULL::Ptr{Cvoid})::Cint) != 0
            secret
        end
    end
end

begin
    let msg = hex2bytes("01a36e30a4117ba974f786c9135c198116d4bb100c516bc59fac48948ecb5abf"),
        pk  = hex2bytes("932fedb11d131b720362372e8248ecd79cb72e16aecba31b0650e4c2ff21ed00"),
        sig = hex2bytes("20c8a626e8b2f70a86938af2fca509a69910111dc735525e15e0ec36e020a12529e7d3116e60126c135f8345353acc9c82a00bfea394007a625d5ecccc0741cc")
        @assert verify(msg, pk, sig)
        global benchfunc() = verify(msg, pk, sig)
    end

    let sk  = hex2bytes("4f1e068572f04922787e79d3c3e9cacfdbaac34a153c8b827dbb919c3fa7830e"),
        pk  = hex2bytes("0e45904bde9ee8762ea62c8b0edd910062ffa86851508a704e1627a85c563679"),
        sec = hex2bytes("26c3d58e5cdf72104cc9b1672cd8eafb4a51523856b108aa545ef4ed8b9a0b90")
        @assert sec == create_shared_secret(pk, sk)
    end
end

end
