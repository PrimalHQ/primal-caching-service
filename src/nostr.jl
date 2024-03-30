module Nostr

import StaticArrays
import JSON
import SHA
import MbedTLS
import Base64

struct EventId; hash::StaticArrays.SVector{32, UInt8}; end
struct PubKeyId; pk::StaticArrays.SVector{32, UInt8}; end
struct SecKey; sk::StaticArrays.SVector{32, UInt8}; end
URL = String
struct Sig; sig::StaticArrays.SVector{64, UInt8}; end

Base.convert(::Type{EventId}, arr::Vector{UInt8}) = EventId(arr)
Base.convert(::Type{PubKeyId}, arr::Vector{UInt8}) = PubKeyId(arr)
Base.convert(::Type{Sig}, arr::Vector{UInt8}) = Sig(arr)

abstract type Tag end
struct TagAny <: Tag
    fields::Vector{Any}
end

@enum Kind::Int begin
    SET_METADATA=0
    TEXT_NOTE=1
    RECOMMEND_SERVER=2
    CONTACT_LIST=3
    DIRECT_MESSAGE=4
    EVENT_DELETION=5
    REPOST=6
    REACTION=7
    ZAP_RECEIPT=9735
    MUTE_LIST=10000
    RELAY_LIST_METADATA=10002
    CATEGORIZED_PEOPLE=30000
    LONG_FORM_CONTENT=30023
end
BOOKMARKS=10003

struct Event
    id::EventId # <32-bytes lowercase hex-encoded sha256 of the the serialized event data>
    pubkey::PubKeyId # <32-bytes lowercase hex-encoded public key of the event creator>,
    created_at::Int # <unix timestamp in seconds>,
    kind::Int # <integer>,
    tags::Vector{Tag}
    content::String # <arbitrary string>,
    sig::Sig # <64-bytes signature of the sha256 hash of the serialized event data, which is the same as the id field>
end


function vec2tags(tags::Vector)
    map(tags) do elem
        TagAny(elem)
    end
end

function dict2event(d::Dict{Symbol, Any})
    dict2event(Dict([string(k)=>v for (k, v) in d]))
end
function dict2event(d::Dict{String, Any})
    Event(EventId(d["id"]),
          PubKeyId(d["pubkey"]),
          d["created_at"],
          d["kind"],
          vec2tags(d["tags"]),
          d["content"],
          Sig(d["sig"]))
end
Event(d::Dict) = dict2event(d)

function event2dict(e::Event) # TODO compare with Utils.obj2dict
    Dict(:id=>JSON.lower(e.id),
         :pubkey=>JSON.lower(e.pubkey),
         :created_at=>e.created_at,
         :kind=>e.kind,
         :tags=>map(JSON.lower, e.tags),
         :content=>e.content,
         :sig=>JSON.lower(e.sig))
end

Base.Dict(e::Event) = event2dict(e)

Base.:(==)(e1::Event, e2::Event) = e1.id == e2.id
Base.hash(e::Event, h::UInt) = hash(e.id.hash, h)

function Event(seckey::SecKey, pubkey::PubKeyId, created_at::Int, kind::Int, tags::Vector, content::String)::Event
    eid = event_id(pubkey, created_at, kind, tags, content)
    Event(eid,
          pubkey, created_at, kind, tags, content,
          Secp256k1.generate_signature(collect(seckey.sk), collect(eid.hash)))
end

for ty in [EventId, SecKey, PubKeyId, Sig]
    eval(:($(ty.name.name)(hex::String) = $(ty.name.name)(hex2bytes(hex))))
    eval(:(hex(v::$(ty.name.name)) = bytes2hex(v.$(fieldnames(ty)[1]))))
    eval(:(Base.show(io::IO, v::$(ty.name.name)) = print(io, string($(ty.name.name))*"("*repr(hex(v))*")")))
    eval(:(Base.isless(a::$(ty.name.name), b::$(ty.name.name)) = Base.isless(a.$(fieldnames(ty)[1]), b.$(fieldnames(ty)[1]))))
end

JSON.lower(eid::EventId) = hex(eid)
JSON.lower(pubkey::PubKeyId) = hex(pubkey)
JSON.lower(sig::Sig) = hex(sig)
# json_lower_relay_url(relay_url) = isnothing(relay_url) ? [] : [relay_url]
# JSON.lower(tag::TagE) = ["e", tag.event_id, json_lower_relay_url(tag.relay_url)..., tag.extra...]
# JSON.lower(tag::TagP) = ["p", tag.pubkey_id, json_lower_relay_url(tag.relay_url)..., tag.extra...]
JSON.lower(tag::TagAny) = tag.fields

include("secp256k1.jl")

function generate_keypair()
    seckey, pubkey = Secp256k1.generate_keypair()
    SecKey(seckey), PubKeyId(pubkey)
end

# Secp256k1.verify(ctx::Secp256k1.Secp256k1Ctx, e::Event) = Secp256k1.verify(ctx, collect(e.id.hash), collect(e.pubkey.pk), collect(e.sig.sig))
Secp256k1.verify(e::Event) = Secp256k1.verify(collect(e.id.hash), collect(e.pubkey.pk), collect(e.sig.sig))

function event_id(pubkey::PubKeyId, created_at::Int, kind::Int, tags::Vector, content::String)::EventId
    [0,
     pubkey,
     created_at,
     kind,
     tags,
     content,
    ] |> JSON.json |> IOBuffer |> SHA.sha256 |> EventId
end

function verify(e::Event)
    e.id == event_id(e.pubkey, e.created_at, e.kind, e.tags, e.content) || return false
    Secp256k1.verify(e) || return false
    true
end

re_hrp = r"^(note|npub|naddr|nevent|nprofile)\w+$"
function bech32_decode(s::AbstractString)
    m = match(re_hrp, s)
    isnothing(m) && error("invalid hrp")
    hrp = m.captures[1]
    id = bech32_decode(s, hrp)
    isnothing(id) && error("unable to decode bech32")
    if     hrp ==     "npub"; PubKeyId(id)
    elseif hrp ==    "naddr"; PubKeyId(id)
    elseif hrp == "nprofile"; PubKeyId(id)
    elseif hrp ==     "note"; EventId(id)
    elseif hrp ==   "nevent"; EventId(id)
    else error("unsupported type of id")
    end
end
function bech32_decode(s::AbstractString, hrp::AbstractString)
    outdata = zeros(UInt8, 100)
    outdata_len = [Csize_t(0)]
    input = string(s)*"\x00"
    hrp = string(hrp)*"\x00"
    GC.@preserve input hrp if (@ccall :libbech32.nostr_bech32_decode(pointer(outdata)::Ptr{Cchar}, 
                                                                     pointer(outdata_len)::Ptr{Csize_t}, 
                                                                     pointer(hrp)::Ptr{Cchar}, 
                                                                     pointer(input)::Ptr{Cchar})::Clong) == 1
        return outdata[1:outdata_len[1]]
    end
    nothing
end

bech32_encode(input::EventId) = bech32_encode(collect(input.hash), "note")
bech32_encode(input::PubKeyId) = bech32_encode(collect(input.pk), "npub")
function bech32_encode(input::Vector{UInt8}, hrp::AbstractString)
    outdata = zeros(UInt8, 100)
    hrp = string(hrp)*"\x00"
    GC.@preserve input hrp if (@ccall :libbech32.nostr_bech32_encode(pointer(outdata)::Ptr{Cchar}, 
                                                                     pointer(hrp)::Ptr{Cchar}, 
                                                                     0::Clong, # witver
                                                                     pointer(input)::Ptr{Cchar},
                                                                     length(input)::Csize_t)::Clong) == 1
        return String(outdata[1:findfirst([0x00], outdata).start-1])
    end
    nothing
end

function nip04_encrypt(seckey::SecKey, pubkey::PubKeyId, msg::Vector{UInt8}, iv::Vector{UInt8})::Vector{UInt8}
    @assert length(iv) == 16
    secret = Secp256k1.create_shared_secret(collect(pubkey.pk), collect(seckey.sk))
    MbedTLS.encrypt(MbedTLS.CIPHER_AES_256_CBC, secret, msg, iv)
end

function nip04_encrypt(seckey::SecKey, pubkey::PubKeyId, msg::String)::String
    iv = rand(UInt8, 16)
    encbytes = nip04_encrypt(seckey, pubkey, map(UInt8, transcode(UInt8, msg)), iv)
    "$(Base64.base64encode(encbytes))?iv=$(Base64.base64encode(iv))"
end

function nip04_decrypt(seckey::SecKey, pubkey::PubKeyId, encmsg::Vector{UInt8}, iv::Vector{UInt8})::Vector{UInt8}
    @assert length(iv) == 16
    secret = Secp256k1.create_shared_secret(collect(pubkey.pk), collect(seckey.sk))
    MbedTLS.decrypt(MbedTLS.CIPHER_AES_256_CBC, secret, encmsg, iv)
end

function nip04_decrypt(seckey::SecKey, pubkey::PubKeyId, encmsg::String)::String
    parts = split(encmsg, "?iv=")
    msgbytes = Base64.base64decode(parts[1])
    iv = Base64.base64decode(parts[2])
    String(nip04_decrypt(seckey, pubkey, msgbytes, iv))
end

function nip04_test()
    sk1, pk1 = SecKey("4f1e068572f04922787e79d3c3e9cacfdbaac34a153c8b827dbb919c3fa7830e"), PubKeyId("e89559104c96f2001c04ab1cf78a53bca8bb6d664ee0e058cbcf8935b0db45a1")
    sk2, pk2 = SecKey("7fa5f2185b0a9698130b284f024ea72915eb9fabe9bf1e2b8da9ea350abafe32"), PubKeyId("0e45904bde9ee8762ea62c8b0edd910062ffa86851508a704e1627a85c563679")

    msg = "hola"^30
    s = nip04_encrypt(sk1, pk2, msg)
    msg2 = nip04_decrypt(sk2, pk1, s)
    @assert msg == msg2

    @assert "hola" == nip04_decrypt(sk2, pk1, "WR5kXpPeyChjQU93V4nAfQ==?iv=roFcA58RlIyJMrj5VVoHwg==")

    sender_sk = SecKey("6b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e")
    sender_pk = Secp256k1.pubkey_of_seckey(sender_sk.sk |> collect) |> Nostr.PubKeyId
    receiver_sk = SecKey("7b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e")
    receiver_pk = Secp256k1.pubkey_of_seckey(receiver_sk.sk |> collect) |> Nostr.PubKeyId
    @assert nip04_decrypt(receiver_sk, sender_pk, "dJc+WbBgaFCD2/kfg1XCWJParplBDxnZIdJGZ6FCTOg=?iv=M6VxRPkMZu7aIdD+10xPuw==") == "Saturn, bringer of old age"
end

end
