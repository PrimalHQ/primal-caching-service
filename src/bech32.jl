module Bech32

import ..Nostr

@enum Encoding::UInt8 begin
    BECH32 = 1
    BECH32M = 2
end

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
BECH32M_CONST = 0x2bc830a3

function bech32_polymod(values)
    generator = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for value in values
        top = chk >> 25
        chk = xor((chk & 0x1ffffff) << 5, value)
        for i in 0:4
            chk = xor(chk, ((top >> i) & 1) != 0 ? generator[i+1] : 0)
        end
    end
    chk
end

function bech32_hrp_expand(hrp)::Vector{UInt8}
    [[UInt8(x) >> 5 for x in hrp]; [0x00]; [UInt8(x) & 31 for x in hrp]]
end

function bech32_verify_checksum(hrp, data)
    c = bech32_polymod([bech32_hrp_expand(hrp); data])
    if c == 1; BECH32
    elseif c == BECH32M_CONST; BECH32M
    else; nothing; end
end

function bech32_create_checksum(hrp, data, spec)
    values = [bech32_hrp_expand(hrp); data]
    c = spec == BECH32M ? BECH32M_CONST : 1
    polymod = xor(bech32_polymod([values; [0x00, 0x00, 0x00, 0x00, 0x00, 0x00]]), c)
    [UInt8((polymod >> (5 * (5 - i))) & 31) for i in 0:5]
end

function bech32_encode(hrp, data, spec)
    combined = [data; bech32_create_checksum(hrp, data, spec)]
    hrp * '1' * join([CHARSET[d+1] for d in combined])
end

function bech32_decode(bech)
    if ((any([UInt8(x) < 33 || UInt8(x) > 126 for x in bech])) ||
        (lowercase(bech) != bech && uppercase(bech) != bech))
        return (nothing, nothing, nothing)
    end
    bech = lowercase(bech)
    pos = findlast('1', bech)
    if isnothing(pos) || pos + 6 > length(bech) #|| length(bech) > 90
        return (nothing, nothing, nothing)
    end
    if !all([x in CHARSET for x in bech[pos+1:end]])
        return (nothing, nothing, nothing)
    end
    hrp = bech[1:pos-1]
    data = [UInt8(findfirst(x, CHARSET)-1) for x in bech[pos+1:end]]
    spec = bech32_verify_checksum(hrp, data)
    if isnothing(spec)
        return (nothing, nothing, nothing)
    end
    (hrp, data[1:end-6], spec)
end

function convertbits(data::Vector{UInt8}, frombits, tobits; pad=true)
    acc = 0
    bits = 0
    ret = UInt8[]
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data
        if value < 0 || (value >> frombits) != 0
            return nothing
        end
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits
            bits -= tobits
            push!(ret, (acc >> bits) & maxv)
        end
    end
    if pad
        if bits != 0
            push!(ret, (acc << (tobits - bits)) & maxv)
        end
    elseif bits >= frombits || ((acc << (tobits - bits)) & maxv) != 0
        return nothing
    end
    ret
end

function decode(hrp, addr)
    hrpgot, data, spec = bech32_decode(addr)
    if hrpgot != hrp
        return nothing
    end
    convertbits(data[1:end], 5, 8; pad=false)
end

function encode(hrp, data)
    bech32_encode(hrp, convertbits(data, 8, 5), BECH32)
end
##
@enum TLVType::UInt8 begin
    Special=0
    Relay=1
    Author=2
    Kind=3
end
##
function nip19_decode(data)
    function tlvs(prefix, data)
        items = []
        i = 1
        while i <= length(data)
            t = TLVType(data[i]); i+=1
            l = data[i]; i+=1
            v = data[i:i+l-1]; i+=l
            if t == Special
                v = if prefix == "nprofile"; Nostr.PubKeyId(v)
                elseif prefix == "nevent"; Nostr.EventId(v)
                elseif prefix == "nrelay"; String(v)
                elseif prefix == "naddr"; String(v)
                end
            elseif t == Relay
                v = String(v)
            elseif t == Author
                v = if prefix == "naddr"; Nostr.PubKeyId(v)
                elseif prefix == "nevent"; Nostr.PubKeyId(v)
                end
            elseif t == Kind
                v = if prefix == "naddr"; ntoh(read(IOBuffer(v), UInt32)) |> Int
                elseif prefix == "nevent"; ntoh(read(IOBuffer(v), UInt32)) |> Int
                end
            end
            push!(items, (t, v))
        end
        items
    end
    for (prefix, ty) in [
                         ("npub", Nostr.PubKeyId),
                         ("nsec", Nostr.SecKey),
                         ("note", Nostr.EventId),
                        ]
        startswith(data, prefix) && return ty(decode(prefix, data))
    end
    for prefix in [
                   "nprofile",
                   "nevent",
                   "nrelay",
                   "naddr",
                  ]
        startswith(data, prefix) && return tlvs(prefix, decode(prefix, data))
    end
end

function nip19_decode_wo_tlv(data)
    d = nip19_decode(data)
    d isa Vector && (d = [v for (t, v) in d if t == Bech32.Special][1])
    d
end

end
