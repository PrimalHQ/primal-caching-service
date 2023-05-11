### Overview

Primal’s caching service for Nostr connects to the specified set of relays, collects all events in real time, stores them locally, and makes them available to nostr clients through a web socket-based API.

### Usage

Running the caching service if you have nix package manager installed:

    nix develop -c sh -c '$start_primal_caching_service'

Running the caching service if you have docker installed:

    docker run -it --rm -v `pwd`:`pwd` -w `pwd` -p 8801:8801 -e PRIMALSERVER_HOST=0.0.0.0 nixos/nix nix --extra-experimental-features 'nix-command flakes' develop -c sh -c '$start_primal_caching_service' 

Log files for fetched messages and sqlite database files will be located in `var` sub-directory by default.

Monitor fetching (press enter to stop monitoring):

    Fetching.mon()

To safely stop the process:

    Fetching.stop(); close(cache_storage); exit()

### API requests

Read `app.jl` for list of all supported arguments.

Examples:

    ["REQ", "amelx49c18", {"cache": ["net_stats"]}]
    ["CLOSE", "amelx49c18"]

    ["REQ", "p0xren2axa", {"cache": ["feed", {"pubkey": "32-hex digits of pubkey id"}]}]

    ["REQ", "vqvv4vc6us", {"cache": ["thread_view", {"event_id": "32-hex digits of event id"}]}]

    ["REQ", "ay4if6pykg", {"cache": ["user_infos", {"pubkeys": ["32-hex digits of pubkey id"]}]}]

    ["REQ", "2t6z17orjp", {"cache": ["events", {"event_ids": ["32-hex digits of event id"]}]}]

    ["REQ", "1uddc0a2fv", {"cache": ["user_profile", {"pubkey": "32-hex digits of pubkey id"}]}]

