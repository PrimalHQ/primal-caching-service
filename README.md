<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]


<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/PrimalHQ/primal-caching-service">
    <img src="https://primal.net/assets/logo_fire-409917ad.svg" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">Primal</h3>

  <p align="center">
    Primal’s caching service for Nostr connects to the specified set of relays, collects all events in real time, stores them locally, and makes them available to nostr clients through a web socket-based API.
    <br />
    <a href="https://github.com/PrimalHQ/primal-caching-service"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/PrimalHQ/primal-caching-service">View Demo</a>
    ·
    <a href="https://github.com/PrimalHQ/primal-caching-service/issues">Report Bug</a>
    ·
    <a href="https://github.com/PrimalHQ/primal-caching-service/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#usage">Usage</a></li>
        <li><a href="#api requests">Api requests</a></li>
      </ul>
    </li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Primal’s caching service for Nostr connects to the specified set of relays, collects all events in real time, stores them locally, and makes them available to nostr clients through a web socket-based API.

### Built With

[![Julia][Julia]][Julia-url]

[![C][C]][C-url]

[![Nix][Nix]][Nix-url]

[![Sqlite][Sqlite]][Sqlite-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

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

    ["REQ", "p0xren2axa", {"cache": ["feed", {"pubkey": "64-hex digits of pubkey id"}]}]

    ["REQ", "vqvv4vc6us", {"cache": ["thread_view", {"event_id": "64-hex digits of event id"}]}]

    ["REQ", "ay4if6pykg", {"cache": ["user_infos", {"pubkeys": ["64-hex digits of pubkey id"]}]}]

    ["REQ", "2t6z17orjp", {"cache": ["events", {"event_ids": ["64-hex digits of event id"]}]}]

    ["REQ", "1uddc0a2fv", {"cache": ["user_profile", {"pubkey": "64-hex digits of pubkey id"}]}]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Read more about contributions in [CONTRIBUTING.md](CONTRIBUTING.md).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

- pedja - [@pedja](https://primal.net/pedja)

Project Link: [https://github.com/PrimalHQ/primal-caching-service](https://github.com/PrimalHQ/primal-caching-service)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/PrimalHQ/primal-caching-service.svg?style=for-the-badge
[contributors-url]: https://github.com/PrimalHQ/primal-caching-service/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/PrimalHQ/primal-caching-service.svg?style=for-the-badge
[forks-url]: https://github.com/PrimalHQ/primal-caching-service/network/members
[stars-shield]: https://img.shields.io/github/stars/PrimalHQ/primal-caching-service.svg?style=for-the-badge
[stars-url]: https://github.com/PrimalHQ/primal-caching-service/stargazers
[issues-shield]: https://img.shields.io/github/issues/PrimalHQ/primal-caching-service.svg?style=for-the-badge
[issues-url]: https://github.com/PrimalHQ/primal-caching-service/issues
[license-shield]: https://img.shields.io/github/license/PrimalHQ/primal-caching-service.svg?style=for-the-badge
[license-url]: https://github.com/PrimalHQ/primal-caching-service/blob/master/LICENSE.txt
[product-screenshot]: https://primal.net/assets/primal_iphone-834937d2.png
[Julia]: https://img.shields.io/badge/julia-000000?style=for-the-badge&logo=julia&logoColor=white
[Julia-url]: https://julialang.org/
[C]: https://img.shields.io/badge/c-000000?style=for-the-badge&logo=c&logoColor=white
[C-url]: https://www.learn-c.org/
[Nix]: https://img.shields.io/badge/nix-000000?style=for-the-badge&logo=nixos&logoColor=white
[Nix-url]: https://sass-lang.com
[Sqlite]: https://img.shields.io/badge/sqlite-000000?style=for-the-badge&logo=sqlite&logoColor=white
[Sqlite-url]: https://www.sqlite.org/