## Configuration
# YACA: Yet Another Chat Application
*YACA* is *yet another chat application* aiming to cover the project requirements: Dynamic discovery, leader election, reliable ordered multicast, and Byzantine fault tolerance.

YACA uses the client-server architecture to enable clients to participate in one global chat room, where clients are able to respond to previous messages (i.e. the happened-before relation is respected). In general, our strategy is to utilize powerful building blocks like *total-ordered reliable multicast*, *view-synchronous group communication*, and the *(Max-)Phase-King Algorithm* to realize this project in a Byzantine fault-tolerant manner.

## Configuration
Adjust the [configuration](https://github.com/eggersn/YACA/blob/master/config/config.json) to fit your purposes. Most likely, you will have to modify the initial view by setting the *"ip_addr"* to your own.
Afterwards, run
```bash
$ python generate_initial.py
```
to generate the [initial view](https://github.com/eggersn/YACA/blob/master/config/initial).

## Run
To run YACA, start by spawning the initial servers (as configured above):
```bash
$ python launch.py -i
```
By removing the *-i* option, you can also start server instances on another device in your local network.

Afterwards, you can launch multiple clients by running:
```bash
$ python client.py
```

## Documentation
Theoretical background can be found [here](https://github.com/eggersn/YACA/blob/master/documentation/final_report.pdf). See Appendix A, for a detailed description of the proposed Max-Phase-King algorithm.

## License [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
This project is distributed under the MIT license. Please see LICENSE file.
