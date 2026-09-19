#!/usr/bin/env python3
"""Print sanitized MUX audit events. This client never writes to the socket."""

import json
import socket


HOST = "127.0.0.1"
PORT = 4406


with socket.create_connection((HOST, PORT)) as sock:
    with sock.makefile("r", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            print(json.dumps(event, indent=2, sort_keys=True))
