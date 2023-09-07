#!/bin/bash

./build/tools/bench/parsec/py/py_bench --component_id=0 --ticket_machine0_endpoint=127.0.0.1:7777 --ticket_machine_count=1 --shard_count=1 --shard0_count=1 --shard00_endpoint=127.0.0.1:5556 --agent_count=1 --agent0_endpoint=127.0.0.1:8888
echo done
