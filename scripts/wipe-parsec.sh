#!/bin/sh
set -x
# kill parsec
pkill -9 -e -f runtime_locking_shardd
pkill -9 -e -f agentd
pkill -9 -e -f ticket_machined
# wipe state, meant to be launched from the same directory as parsec-run-local.sh
rm -rf runtime_locking_shard0_raft_* ticket_machine_raft_*
rm -rf logs/agentd.log logs/shardd.log logs/ticket_machined.log
