add_executable(lua_bench lua_bench.cpp
                         wallet.cpp)
target_link_libraries(lua_bench broker
                                directory
                                runtime_locking_shard
                                ticket_machine
                                raft
                                parsec
                                agent
                                rpc
                                network
                                common
                                serialization
                                crypto
                                secp256k1
                                ${NURAFT_LIBRARY}
                                ${LEVELDB_LIBRARY}
                                ${LUA_LIBRARY}
                                ${CMAKE_THREAD_LIBS_INIT})
