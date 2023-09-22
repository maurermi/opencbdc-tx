add_executable(py_bench py_bench.cpp) #wallet.cpp py_bench.cpp)
target_link_libraries(py_bench  broker
                                directory
                                runtime_locking_shard
                                ticket_machine
                                parsec
                                agent
                                rpc
                                network
                                common
                                serialization
                                crypto
                                secp256k1
                                python3.10
                                ${CMAKE_THREAD_LIBS_INIT})

include_directories(/usr/include/python3.10)