add_executable(evm_bench evm_bench.cpp
                         rpc_client.cpp
                         contracts.cpp
                         main.cpp)
target_link_libraries(evm_bench evm_runner
                                parsec
                                agent
                                runners
                                rpc
                                network
                                json_rpc_http
                                serialization
                                common
                                crypto
                                secp256k1
                                ${JSON_LIBRARY}
                                ${CURL_LIBRARY}
                                ${MHD_LIBRARY}
                                ${CMAKE_THREAD_LIBS_INIT}
                                ${OPENSSL_LIBRARIES})
