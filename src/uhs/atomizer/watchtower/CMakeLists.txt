project(watchtower)

add_library(watchtower controller.cpp
                       watchtower.cpp
                       status_update.cpp
                       error_cache.cpp
                       block_cache.cpp
                       tx_error_messages.cpp
                       status_update_messages.cpp
                       messages.cpp
                       client.cpp)

add_executable(watchtowerd watchtowerd.cpp)
target_link_libraries(watchtowerd watchtower
                                  archiver
                                  atomizer
                                  raft
                                  transaction
                                  network
                                  common
                                  serialization
                                  crypto
                                  ${LEVELDB_LIBRARY}
                                  ${NURAFT_LIBRARY}
                                  secp256k1
                                  ${CMAKE_THREAD_LIBS_INIT})
