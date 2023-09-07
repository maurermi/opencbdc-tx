project(py_runner)

add_library(py_runner impl.cpp
                       py_server.cpp)

target_link_libraries(py_runner python3.10)
include_directories(/usr/include/python3.10)