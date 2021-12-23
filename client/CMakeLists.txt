cmake_minimum_required(VERSION 3.21)
set(CMAKE_CXX_STANDARD 17)
add_executable(client Client.cpp)
target_link_libraries(client spdlog)
