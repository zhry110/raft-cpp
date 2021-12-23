include_directories(../raft)
add_subdirectory(../third-party/spdlog spdlog)
add_executable(Raft main.cpp Raft.cpp message/Message.cpp server/Server.cpp)
target_link_libraries(Raft spdlog)