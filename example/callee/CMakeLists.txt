set(SRC_LIST userservice.cc)

add_executable(provider ${SRC_LIST})

target_link_libraries(provider mprpc example_proto protobuf)
