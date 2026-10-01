set(SRC_LIST call_userservice.cc)

add_executable(consumer ${SRC_LIST})

target_link_libraries(consumer mprpc example_proto protobuf)
