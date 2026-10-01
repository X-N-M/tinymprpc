protobuf_generate_cpp(USER_PROTO_SRCS USER_PROTO_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/user.proto)

add_library(example_proto STATIC ${USER_PROTO_SRCS} ${USER_PROTO_HDRS})
target_include_directories(example_proto PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}"
    ${Protobuf_INCLUDE_DIRS})
target_link_libraries(example_proto PUBLIC protobuf)

add_subdirectory(callee)
add_subdirectory(caller)
