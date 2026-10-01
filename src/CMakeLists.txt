set(MPRPC_SOURCES
    logger.cc
    mprpc_app.cc
    mprpc_channel.cc
    mprpc_client_connection.cc
    mprpc_codec.cc
    mprpc_config.cc
    mprpc_controller.cc
    mprpc_provider.cc
    mprpc_round_robin_picker.cc
    zookeeperutil.cc)

protobuf_generate_cpp(MPRPC_PROTO_SRCS MPRPC_PROTO_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/mprpc_header.proto)

set(MPRPC_SOURCES ${MPRPC_SOURCES} ${MPRPC_PROTO_SRCS} ${MPRPC_PROTO_HDRS})

add_library(mprpc ${MPRPC_SOURCES})

target_include_directories(mprpc PUBLIC
    "${PROJECT_SOURCE_DIR}/src/include"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}"
    ${Protobuf_INCLUDE_DIRS})

target_link_libraries(mprpc PUBLIC
    muduo_net
    muduo_base
    protobuf
    zookeeper_mt
    pthread)
