add_executable(mprpc_benchmark rpc_benchmark.cc)

target_include_directories(mprpc_benchmark PRIVATE ${PROJECT_SOURCE_DIR}/example)

target_link_libraries(mprpc_benchmark PRIVATE mprpc example_proto protobuf)
