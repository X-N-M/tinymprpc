#pragma once
#include "mprpc_client_connection.h"
#include "mprpc_round_robin_picker.h"
#include "zookeeperutil.h"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class ServiceDiscovery
{
public:
    bool Start(std::string& error, std::chrono::steady_clock::time_point deadline);

    bool Resolve(const std::string& service_name, const std::string& method_name,
        std::vector<RpcEndpoint>* endpoints, std::string& error,
        std::chrono::steady_clock::time_point deadline);

    void Invalidate(const std::string& service_name, const std::string& method_name);

private:
    bool ParseEndpoint(const std::string& host_data, RpcEndpoint* endpoint, std::string& error);
    
    std::mutex cache_mutex_;
    ZKClient zk_client_;

    std::unordered_map<std::string,
        std::vector<RpcEndpoint>> endpoint_cache_;
};

class MpRpcChannel final : public google::protobuf::RpcChannel
{
public:

    explicit MpRpcChannel(std::size_t pool_size = 1);
    ~MpRpcChannel() override;

    void CallMethod(const google::protobuf::MethodDescriptor* method,
        google::protobuf::RpcController* controller, const google::protobuf::Message* request,
        google::protobuf::Message* response, google::protobuf::Closure* done) override;

private:
    struct ConnectionPool
    {
        std::vector<std::shared_ptr<RpcClientConnection>> connections;
        std::atomic<std::size_t> next_index_{ 0 };
    };

    std::shared_ptr<ConnectionPool> GetOrCreateConnection(const RpcEndpoint& endpoint);
    std::shared_ptr<RpcClientConnection> SelectConnection(const std::shared_ptr<ConnectionPool>& pool);
    
private:
    ServiceDiscovery discovery_;
    RoundRobinPicker picker_;

    const std::size_t pool_size_;
    
    std::atomic<uint64_t> next_request_id_{ 1 }; //多个并发rpc不同身份
    std::unordered_map<std::string, std::shared_ptr<ConnectionPool>> connection_pools; //复用endpoint长连接
    std::mutex connections_mutex_;

};