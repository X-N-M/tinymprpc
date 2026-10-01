#include "mprpc_channel.h"
#include "logger.h"
#include "mprpc_client_connection.h"
#include "mprpc_codec.h"
#include "mprpc_header.pb.h"
#include "mprpc_app.h"
#include "mprpc_config.h"
#include "mprpc_options.h"
#include "zookeeperutil.h"
#include "mprpc_controller.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <charconv>
#include <vector>

MpRpcChannel::MpRpcChannel(std::size_t pool_size) : pool_size_(pool_size)
{
    if (pool_size_ == 0)
    {
        throw std::invalid_argument("MpRpcChannel pool size must be positive");
    }
}

void SetRpcError(google::protobuf::RpcController* controller,
                 const std::string& msg,
                 int err)
{
    char errtxt[1024];
    snprintf(errtxt, sizeof(errtxt), "%s errno:%d", msg.c_str(), err);
    controller->SetFailed(errtxt);
}

void MpRpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
    const google::protobuf::Message* request, google::protobuf::Message* response,
    google::protobuf::Closure* done)
{
    auto pending = std::make_shared<PendingCall>();

    pending->response = response;
    pending->controller = controller;
    pending->done = done;

    // 统一早期失败
    auto fail_before_handoff = [&pending](const std::string& reason)
        {
            const std::string error = reason.empty() ? "RPC call failed before handoff" : reason;
            pending->Finish(error);
        };

    if (method == nullptr)
    {
        fail_before_handoff("RPC method descriptor is null");
        return;
    }

    if (request == nullptr)
    {
        fail_before_handoff("RPC request is null");
        return;
    }

    if (response == nullptr)
    {
        fail_before_handoff("RPC response is null");
        return; //用户回调有可能释放 Channel，执行完成后继续访问 this 的成员会产生生命周期风险，所以必须return
    }

    pending->response_staging.reset(response->New());
    if (!pending->response_staging)
    {
        fail_before_handoff("failed to create rpc response staging message");
        return;
    }

    const google::protobuf::ServiceDescriptor* service = method->service();
 
    const std::string service_name = service->name();
    const std::string method_name = method->name();

    std::string request_payload;

    int64_t timeout_ms = mprpc::kDefaultRpcTimeoutMs;

    if (controller != nullptr)
    {
        if (auto* mprpc_controller = dynamic_cast<MprpcController*>(controller))
        {
            timeout_ms = mprpc_controller->TimeoutMs();
        }
    }

    const auto steady_deadlines = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeout_ms);

    const auto unix_deadline=std::chrono::system_clock::now() +
        std::chrono::milliseconds(timeout_ms);

    pending->deadline = steady_deadlines;

    if (!request->SerializeToString(&request_payload))
    {
        fail_before_handoff("serialize RPC request failed");
        return;
    }

    mprpc::RpcHeader request_meta;
    const auto unix_deadline_ms = std::chrono::duration_cast<std::chrono::milliseconds>
        (unix_deadline.time_since_epoch()).count();
    request_meta.set_method_name(method_name);
    request_meta.set_service_name(service_name);
    request_meta.set_deadline_unix_ms(unix_deadline_ms);


    std::string request_meta_data;
    if (!request_meta.SerializeToString(&request_meta_data))
    {
        fail_before_handoff("serialize RPC request meta failed");
        return;
    }


    std::string error;
    if (!discovery_.Start(error, steady_deadlines))
    {
        fail_before_handoff(error);
        return;
    }

    std::vector<RpcEndpoint> endpoints;

    // 启动zookeeper
    if (!discovery_.Resolve(service_name, method_name,
        &endpoints, error, steady_deadlines))
    {
        fail_before_handoff(error);
        return;
    }

    const std::string route = service_name + "/" + method_name;
    RpcEndpoint selected;
    if (!picker_.Pick(route, endpoints,
        selected, error))
    {
        fail_before_handoff(error);
        return;
    }



    const uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
    RpcFrame reqeust_frame;
    reqeust_frame.message_type = RpcMessageType::kRequest;
    reqeust_frame.flags = 0;
    reqeust_frame.request_id = request_id;
    reqeust_frame.meta = std::move(request_meta_data);
    reqeust_frame.payload = std::move(request_payload);

    std::string encoded;
    if (!RpcCodec::Encode(reqeust_frame, &encoded))
    {
        fail_before_handoff("encode RPC request frame failed");
        return;
    }

    //预先保留done的布尔值，不再依赖它的生命周期
    const bool synchronous = done == nullptr;

    // 拿到连接池，再从连接池拿到连接
    auto pool = GetOrCreateConnection(selected);
    auto connection = SelectConnection(pool);

    if (!connection)
    {
        fail_before_handoff("no available RPC connection");
        return;
    }

    const bool hand_off = connection->StartCall(request_id, encoded, pending,
        error, steady_deadlines);

    if (!hand_off)
    {
        discovery_.Invalidate(service_name, method_name);
        fail_before_handoff(error);
        return;
    }

    if (synchronous)
    {
        // 引入超时逻辑后，如果根据Expire返回值判断是否超时，这里其实存在很小的窗口会发生use-after-free的错误
        const bool completed = pending->WaitUntil(pending->deadline);
        if (!completed)
        {
            connection->ExpirePending(request_id);
            pending->Finish("RPC deadline exceeded");
            
        }
    }

}

MpRpcChannel::~MpRpcChannel()
{
    std::unordered_map<std::string, std::shared_ptr<MpRpcChannel::ConnectionPool>> pools;

    {
        std::lock_guard<std::mutex> lock(connections_mutex_);

        pools.swap(connection_pools);
    }

    for (auto& [endpoint, pool] : pools)
    {
        if (!pool)
        {
            continue;
        }

        for (auto& connection : pool->connections)
        {
            connection->Close();
        }
    }
}


bool ServiceDiscovery::Resolve(const std::string& service_name, const std::string& method_name,
    std::vector<RpcEndpoint>* endpoints, std::string& error,
    std::chrono::steady_clock::time_point deadline)
{

    error.clear();

    auto expired = [&deadline]
        {
            return std::chrono::steady_clock::now() >= deadline;
        };

    if (expired())
    {
        error = "RPC deadline exceeded";
        return false;
    } //先制作调用前的检查，过后再进行升级
    
    const std::string path = "/" + service_name + "/" + method_name;
    if (endpoints == nullptr)
    {
        return false;
    }
  
 
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        const auto it = endpoint_cache_.find(path);

        if (it != endpoint_cache_.end())
        {
            *endpoints = it->second;
            return true;
        }
    }


    std::vector<std::string> children;
    std::string host_data;
    if (!zk_client_.GetChildren(path, &children, error))
    {
        return false;
    }

    std::vector<RpcEndpoint> result;

    for (const auto& child : children)
    {
        std::string endpoint_text;

        std::string child_path = path + "/" + child;

        if (!zk_client_.GetData(child_path, endpoint_text, error))
        {
            RPC_LOG_ERR(
            "skip unavailable provider node: path=%s, reason=%s",
            child_path.c_str(),
            error.c_str());
            continue; //现阶段先跳过可能覆盖错误的问题
        }

        RpcEndpoint endpoint;
        if (!ParseEndpoint(endpoint_text, &endpoint, error))
        {
            RPC_LOG_ERR(
            "parse RPC endpoint failed: path=%s reason=%s",
            child_path.c_str(),
            error.c_str());
            continue;
        }

        result.push_back(std::move(endpoint));
    }

    if (result.empty())
    {
        error = "no available RPC endpoint";
        return false;
    }

      //按照key的字典序给得到的结果排序，因为zk返回数据不可靠，防止程序误以为列表改变
    std::sort(result.begin(), result.end(), [](const RpcEndpoint& lhs,
        const RpcEndpoint& rhs)
        {
            return lhs.Key() < rhs.Key();
        });

    
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);

        endpoint_cache_[path] = result;
        *endpoints = result;
    }

    RPC_LOG_INFO(
        "event=route_resolved path=%s endpoint_count=%zu",
        path.c_str(),
        result.size());
    
    return true;
}

// 创建连接池函数
std::shared_ptr<MpRpcChannel::ConnectionPool> MpRpcChannel::GetOrCreateConnection(const RpcEndpoint& endpoint)
{
    std::string key = endpoint.Key();

    std::lock_guard<std::mutex> lock(connections_mutex_);

    auto it = connection_pools.find(key);
    if (it != connection_pools.end())
    {
        return it->second;
    }

    auto pool = std::make_shared<ConnectionPool>();
    pool->connections.reserve(pool_size_);

    for (std::size_t i = 0; i < pool_size_; ++i)
    {
        pool->connections.emplace_back(std::make_shared<RpcClientConnection>(endpoint));
    }

    connection_pools.emplace(key, pool);
    return pool;
}

//连接池的连接选择器
std::shared_ptr<RpcClientConnection> MpRpcChannel::SelectConnection(const std::shared_ptr<ConnectionPool>& pool)
{
    if (!pool || pool->connections.empty())
    {
        return nullptr;
    }

    const std::size_t index = pool->next_index_.fetch_add(1, std::memory_order_relaxed);
    return pool->connections[index % pool->connections.size()];
}

bool ServiceDiscovery::ParseEndpoint(const std::string& host_data, RpcEndpoint* endpoint, std::string& error)
{
    error.clear();
    if (endpoint == nullptr)
    {
        error = "endpoint output is null";
        return false;
    }
    
    const size_t it = host_data.rfind(":");

    if (it == std::string::npos || it == 0 || it + 1 >= host_data.size())
    {
        error = "invalid RPC endpoint: " + host_data;
        return false;
    }

      const std::string ip =
        host_data.substr(0, it);

    const char* port_begin =
        host_data.data() + it + 1;

    const char* port_end =
        host_data.data() + host_data.size();

    uint32_t parsed_port = 0;

    const auto result = std::from_chars(
        port_begin,
        port_end,
        parsed_port);

    if (result.ec != std::errc{} || result.ptr != port_end || parsed_port == 0 || parsed_port > 65535||ip.empty())
    {
        error = "invalid RPC endpoint port: " + host_data;
        return false;
    }

    endpoint->ip = ip;
    endpoint->port = static_cast<uint16_t>(parsed_port);
    return true;
}

bool ServiceDiscovery::Start(std::string &error, std::chrono::steady_clock::time_point deadline)
{
    if(!zk_client_.Start(error, deadline))
    {
        return false;
    }

    return true;
}

void ServiceDiscovery::Invalidate(const std::string& service_name, const std::string& method_name)
{
    const std::string path ="/" + service_name + "/" + method_name;
    std::lock_guard<std::mutex> lock(cache_mutex_);

    endpoint_cache_.erase(path);
}