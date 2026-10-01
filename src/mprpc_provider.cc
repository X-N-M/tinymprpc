#include "mprpc_provider.h"
#include "mprpc_codec.h"
#include "mprpc_config.h"
#include "mprpc_header.pb.h"
#include "logger.h"
#include "mprpc_app.h"
#include <charconv>
#include <chrono>
#include <cstdint>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include "zookeeperutil.h"
#include <cstring>


void RpcProvider::NotifyService(google::protobuf::Service* service)
{
    //将对应服务本体和方法描述器放到Info结构体中
    ServiceInfo Info;
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor> methods;
    const google::protobuf::ServiceDescriptor* pService = service->GetDescriptor();
    Info.service = service;
    std::string service_name = pService->name();

    int method_conut = pService->method_count();

    for (int i = 0; i < method_conut; ++i)
    {
        const google::protobuf::MethodDescriptor* pMethod = pService->method(i);
        std::string method_name = pMethod->name();
        Info.methods.emplace(method_name, pMethod);
    }

    
    Service_Info.emplace(service_name, Info);

}
//启动rpc节点，开启远程网络服务
void RpcProvider::Run()
{
    uint16_t port;
    std::string ip;
    std::string error;

    if(!ParseFromConfig(ip, port, error))
    {
        RPC_LOG_ERR("Parse From Config error, reason=%s", error.c_str());
        return;
    }

    muduo::net::InetAddress address(ip, port);
    
    // 创建TcpServer对象
    muduo::net::TcpServer server(&m_eventLoop, address, "RpcProvider");
    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    server.setThreadNum(4);
    server.start(); //启动网络服务, 先监听确保注册后客户端可以立即连接


    ZKClient zkCli;
    const auto zk_connect_deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (!zkCli.Start(error, zk_connect_deadline))
    {
        RPC_LOG_ERR("start ZooKeeper failed: %s", error.c_str());
        return;
    }

    if (!RegisterServicesToZookeeper(&zkCli, ip, port, error))
    {
        RPC_LOG_ERR("RegisterServicesToZookeeper failed, reason=%s ", error.c_str());
        return;
    }
    
    m_eventLoop.loop(); //阻塞等待
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr& conn)
{   
    if (!conn->connected())
    {
        conn->shutdown();
    }
}

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buffer,muduo::Timestamp)
{
    while (true)
    {
        RpcFrame frame;
        std::string error_text;

        const DecodeStatus status = RpcCodec::Decode(buffer, &frame, error_text);

        if (status == DecodeStatus::kNeedMoreData)
        {
            return;
        }

        // 帧边界或协议本身不可信，无法解析
        if (status == DecodeStatus::kProtocolError)
        {
            RPC_LOG_ERR(
                "event=protocol_error peer=%s reason=%s",
                conn->peerAddress().toIpPort().c_str(),
                error_text.c_str());
            
            conn->forceClose(); //协议边界已不可信，强行结束
            return;
        }
        // provider只接受request
        if (frame.message_type != RpcMessageType::kRequest)
        {
            RPC_LOG_ERR(
                "event=unexpected_message_type peer=%s",
                conn->peerAddress().toIpPort().c_str());
            conn->shutdown();
            return;
        }
        if (!DispatchRpcRequest(conn, frame)) //分发成功, 继续解析下一个粘包帧
        {
            return;
        }
    }
}

void RpcProvider::SendRpcResponse(std::shared_ptr<RpcCallContext> context)
{
    if (context->deadline_unix_ms > 0)
    {
        const auto now = std::chrono::system_clock::now();
        const int64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

        if (now_ms >= context->deadline_unix_ms)
        {
            SendRpcError(
                context->conn,
                context->request_id,
                mprpc::RPC_STATUS_DEADLINE_EXCEEDED,
                "RPC deadline exceeded during processing");
            return;
        }
    }

    std::string payload;
    if (!context->response->SerializeToString(&payload))
    {
        SendRpcError(context->conn, context->request_id,
            mprpc::RPC_STATUS_RESPONSE_SERIALIZE_ERROR, "failed to serialize RPC response");
        return;
    }

    SendRpcFrame(context->conn, context->request_id, mprpc::RPC_STATUS_OK, "", payload);
}

bool RpcProvider::SendRpcFrame(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id,
    mprpc::RpcStatusCode status, const std::string& error_text, const std::string& payload)
{
    mprpc::RpcResponseMeta response_meta;
    response_meta.set_rpc_status_code(status);
    response_meta.set_error_text(error_text);

    std::string meta_data;
    if (!response_meta.SerializeToString(&meta_data))
    {
        RPC_LOG_ERR("serialize RPC response meta failed");
        conn->shutdown();
        return false;
    }

    RpcFrame frame;
    frame.message_type = RpcMessageType::kResponse;
    frame.flags = 0;
    frame.meta = std::move(meta_data);
    frame.request_id = request_id;
    frame.payload = payload;

    std::string encoded;
    if (!RpcCodec::Encode(frame, &encoded))
    {
        RPC_LOG_ERR("encode RPC response frame failed");
        conn->shutdown();
        return false;
    }

    conn->send(encoded);
    return true;
}

bool RpcProvider::SendRpcError(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::RpcStatusCode status, const std::string& error_text)
{
    if (!SendRpcFrame(conn, request_id, status, error_text, ""))
    {
        return false;
    }

    return true;
}

bool RpcProvider::DispatchRpcRequest(const muduo::net::TcpConnectionPtr& conn, const RpcFrame& frame)
{
    mprpc::RpcHeader rpc_header;

    if (!rpc_header.ParseFromString(frame.meta))
    {
        return SendRpcError(conn, frame.request_id, mprpc::RPC_STATUS_BAD_REQUEST, "invalid RPC request meta");
    }

    const int64_t deadline_ms = rpc_header.deadline_unix_ms();
    if (deadline_ms > 0)
    {
        const auto now = std::chrono::system_clock::now();
        const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>
            (now.time_since_epoch()).count();

        if (now_ms >= deadline_ms)
        {
            return SendRpcError(conn, frame.request_id,
                mprpc::RPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded before dispaatch");
        }
    }

    const std::string& service_name = rpc_header.service_name();
    const std::string& method_name = rpc_header.method_name();

    
    if (service_name.empty() || method_name.empty())
    {
        return SendRpcError(conn, frame.request_id, mprpc::RPC_STATUS_BAD_REQUEST, "method or service not found");
    }
    
    auto service_it = Service_Info.find(service_name);
    if (service_it == Service_Info.end())
    {
        return SendRpcError(conn, frame.request_id, mprpc::RPC_STATUS_SERVICE_NOT_FOUND, service_name+"not found");
    }


    auto method_it = service_it->second.methods.find(method_name);
    if (method_it == service_it->second.methods.end())
    {
        return SendRpcError(conn, frame.request_id, mprpc::RPC_STATUS_METHOD_NOT_FOUND, method_name+"not found");
    }

    google::protobuf::Service* service = service_it->second.service;
    const google::protobuf::MethodDescriptor* method = method_it->second;

    auto context = std::make_shared<RpcCallContext>();
    context->conn = conn;
    context->request_id = frame.request_id;
    context->deadline_unix_ms = rpc_header.deadline_unix_ms();
    context->request.reset(service->GetRequestPrototype(method).New());
    context->response.reset(service->GetResponsePrototype(method).New());

    if (!context->request->ParseFromString(frame.payload))
    {
        return SendRpcError(conn,frame.request_id,mprpc::RPC_STATUS_REQUEST_PARSE_ERROR,"failed to parse RPC request payload");
    }

    google::protobuf::Closure* done = google::protobuf::NewCallback<RpcProvider, std::shared_ptr<RpcCallContext>>(this, &RpcProvider::SendRpcResponse, context);

    service->CallMethod(method, nullptr, context->request.get(), context->response.get(), done);
    return true;
}

bool RpcProvider::RegisterServicesToZookeeper(ZKClient* zk_client, const std::string& ip, uint16_t port, std::string& error)
{
    if (zk_client == nullptr)
    {
        error = "zk_client is null";
        return false;
    }

    error.clear();

    if (ip.empty() || port == 0)
    {
        error = "invalid endpoint";
        return false;
    }

    const std::string endpoint = ip + ":" + std::to_string(port);

    for (const auto& service_entry : Service_Info)
    {
        const std::string service_name = service_entry.first;
        const std::string service_path = "/" + service_name;

        if (!zk_client->CreatePersistent(service_path, "", error))
        {
            error = "create service path failed, path: " + service_path + ", reason=" + error;
            return false;
        }

        for (const auto& method_entry : service_entry.second.methods)
        {
            const std::string method_name = method_entry.first;
            const std::string method_path = service_path + "/" + method_name;

            if (!zk_client->CreatePersistent(method_path, "", error))
            {
                error = "create method path failed, path: " + method_path + ", reason=" + error;
                return false;
            }

            std::string created_path;
            if (!zk_client->CreateEphemeralSequential(method_path + "/instance-", endpoint, created_path, error))
            {
                error = "create method-instance path fail, path: " + method_path + "/instance-" + ". reason=" + error;
                return false;
            }


            RPC_LOG_INFO("registered provider: service=%s, method=%s, endpoint=%s, path=%s", service_name.c_str(),
                method_name.c_str(), endpoint.c_str(), created_path.c_str());
        }


    }

    return true;
}

bool RpcProvider::ParseFromConfig(std::string& ip, uint16_t& port, std::string& error)
{
    static MpRpcConfig config = MpRpcApplication::GetInstance().GetConfig();
    ip = config.Load("rpcserverip");

    if (ip.empty())
    {
        error = "ip is empty";
        return false;
    }

    std::string text = config.Load("rpcserverport");
    if (text.empty())
    {
        error = "port is empty";
        return false;
    }


    const char* begin = text.data();
    const char* end = text.data() + text.size();

    unsigned int value = 0;
    const std::from_chars_result result = std::from_chars(begin,
        end, value);

    if (result.ec == std::errc::invalid_argument)
    {
        error = "port is not a number";
        return false;
    }

    if(result.ec == std::errc::result_out_of_range)
    {
        error = "port is out of range";
        return false;
    }

    if (result.ptr != end)
    {
        error = "port contains invalid characters";
        return false;
    }

    if (value == 0 || value > 65535)
    {
        error = "port must be in range[1, 65535]";
        return false;
    }

    port = static_cast<uint16_t>(value);

    return true;
}