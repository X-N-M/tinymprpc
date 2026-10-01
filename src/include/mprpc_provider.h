#pragma once
#include "mprpc_codec.h"
#include "mprpc_config.h"
#include "mprpc_header.pb.h"
#include "zookeeperutil.h"
#include <cstdint>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <muduo/net/InetAddress.h>
#include <memory>
#include <muduo/net/Callbacks.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>
#include <stdlib.h>
#include <unordered_map>
#include <google/protobuf/descriptor.h>
#include <memory>


class RpcProvider
{
public:
    void NotifyService(google::protobuf::Service* service);

    void Run();
private:
    muduo::net::EventLoop m_eventLoop;
    struct ServiceInfo
    {
        google::protobuf::Service* service;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> methods;
    };

    std::unordered_map<std::string, struct ServiceInfo> Service_Info;

    struct RpcCallContext
    {
        muduo::net::TcpConnectionPtr conn;
        uint64_t request_id{ 0 };
        std::unique_ptr<google::protobuf::Message> request;
        std::unique_ptr<google::protobuf::Message> response;
        int64_t deadline_unix_ms{0}; //协作式deadline，无法中断
    };

    void OnConnection(const muduo::net::TcpConnectionPtr& conn);
    void OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buffer, muduo::Timestamp);
    void SendRpcResponse(std::shared_ptr<RpcCallContext> context);
    
    bool SendRpcFrame(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::RpcStatusCode status, const std::string& error_text, const std::string& payload);
    bool SendRpcError(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::RpcStatusCode status, const std::string& error_text);
    bool DispatchRpcRequest(const muduo::net::TcpConnectionPtr& conn, const RpcFrame& frame);
    bool RegisterServicesToZookeeper(ZKClient* zk_client, const std::string& ip, uint16_t port, std::string& error);
    bool ParseFromConfig(std::string& ip, uint16_t& port, std::string& error);

};