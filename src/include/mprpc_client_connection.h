#pragma once
#include "mprpc_codec.h"
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <atomic>
#include <muduo/net/Buffer.h>
#include <mutex>
#include <sys/types.h>
#include <thread>
#include <string>
#include <unordered_map>

struct PendingCall
{
    google::protobuf::Message* response{ nullptr };
    google::protobuf::RpcController* controller{ nullptr };
    google::protobuf::Closure* done{ nullptr };

    std::unique_ptr<google::protobuf::Message> response_staging;
    std::mutex mutex;
    std::condition_variable condition;
    bool completed{ false };
    std::string error;

    std::chrono::steady_clock::time_point deadline; //结果边界进行软deadline, 长时间阻塞的资源等待使用硬deadline

    // 完成行为属于 PendingCall 自己，而不属于 Channel 或 Connection
    bool Finish(const std::string& reason, const google::protobuf::Message* decoded_response=nullptr);
    bool WaitUntil(std::chrono::steady_clock::time_point deadline);
};

enum class ConnectionState
{
    kDisconnected,
    kConnecting,
    kConnected,
    kClosing
};

struct RpcEndpoint
{
    std::string ip;
    uint16_t port{0};

    std::string Key() const;
};

class RpcClientConnection
{
public:
    explicit RpcClientConnection(RpcEndpoint endpoint);
    ~RpcClientConnection();

    /* 返回true只代表“已接管”不代表rpc成功，也不保证请求成功发送，Connection 保证 PendingCall 最终恰好完成一次，
    调用方不得再次完成或执行 done, 即使发送失败，connection仍负责Finish
    返回false，connection并未接管该pendingcall（参数非法，建连失败，request_id重复）
    Connection 不会完成 PendingCall，
    调用方负责将其完成为失败
    */
    bool StartCall(uint64_t request_id, const std::string& encoded,
        std::shared_ptr<PendingCall> pending, std::string& error,
            std::chrono::steady_clock::time_point deadline);
    bool ExpirePending(uint64_t request_id);
    
    void Close();
    
private:
    bool SendAll(int socket_fd,const std::string& data, std::string& error, std::chrono::steady_clock::time_point deadline);
    bool EnsureConnected(std::string& error,
        std::chrono::steady_clock::time_point deadline);
    void ReceiveLoop(uint64_t generation);

    /*返回true:当前响应已经得到处理，连接状态仍然可信
    即使 RPC 返回 SERVICE_NOT_FOUND、INTERNAL_ERROR
    等调用级错误，也应返回 true，因为只影响对应请求。
    返回false则是连接级或协议级错误，应该调用BreakConnection并退出
    */
    bool HandleResponse(const RpcFrame& frame, std::string& error);



    void FailAllPending(const std::string& error);

    std::shared_ptr<PendingCall> TakePending(uint64_t request_id);
    // void FinishPending(const std::shared_ptr<PendingCall>& pending, const std::string& error); 弃用单独的处理
    void BreakConnection(const std::string& reason, uint64_t expected_generation);

    std::mutex state_mutex_;
    int socket_fd{ -1 };

    std::condition_variable state_condition_;
    ConnectionState state_{ ConnectionState::kDisconnected };

    std::atomic<bool> stop_requested_{ false };

    std::thread receive_thread_;

    //pending_mutex_存在一个竞态
    // 响应处理与超时清理竞争 pending 所有权；
    // 先取得所有权的一方决定结果；
    // Finish 保证终态只发布一次。
    std::mutex pending_mutex_;
    std::timed_mutex send_mutex_;

    const size_t max_inflight_{ 1024 };
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending_calls_;
    muduo::net::Buffer receive_buffer_;
    RpcEndpoint endpoint_;
    uint64_t connection_generation_{ 0 }; //连接代次，由state_mutex_保护

private:
    void TimeoutLoop();//监测异步超时的线程函数

    std::thread time_out_thread_;
    std::condition_variable pending_condition_;
};