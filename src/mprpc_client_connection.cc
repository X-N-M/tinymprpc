#include "mprpc_client_connection.h"
#include "mprpc_codec.h"
#include "logger.h"
#include "mprpc_codec.h"
#include "mprpc_header.pb.h"
#include <algorithm>
#include <arpa/inet.h>
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <asm-generic/socket.h>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <limits>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <fcntl.h>
#include <poll.h>

namespace
{
    using SteadyDeadline = std::chrono::steady_clock::time_point;
    bool Expired(SteadyDeadline deadline)
    {
        return std::chrono::steady_clock::now() >= deadline;
    }

    int RemainPollTimeoutMs(std::chrono::steady_clock::time_point deadline)
    {
        const auto now = std::chrono::steady_clock::now();

        if (now >= deadline)
        {
            return 0;
        }

        const auto remain = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);

        if (remain.count() > std::numeric_limits<int>::max())
        {
            return std::numeric_limits<int>::max();
        }

        return static_cast<int>(remain.count());
    }

    bool NonBlock_fd(int& fd, const int& flags, std::string& error)
    {
        const int original_flags = ::fcntl(fd, F_GETFL, 0);

        if (original_flags < 0)
        {
            const int save_errno = errno;
            error = "get socket flags failed:" + std::string(std::strerror(save_errno));
            return false;
        }

        if (::fcntl(fd, F_SETFL, original_flags | flags))
        {
            const int save_errno = errno;
            error = "set socket flags failed:" + std::string(std::strerror(save_errno));
            return false;
        }

        return true;
    }

    bool Block_fd(int& fd, std::string& error)
    {
                // 如果成功则修改回来
        const int current_flags = ::fcntl(fd, F_GETFL, 0);
        if (current_flags < 0 ||
            ::fcntl(fd, F_SETFL, current_flags & ~O_NONBLOCK) < 0)
        {
            const int saved_errno = errno;
            error = "restore blocking mode failed: " +
                    std::string(std::strerror(saved_errno));
            return false;
        }

        return true;
    }

}

std::string RpcEndpoint::Key() const
{
    return ip + ":" + std::to_string(port);
}

RpcClientConnection::RpcClientConnection(RpcEndpoint endpoint):endpoint_(std::move(endpoint))
{
    time_out_thread_=std::thread(&RpcClientConnection::TimeoutLoop, this);
}

RpcClientConnection::~RpcClientConnection()
{
    Close();
}

std::shared_ptr<PendingCall> RpcClientConnection::TakePending(uint64_t request_id)
{
    std::lock_guard<std::mutex> lock(pending_mutex_);

    auto it = pending_calls_.find(request_id);
    if (it == pending_calls_.end())
    {
        return nullptr;
    }

    auto pending = it->second;
    pending_calls_.erase(it);
    return pending;
}

// void RpcClientConnection::FinishPending(const std::shared_ptr<PendingCall>& pending, const std::string& error)
// {
//     google::protobuf::Closure* done = nullptr;
//     {
//         std::lock_guard<std::mutex> lock(pending->mutex);
//         if (pending->completed)
//         {
//             return;
//         }

//         pending->error = error;

//         if (!error.empty() && pending->controller != nullptr)
//         {
//             pending->controller->SetFailed(error);
//         }

//         pending->completed = true;
//         done = pending->done;
//     }

//     pending->condition.notify_all();
//     if (done != nullptr)
//     {
//         done->Run();
//     }
// }

void RpcClientConnection::FailAllPending(const std::string& error)
{
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> tmp;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        std::swap(tmp, pending_calls_);
    }

    for (auto& [id, pending] : tmp)
    {
        pending->Finish(error);
    }
}

bool RpcClientConnection::SendAll(int socket_fd, const std::string& data,
    std::string& error, std::chrono::steady_clock::time_point deadline)
{
    error.clear();
    size_t sent = 0;

    while (sent < data.size())
    {
        if (Expired(deadline))
        {
            error = "RPC deadline exceeded during send";
            return false;
        }
        
        const ssize_t count = send(socket_fd, data.data() + sent, 
            data.size() - sent, MSG_NOSIGNAL|MSG_DONTWAIT);

        if (count > 0)
        {
            sent += static_cast<size_t>(count);
            continue;
        }

        if (count < 0 && errno == EINTR)
        {
            continue;
        }

        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            pollfd event{};
            event.fd = socket_fd;
            event.events = POLLOUT;
            const int timeout_ms = RemainPollTimeoutMs(deadline);

            if (timeout_ms == 0)
            {
                error = "RPC deadline exceeded during send";
                return false;
            }

            auto result = ::poll(&event, 1, timeout_ms);

            if (result == 0)
            {
                error = "RPC deadline exceeded during send";
                return false;
            }

            if (result < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                error = "poll for send failed:" + std::string(std::strerror(errno));
                return false;
            }

            if (event.revents & (POLLERR | POLLHUP | POLLNVAL)) 
            {
                error = "socket became unavaliable during send";
                return false;
            }

            continue;
        }

        if (count == 0)
        {
            error = "send return ed zero before frame completed";
        }
        else
        {
            const int save_errno = errno;
            error = "send failed:" + std::string(std::strerror(save_errno));
        }

        return false;
    }

    return true;
}

bool RpcClientConnection::EnsureConnected(std::string& error, std::chrono::steady_clock::time_point deadline)
{
    error.clear();
    std::thread stale_receiver;

    {
        std::unique_lock<std::mutex> lock(state_mutex_);

        //等待直到不是正在连接状态
        const bool state_ready = state_condition_.wait_until(lock, deadline,
            [this] { return state_ != ConnectionState::kConnecting; });

        if (!state_ready)
        {
            error = "RPC deadline exceeded";
            return false;
        }

        if (state_ == ConnectionState::kConnected)
        {
            return true;
        }

        if (state_ == ConnectionState::kClosing || stop_requested_.load())
        {
            error = "connection is closing";
            return false;
        }

        if (receive_thread_.joinable())
        {
            if (receive_thread_.get_id() == std::this_thread::get_id())
            {
                error = "can not reconnected from receviver thread";
                return false;
            }
            stale_receiver = std::move(receive_thread_);
        }

        state_ = ConnectionState::kConnecting;
    }

    if (stale_receiver.joinable())
    {
        stale_receiver.join();
    }
   
    // 临时函数发布错误
    auto publish_failure = [this]
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (state_ == ConnectionState::kConnecting)
        {
            state_ = ConnectionState::kDisconnected;
        }
        state_condition_.notify_all();
    };
    
    if (Expired(deadline))
    {
        error = "RPC deadline exceeded before connect";
        publish_failure();
        return false;  //软检查，保证join返回后如果超时不继续做昂贵操作
    }
    
    //基础TCP编程
    if (endpoint_.ip.empty() || endpoint_.port == 0)
    {
        error = "invalid RPC endpoint" + endpoint_.Key();
        publish_failure();
        return false;;
    }

    int new_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (new_fd < 0)
    {
        const int saved_errno = errno;
        error = "create socket failed: " + std::string(std::strerror(saved_errno));
        publish_failure();
        return false;
    }
    
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint_.port);

    if (inet_pton(AF_INET, endpoint_.ip.c_str(), &address.sin_addr) != 1)
    {
        error = "invalid IPv4 address:" + endpoint_.ip;
        ::close(new_fd);
        publish_failure();
        return false;
    }

    if (!NonBlock_fd(new_fd, O_NONBLOCK,error))
    {
        ::close(new_fd);
        publish_failure();
        return false;
    }

    const int connected_res = connect(new_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));

    if (connected_res < 0)   
    {
        const int connect_errno = errno;
     
        while (true)
        {
            const auto timeout_ms = RemainPollTimeoutMs(deadline);


            if (connect_errno != EINPROGRESS)
            {
                // 已明确失败
                const int save_error = errno;
                error = "connect error: " + std::string(std::strerror(save_error));
                ::close(new_fd);
                publish_failure();
                return false;
            }
                
            if (timeout_ms == 0)
            {
                error = "RPC deadline exceeded during connect";
                ::close(new_fd);
                publish_failure();
                return false;
            }
    
            pollfd poll_fd{};
            poll_fd.fd = new_fd;
            poll_fd.events = POLLHUP | POLLERR | POLLOUT;
    
            const int result = ::poll(&poll_fd, 1, timeout_ms);
            
            if (result == 0)
            {
                error = "RPC deadline exceeded during connect";
                ::close(new_fd);
                publish_failure();
                return false;
            }
            
            if (result < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }
                
                error = "poll failed";
                ::close(new_fd);
                publish_failure();
                return false;
                
            }
            
            int socket_error = 0;
            socklen_t legth = sizeof(socket_error);

            if (::getsockopt(new_fd, SOL_SOCKET, SO_ERROR,
                &socket_error, &legth) < 0)
            {
                error = "getsockopt failed";
                ::close(new_fd);
                publish_failure();
                return false;
            }

            if (socket_error != 0)
            {
                error = std::strerror(socket_error);
                ::close(new_fd);
                publish_failure();
                return false;
            }

            break;
        }
    }

    // 统一路径，不管是res什么返回值都会变回阻塞
    if (!Block_fd(new_fd, error))
    {
        ::close(new_fd);
        publish_failure();
        return false;
    }

    

    bool should_close = false;
    uint64_t receiver_generation = 0;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        if (stop_requested_.load() || state_ == ConnectionState::kClosing)
        {
            should_close = true;
        }
        else
        {
            socket_fd = new_fd;
            receive_buffer_.retrieveAll();

            ++connection_generation_;
            receiver_generation = connection_generation_;

            state_ = ConnectionState::kConnected;
        }

        
        try
        {   if(!should_close)
            {
                receive_thread_ = std::thread(&RpcClientConnection::ReceiveLoop, this, receiver_generation);
            }
        }
        catch (const std::exception& exception)
        {
            socket_fd = -1;
            state_ = ConnectionState::kDisconnected;
            error = "start receiver thread failed:" + std::string(exception.what());
            should_close = true;
        }
    }

    if (should_close)
    {
        ::shutdown(new_fd, SHUT_RDWR);
        ::close(new_fd);
        if (error.empty())
        {
            error = "connecion closed while connect was completing";
        }

        state_condition_.notify_all();
        return false;
    }

    RPC_LOG_INFO(
    "event=connection_established endpoint=%s "
    "generation=%lu",
    endpoint_.Key().c_str(),
    receiver_generation);
    state_condition_.notify_all();
    return true;

}

bool RpcClientConnection::StartCall(uint64_t request_id, const std::string& encoded,
    std::shared_ptr<PendingCall> pending, std::string& error,
    std::chrono::steady_clock::time_point deadline)
{
    error.clear();

    if (!pending || pending->response == nullptr)
    {
        error = "invalid pending call";
        return false;
    }

    if(!EnsureConnected(error, deadline))
    {
        return false;
    }

    std::unique_lock<std::timed_mutex> send_lock(send_mutex_, std::defer_lock);

    if (!send_lock.try_lock_until(deadline))
    {
        error = "RPC deadline exceeded while waiting for send lock";
        return false;
    }

    
    if (Expired(deadline))
    {
        error = "RPC deadline exceeded";
        return false;
    }

    
    bool connected = false;
    int tmp_fd = -1;
    bool sent = false;
    uint64_t call_generation = 0;
    
    
    
    {
        std::lock_guard<std::mutex>lock(state_mutex_);
        connected = state_ == ConnectionState::kConnected;
        
        if (connected)
        {
            tmp_fd = socket_fd;
            call_generation = connection_generation_;
        }
        
        
        if (Expired(deadline))
        {
            error = "RPC deadline exceeded before pending registration";
            return false;
        }
        
        
        std::lock_guard<std::mutex> pending_lock(pending_mutex_);
        if (pending_calls_.size() >= max_inflight_)
        {
            error = "RPC resource exhausted: too many in-flight calls";
            return false;
        }
        
        auto result = pending_calls_.emplace(request_id, pending);
        if (!result.second)
        {
            error = "duplicate RPC request id";
            return false;
        }    
    }
    pending_condition_.notify_one();
    
    if (Expired(deadline))
    {
        auto expired = TakePending(request_id);
        send_lock.unlock();
        if (expired) {
            expired->Finish("RPC deadline exceeded before send");
        }

        return true;  // 已完成所有权交接
    }

    if (connected)
    {
        sent = SendAll(tmp_fd, encoded, error, deadline);
    }
    else
    {
        error = "connection was lost before send";
    }

    send_lock.unlock();
    if (!sent)
    {
        if (connected)
        {
            BreakConnection(error, call_generation);
        }

        auto failed = TakePending(request_id);

        if (failed)
        {
            failed->Finish(error);
        }
        
    }//不管TakePending是否成功，此时接管权已经移交给connection, channel不再进行Finish

    return true;

}

bool RpcClientConnection::HandleResponse(const RpcFrame& frame, std::string& error)
{
    error.clear();
    if (frame.message_type != RpcMessageType::kResponse)
    {
        error = "client receive a non-response frame";
        return false;
    }

    auto pending = TakePending(frame.request_id);
    if (!pending)
    {
        // 可能是超时、断线或重复响应之后到达的迟到响应。
        // 它不应该破坏一条仍然健康的连接。
        error = "can not find reqeust_id:" + std::to_string(frame.request_id);
        return true;
    }

    mprpc::RpcResponseMeta response_meta;
    if (!response_meta.ParseFromString(frame.meta)) 
    {
        error = "invalid rpc meta";
        pending->Finish(error); //响应meta无法解析
        return false;
    }


    if (!pending->response_staging)
    {
        error = "pending response staging is null";
        pending->Finish(error);
        return false;
    }   

    if (response_meta.rpc_status_code() != mprpc::RPC_STATUS_OK)
    {
        error = response_meta.error_text();

        if (error.empty())
        {
            error = "HandleResponse failed with" + std::to_string(response_meta.rpc_status_code());
        }

        pending->Finish(error);  //服务端返回结构化rpc错误
        return true;
    }
    
    if (!pending->response_staging->ParseFromString(frame.payload))
    {
        error = "invalid rpc response payload";
        pending->Finish(error); //响应payload无法解析
        return true; 
    }

    const bool won = pending->Finish("", pending->response_staging.get());  //正常成功
    if (!won)
    {
        RPC_LOG_INFO(
        "late response ignored, request_id=%lu",  //后续可以监测晚到响应进行热点分析
        frame.request_id);
    }

    return true;
}

void RpcClientConnection::ReceiveLoop(uint64_t generation)
{
    char data[8192]{};

    while (!stop_requested_.load())
    {
        int fd = -1;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);

            if (state_ != ConnectionState::kConnected||generation!=connection_generation_)
            {
                return;
            }

            fd = socket_fd;
        }

        const ssize_t count = ::recv(fd, data, sizeof(data), 0);
    
        if (count > 0)
        {
            receive_buffer_.append(data, static_cast < size_t>(count));
        }
        else if (count == 0)
        {
            BreakConnection("serve close rpc connection", generation);
            return;
        }
        else if (errno == EINTR)
        {
            continue;
        }
        else
        {
            const int save_errno = errno;
            if (stop_requested_)
            {
                return;
            }

            BreakConnection("recv failed" + std::string(strerror(save_errno)), generation);
            return;
        }

        while (true)
        {
            RpcFrame frame;
            std::string error;
            const DecodeStatus status = RpcCodec::Decode(&receive_buffer_, &frame, error);

            if (status== DecodeStatus::kNeedMoreData)
            {
                break;
            }

            if (status == DecodeStatus::kProtocolError)
            {
                BreakConnection("decode response failed" + error, generation);
                return;
            }

            if (!HandleResponse(frame, error))
            {
                BreakConnection(error,generation);
                return;
            }
        }

    }
}

void RpcClientConnection::BreakConnection(const std::string& reason, uint64_t expected_generation)
{
    int fd = -1;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> old_pending;
    {
        std::lock_guard<std::timed_mutex> send_lock(send_mutex_);      
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        std::lock_guard<std::mutex> pending_lock(pending_mutex_);
        
        if (expected_generation != connection_generation_)
        {
            return;
        }
        if (state_ == ConnectionState::kClosing||state_==ConnectionState::kDisconnected)
        {
            return;
        }
        
        
        old_pending.swap(pending_calls_);
        
        fd = socket_fd;
        socket_fd = -1;

        // 失效所有迟来的事件
        ++connection_generation_;
        state_ = ConnectionState::kDisconnected;
    }

    if (fd >= 0)
    {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }

    state_condition_.notify_all();

    RPC_LOG_ERR("event=connection_broken endpoint=%s generation=%lu affected_calls=%zu reson=%s"
        , endpoint_.Key().c_str(), expected_generation, old_pending.size(), reason.c_str());


    //如果直接failAllpending会出现一个问题, 还未Fail成功就会其他线程注册新连接进表，导致误伤
    for (auto& [id, pending] : old_pending)
    {
        pending->Finish(reason);
    }
}

void RpcClientConnection::Close()
{
    stop_requested_.store(true);
    pending_condition_.notify_all();

    int fd = -1;
    {
        std::lock_guard<std::timed_mutex> lock(send_mutex_);
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            fd = socket_fd;
            socket_fd = -1;
            ++connection_generation_;
            state_ = ConnectionState::kClosing;
        }
    }

    if (fd >= 0)
    {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }

    state_condition_.notify_all();
    FailAllPending("RPC connection closed");

    if (receive_thread_.joinable())
    {
        if (receive_thread_.get_id() == std::this_thread::get_id())
        {
            //这里先暴露出生命周期管理的问题，后面再来修改
            std::terminate();
        }

        receive_thread_.join();
    }

    if (time_out_thread_.joinable())
    {
        if (time_out_thread_.get_id() == std::this_thread::get_id())
        {
            //这里先暴露出生命周期管理的问题，后面再来修改
            std::terminate();
        }

        time_out_thread_.join();

    }
}

bool PendingCall::Finish(const std::string& reason, const google::protobuf::Message* decoded_response)
{
    google::protobuf::Closure* callback = nullptr;

    {
        std::lock_guard<std::mutex> lock(mutex);

        if (completed)
        {
            return false;
        }

        if (decoded_response != nullptr)
        {
            if (response == nullptr)
            {
                return false;
            }

            response->CopyFrom(*decoded_response);
        }

        error = reason;
        if (!reason.empty() && controller != nullptr)
        {
            controller->SetFailed(reason);
        }

        completed = true;
        callback = done;
        done = nullptr;
    }

    condition.notify_all();

    if (callback != nullptr)
    {
        callback->Run();
    }

    return true;
}
// void PendingCall::Wait()
// {
//     std::unique_lock<std::mutex> lock(mutex);
//     condition.wait(lock, [this] { return completed; });
// }

bool PendingCall::WaitUntil(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_until(lock, deadline, [this]
        {
            return completed;
        });
}

bool RpcClientConnection::ExpirePending(uint64_t request_id) //这里暂时制作简单的根据id进行超时清理，复杂的代次逻辑，id复用后面可升级
{
    std::lock_guard <std::mutex> lock(pending_mutex_);

    const auto it = pending_calls_.find(request_id);

    if (it == pending_calls_.end())
    {
        return false;
    }

    pending_calls_.erase(it);
    return true;
}

void RpcClientConnection::TimeoutLoop()
{
    std::unique_lock<std::mutex> lock(pending_mutex_);
    while (!stop_requested_.load())
    {
        if (pending_calls_.empty())
        {
            pending_condition_.wait(lock, [this]
                {
                    return stop_requested_.load() || !pending_calls_.empty();
                });
        }

        if (stop_requested_.load())
        {
            break;
        }

        const auto now = std::chrono::steady_clock::now();
        auto earliest = std::chrono::steady_clock::time_point::max();
        std::vector<std::shared_ptr<PendingCall>> expired;
       

        for (auto it = pending_calls_.begin(); it != pending_calls_.end(); )
        {
            const auto& pending = it->second;

            if (pending->deadline <= now)
            {
                expired.push_back(pending);
                it = pending_calls_.erase(it);
                continue;
            }

            earliest = std::min(earliest, pending->deadline);
            ++it;
        }

        if (!expired.empty())
        {
            lock.unlock();

             RPC_LOG_INFO(
                "event=request_timeout_batch endpoint=%s count=%zu",
                endpoint_.Key().c_str(),
                expired.size());
             
            for (const auto& it : expired)
            {
                it->Finish("RPC deadline exceeded");
            }

            lock.lock();
            continue;
        }

        pending_condition_.wait_until(lock, earliest);
    }
}