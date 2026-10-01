#include "zookeeperutil.h"
#include "mprpc_app.h"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <semaphore.h>
#include <string>
#include <vector>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
#include "logger.h"

//watcher观察器
void ZKClient::global_watcher(zhandle_t* zh, int type,
        int state, const char *path,void *watcherCtx)
{
    if (type == ZOO_SESSION_EVENT && watcherCtx != nullptr)
    {

        auto* context = static_cast<ZkSessionContext*>(watcherCtx);

        std::string error{};
        ZkState mapped_state;

        if (state == ZOO_CONNECTED_STATE)
        {
            mapped_state = ZkState::kConnected;
        }
        else if (state == ZOO_ASSOCIATING_STATE || state == ZOO_CONNECTING_STATE)
        {
            mapped_state = ZkState::kConnecting;
        }
        else if (state == ZOO_EXPIRED_SESSION_STATE)
        {
            mapped_state = ZkState::kExpired;
            error = "ZooKeeper session expired";
        }
        else
        {
            mapped_state = ZkState::kDisconnected;
            error = "ZooKeeper session disconnected";
        }

        {
            std::lock_guard<std::mutex> lock(context->zk_mutex);

            if (context->state == ZkState::kClosed)
            {
                return;
            }

            context->state = mapped_state;
            context->last_error = error;
        }

        context->condition.notify_all();
    }
}

namespace //这里是Startinternal后面用的rall守卫
{
    struct ZKHandleDeleter
    {
        void operator()(zhandle_t* handle) const noexcept
        {
            if (handle != nullptr)
            {
                zookeeper_close(handle);
            }
        }
    };

    using PendingZkHandle = std::unique_ptr<zhandle_t, ZKHandleDeleter>;
}

ZKClient::ZKClient():m_zhandle(nullptr)
{

}

ZKClient::~ZKClient()
{
    Close();
}

bool ZKClient::Start(std::string &error, std::chrono::steady_clock::time_point deadline)
{
    bool should_initialize = false;

    {
        std::lock_guard<std::mutex> lock(context_.zk_mutex);

        switch (context_.state)
        {
            case ZkState::kConnected:
            {
                if (context_.handle_ready && m_zhandle != nullptr)
                {
                    error.clear();
                    return true;
                }

                break;
            }

            case ZkState::kNostarted:
            {
                context_.state = ZkState::kConnecting;
                should_initialize = true;
                context_.last_error.clear();
                break;
            }

            case ZkState::kConnecting:
            {
                break;
            }

            case ZkState::kDisconnected:
            {
                error = context_.last_error;
                return false;
            }

            case ZkState::kExpired:
            {
                error = "ZooKeeper session expired";
                return false;
            }
            case ZkState::kClosed:
            {
                error = "ZooKeeper client is closed";
                return false;
            }
        }

    }

    if (should_initialize)
    {
        return StartInternal(error, deadline);
    }

    return WaitForStartResult(error, deadline);
}

// void ZKClient::Create(const char* path, const char* data, int datalen, int state)
// {
//     char path_buf[128];
//     int buflen = sizeof(path_buf);
//     int flag;
//     flag = zoo_exists(m_zhandle, path, 0, nullptr);
//     if (flag == ZNONODE)
//     {
//         flag = zoo_create(m_zhandle, path,
//             data,
//             datalen,
//             &ZOO_OPEN_ACL_UNSAFE,
//             state,
//             path_buf,
//             buflen);

//         if (flag != ZOK)
//         {
//             RPC_LOG_ERR("creat znode error, path:%s", path);
//             RPC_LOG_ERR("flag:%d", flag);
//             return;
//         }
//         else
//         {
//             RPC_LOG_INFO("creat znode success...path:%s", path);
//         }
//     }

// }



void ZKClient::SetState(ZkState state, const std::string& error)
{
    {
        std::lock_guard<std::mutex> lock(context_.zk_mutex);

        if (context_.state == ZkState::kClosed && state != ZkState::kClosed)
        {
            return;
        }

        context_.state = state;
        context_.last_error = error;
    }

    context_.condition.notify_all();
}

bool ZKClient::WaitForStartResult(std::string& error, std::chrono::steady_clock::time_point deadline)
{
    {
        std::unique_lock<std::mutex> lock(context_.zk_mutex);

        context_.condition.wait_until(lock, deadline, [this]
            {
                bool connected = context_.state == ZkState::kConnected && context_.handle_ready && m_zhandle != nullptr;
                bool failure = context_.state == ZkState::kDisconnected ||
                    context_.state == ZkState::kExpired ||
                    context_.state == ZkState::kClosed;
                
                return connected||failure;
            });

        if (context_.state == ZkState::kConnected&&context_.handle_ready&&m_zhandle!=nullptr)
        {
            return true;
        }

        error = context_.last_error;
    }

    return false;
}

bool ZKClient::StartInternal(std::string& error, std::chrono::steady_clock::time_point deadline)
{
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);  //先隐藏一下终端不必要的日志
    const std::string ip = MpRpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    const std::string port = MpRpcApplication::GetInstance().GetConfig().Load("zookeeperport");

    const std::string endpoint = ip + ":" + port;
    zhandle_t* handle = zookeeper_init(endpoint.c_str(), global_watcher, 30000, nullptr, &context_, 0);

    PendingZkHandle pending_handle(handle);

    if (pending_handle == nullptr)
    {
        SetState(ZkState::kDisconnected, "zookeeper init fail");
        error = "zookeeper init fail";
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(context_.zk_mutex);
        if (context_.state == ZkState::kClosed)
        {
            error = "zookeeper had been closed";
            return false;
        }
        
        m_zhandle = pending_handle.release();
        context_.handle_ready = true;
    }

    context_.condition.notify_all();

    {
        std::unique_lock<std::mutex> lock(context_.zk_mutex);
        bool finished = context_.condition.wait_until(lock, deadline,
            [this]
            {
                const bool connected =
                context_.state == ZkState::kConnected &&
                context_.handle_ready &&
                m_zhandle != nullptr;

                const bool failed =
                context_.state == ZkState::kDisconnected ||
                context_.state == ZkState::kExpired ||
                context_.state == ZkState::kClosed;

            return connected || failed;
            });

        if (!finished)
        {
            
        }
        else if (context_.state == ZkState::kConnected && context_.handle_ready && m_zhandle != nullptr)   
        {
            error.clear();
            return true;
        }
        else
        {
            error = context_.last_error;
            return false;
        }
    }

    error = "zookeeper connection timed out";
    // Close(); 这里不应该调用Close，虽然资源释放机制是类似的，但是状态语义不一样
    ReleaseHandle(ZkState::kDisconnected, error);
    
    return false;
}

void ZKClient::Close()
{

    std::lock_guard<std::mutex> lock(operation_mutex_);

    zhandle_t* handle_to_close = nullptr;

    {
        std::lock_guard<std::mutex> lock(context_.zk_mutex);
        context_.handle_ready = false;
        context_.state = ZkState::kClosed;
        context_.last_error = "ZkClient is closed";

        handle_to_close = m_zhandle;
        m_zhandle = nullptr;
    }

    context_.condition.notify_all();

    if (handle_to_close)
    {
        zookeeper_close(handle_to_close);
    }
}

void ZKClient::ReleaseHandle(ZkState final_state, const std::string& reason)
{

    std::lock_guard<std::mutex> lock(operation_mutex_);
    
    zhandle_t* handle_to_close = nullptr;

    {
        std::lock_guard<std::mutex> lock(context_.zk_mutex);

        if (context_.state == ZkState::kClosed && final_state != ZkState::kClosed)
        {
            return;
        }

        context_.handle_ready = false;
        context_.state = final_state;
        context_.last_error = reason;

        handle_to_close = m_zhandle;
        m_zhandle = nullptr;
    }

    context_.condition.notify_all();

    if (handle_to_close)
    {
        zookeeper_close(handle_to_close);
    }
}

ZkState ZKClient::GetState() const
{
    std::lock_guard<std::mutex> lock(context_.zk_mutex);
    return context_.state;
}

bool ZKClient::CreatePersistent(const std::string& path, const std::string& data, std::string& error)
{
    error.clear();

    std::string created_path;

    const int result = CreateNode(path, data, 0, created_path, error);

    if(result==ZOK||result==ZNODEEXISTS)
    {
        error.clear();
        return true;
    }

    return false;
}

bool ZKClient::CreateEphemeralSequential(const std::string &path_prefix, const std::string &data, std::string &created_path, std::string &error)
{
    error.clear();
    created_path.clear();

    const int res = CreateNode(path_prefix, data, ZOO_EPHEMERAL | ZOO_SEQUENCE, created_path, error);

    if (res != ZOK)
    {
        return false;
    }

    if (created_path.empty())
    {
        error = "zookeeper return an empty created path";
        return false;
    }

    return true;
}


int ZKClient::CreateNode(const std::string& path, const std::string& data, int flags, std::string& created_path, std::string& error)
{
    error.clear();
    created_path.clear();

    if (!ValidatePath(path, error))
    {
        return ZBADARGUMENTS;
    }

    if (data.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
    {
        error = "zookeeper node data is too large";
        return ZBADARGUMENTS;
    }

    std::unique_lock<std::mutex> operation_lock;

    zhandle_t* handle = nullptr;


    if (!AcquireHandle(operation_lock, &handle, error))
    {
        return ZINVALIDSTATE;
    }


    std::vector<char> path_buffer(path.size() + 32, '\0');
    const int res = zoo_create(handle, path.c_str(), data.empty() ? nullptr : data.data()
        , static_cast<int>(data.size()), &ZOO_OPEN_ACL_UNSAFE, flags, path_buffer.data(),
        static_cast<int>(path_buffer.size()));

    if (res != ZOK)
    {
        error=FormatZkError("create zookeeper node failed ", path, res);
        return res;
    }

    created_path.assign(path_buffer.data());
    return ZOK;
}

bool ZKClient::GetChildren(const std::string& path, std::vector<std::string>* children, std::string& error)
{

    error.clear();
    if (children == nullptr)
    {
        error = "children out put is null";
        return false;
    }

    children->clear();
   

    
    if (!ValidatePath(path, error))
    {
        return false;
    }

    std::unique_lock<std::mutex> operation_lock;
    zhandle_t* handle = nullptr;
    if (!AcquireHandle(operation_lock, &handle, error))
    {
        return false;;
    }

    String_vector child_list{};
    const int res = zoo_get_children(handle, path.c_str(), 0, &child_list);

    if (res != ZOK)
    {
        error=FormatZkError("get zookeeper children failed", path, res);
        return false;
    }

    std::vector<std::string> result_children;
    for (int i = 0; i < child_list.count; ++i)
    {
        result_children.emplace_back(child_list.data[i]);
    }

    deallocate_String_vector(&child_list);

    children->swap(result_children);
    return true;
}

bool ZKClient::GetData(const std::string &path, std::string &data, std::string &error)
{
    data.clear();
    error.clear();

    if (!ValidatePath(path, error))
    {
        return false;
    }

    std::unique_lock<std::mutex> operation_lock;
    zhandle_t* handle = nullptr;

    if (!AcquireHandle(operation_lock, &handle, error))
    {
        return false;
    }

    constexpr size_t kInitialBufferSize = 4096;
    std::vector<char> buffer(kInitialBufferSize);

    
    int buffer_size = static_cast<int>(buffer.size());

    const int res = zoo_get(handle, path.c_str(), 0, buffer.data(), &buffer_size, nullptr);

    if (res != ZOK)
    {
        error=FormatZkError("zoo_get failed", path, res);
        return false;
    }

    if (buffer_size < 0 ||
        buffer_size > static_cast<int>(buffer.size()))
    {
        error = "invalid ZooKeeper data length";
        return false;
    }

        
    data.assign(buffer.data(), static_cast<size_t>(buffer_size));
    return true;
    
}


std::string ZKClient::FormatZkError(const std::string& operation, const std::string& path, int result)
{
    return operation +
        " failed: path=" +
        path +
        ", code=" +
        std::to_string(result) +
        ", message=" +
        zerror(result);
}

bool ZKClient::ValidatePath(const std::string& path, std::string& error)
{
    if (path.empty())
    {
        error = "ZooKeeper path is empty";
        return false;
    }

    if (path.front() != '/')
    {
        error =
            "ZooKeeper path must be absolute: " +
            path;
        return false;
    }

    return true;
}

bool ZKClient::AcquireHandle(std::unique_lock<std::mutex>& operation_lock,zhandle_t** handle,std::string& error)
{
    error.clear();

    if (handle == nullptr)
    {
        error = "ZooKeeper handle output is null";
        return false;
    }

    *handle = nullptr;

    // 锁交给调用方持有到本次 ZooKeeper 操作结束。
    operation_lock = std::unique_lock<std::mutex>(
        operation_mutex_);

    {
        std::lock_guard<std::mutex> state_lock(
            context_.zk_mutex);

        if (context_.state == ZkState::kClosed)
        {
            error = "ZooKeeper client is closed";
            return false;
        }

        if (context_.state == ZkState::kExpired)
        {
            error = "ZooKeeper session expired";
            return false;
        }

        if (context_.state != ZkState::kConnected ||
            !context_.handle_ready ||
            m_zhandle == nullptr)
        {
            error = "ZooKeeper client is not connected";
            return false;
        }

        *handle = m_zhandle;
    }

    return true;
}