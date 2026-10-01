#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <zookeeper/zookeeper.h>
#include <string>

enum class ZkState
{
    kNostarted,
    kConnecting,
    kConnected,
    kDisconnected,
    kExpired,
    kClosed
};


struct ZkSessionContext
{
    mutable std::mutex zk_mutex;
    std::condition_variable condition;
    ZkState state{ ZkState::kNostarted };
    std::string last_error;
    bool handle_ready{ false };
};

class ZKClient
{
public:

    ZKClient();
    ~ZKClient();

    // ZKClient拥有独占资源
    ZKClient(const ZKClient&) = delete;
    ZKClient& operator=(const ZKClient&) = delete;

    bool Start(std::string& error, std::chrono::steady_clock::time_point deadline);
    ZkState GetState() const;
    bool CreatePersistent(const std::string &path, const std::string &data, std::string &error);
    bool CreateEphemeralSequential(const std::string& path_prefix, const std::string& data, std::string& created_path, std::string& error);
    bool GetChildren(const std::string& path, std::vector<std::string>* children, std::string& error);
    bool GetData(const std::string& path, std::string& data, std::string& error);
    
private:
    void Close();
    void ReleaseHandle(ZkState final_state, const std::string& reason);
    bool StartInternal(std::string& error, std::chrono::steady_clock::time_point deadline);
    bool WaitForStartResult(std::string& error, std::chrono::steady_clock::time_point deadline);
    int CreateNode(const std::string& path, const std::string& data, int flags, std::string& created_path, std::string& error);
    static void global_watcher(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx);
    void SetState(ZkState state, const std::string& error);

    bool AcquireHandle(std::unique_lock<std::mutex>& operation_lock, zhandle_t** handle, std::string& error);
    std::string FormatZkError(const std::string& operation, const std::string& path, int result);
    bool ValidatePath(const std::string& path, std::string& error);
private:
    zhandle_t* m_zhandle;
    ZkSessionContext context_;
    mutable std::mutex operation_mutex_; //保证zoo_create zoo_get zoo_get_children与Close不并发释放handle
    
};