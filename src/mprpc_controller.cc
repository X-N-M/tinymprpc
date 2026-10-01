#include "mprpc_controller.h"
#include <cstdint>
#include <string>

MprpcController::MprpcController()
{
    m_failed = false;
    m_errText = "";
}

bool MprpcController::Failed() const
{
    return m_failed;
}

std::string MprpcController::ErrorText() const
{
    return m_errText;
}

void MprpcController::Reset()
{
    m_failed = false;
    m_errText = "";
    timeout_ms_ = mprpc::kDefaultRpcTimeoutMs;
}

void MprpcController::SetFailed(const std::string& reason)
{
    m_failed = true;
    m_errText = reason;
}

void MprpcController::SetTimeoutMs(int64_t timeout_ms)
{
    if (timeout_ms <= 0)
    {
        timeout_ms_ = mprpc::kDefaultRpcTimeoutMs;
        return;
    }

    timeout_ms_ = timeout_ms;
}

int64_t MprpcController::TimeoutMs() const
{
    return timeout_ms_;
}

void MprpcController::NotifyOnCancel(google::protobuf::Closure *callback){}
bool MprpcController::IsCanceled()const { return false; }
void MprpcController::StartCancel(){}