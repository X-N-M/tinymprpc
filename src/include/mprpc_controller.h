#pragma once

#include <cstdint>
#include "mprpc_options.h"
#include <google/protobuf/service.h>
#include <string>

class MprpcController : public google::protobuf::RpcController
{
public:
    MprpcController();
    
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;

    //尚未实现的功能
    void StartCancel() override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure *callback) override; 

    void SetTimeoutMs(int64_t timeout_ms);
    int64_t TimeoutMs() const;

  private:
    bool m_failed;
    std::string m_errText;
    int64_t timeout_ms_{ mprpc::kDefaultRpcTimeoutMs };
};