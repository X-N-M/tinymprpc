#pragma once
#include "mprpc_config.h"

class MpRpcApplication
{
public:
    
    static void Init(int argc, char** argv); //初始化
    static MpRpcApplication& GetInstance(); //获取单例
    static MpRpcConfig& GetConfig();
    static int BenchmarkRequests(); //暂时先把benchmark的命令行参数解析用框架公共复用一下
    static int WarmUpCounts();
    static int BenchmarkConcurrency();
    static int BenchmarkPoolSize();
    static int BenchmarkPayloadSize();
private:

    static MpRpcConfig m_config;
    static int benchmark_requests_;
    static int warmup;
    static int benchmark_concurrency_;
    static int benchmark_pool_size_;
    static int benchmark_payload_size_;
    MpRpcApplication() {}
    MpRpcApplication(const MpRpcApplication&) = delete;
    MpRpcApplication(MpRpcApplication&&) = delete;
};

