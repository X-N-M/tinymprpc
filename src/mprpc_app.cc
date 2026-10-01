#include "mprpc_app.h"
#include "mprpc_config.h"
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

MpRpcConfig MpRpcApplication::m_config;
int MpRpcApplication::benchmark_concurrency_ = 1;
int MpRpcApplication::benchmark_requests_ = 1000;
int MpRpcApplication::warmup = 0;
int MpRpcApplication::benchmark_pool_size_ = 1;
int MpRpcApplication::benchmark_payload_size_ = 1024;

void ShowHelp()
{
    std::cout << "format command -i <configfile>" << std::endl;
}

void MpRpcApplication::Init(int argc, char** argv)
{
    if (argc < 2)
    {
        ShowHelp();
        exit(EXIT_FAILURE);
    }

    int c = 0;
    std::string config_file;
    while ((c = getopt(argc, argv, "i:n:w:c:p:s:")) != -1)
    {
        switch (c)
        {
        case 'i':
        {
            config_file = optarg;
            break;
        }
        case 'n':
        {
            benchmark_requests_ = std::stoi(optarg);
            if (benchmark_requests_ <= 0)
            {
                std::cerr << "-n must be positive\n";
                exit(EXIT_FAILURE);
            }

            break;
        }
        case 'w':
        {
            warmup = std::stoi(optarg);

            if (warmup < 0)
            {
                std::cerr << "-w must be non-negative\n";
                exit(EXIT_FAILURE);
            }

            break;
        }
        case 'c':
        {
            benchmark_concurrency_ = std::stoi(optarg);
            if (benchmark_concurrency_ <= 0)
            {
                std::cerr << "-c must be positive\n";
                exit(EXIT_FAILURE);
            }

            break;
        }
        case 'p':
        {
            benchmark_pool_size_ = std::stoi(optarg);
            if (benchmark_pool_size_ <= 0)
            {
                std::cerr << "-p must be positive\n";
                exit(EXIT_FAILURE);
            }

            break;
        }
        case 's':
        {
            benchmark_payload_size_ = std::stoi(optarg);
            if (benchmark_payload_size_ <= 0)
            {
                std::cerr << "-s must be positive\n";
                exit(EXIT_FAILURE);
            }

            break;
        }
        case '?':
        {
            ShowHelp();
            exit(EXIT_FAILURE);
        }
        default:
        {
            ShowHelp();
            exit(EXIT_FAILURE);
        }
        
        }
    }

    // 加载配置
    MpRpcApplication::GetInstance().GetConfig().LoadFromConfig(config_file.c_str());

}

int MpRpcApplication::BenchmarkRequests()
{
    return benchmark_requests_;
}

MpRpcApplication& MpRpcApplication::GetInstance()
{
    static MpRpcApplication app;
    return app;
}

MpRpcConfig &MpRpcApplication::GetConfig()
{
    return m_config;
}

int MpRpcApplication::BenchmarkConcurrency()
{
    return benchmark_concurrency_;
}

int MpRpcApplication::WarmUpCounts()
{
    return warmup;
}

int MpRpcApplication::BenchmarkPoolSize()
{
    return benchmark_pool_size_;
}

int MpRpcApplication::BenchmarkPayloadSize()
{
    return benchmark_payload_size_;
}