#include "mprpc_app.h"
#include "mprpc_channel.h"
#include "mprpc_controller.h"
#include "user.pb.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <limits>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <vector>

using MicrosecondsRep = std::chrono::microseconds::rep;
struct Worker
{
    std::vector<MicrosecondsRep> latencies;
    MicrosecondsRep total_us = 0;
    int success_count = 0;
    int failed_count = 0;
};

class StartGate //做一个简单的闸口，等待线程创建完毕
{
public:
    StartGate(int expected) : expected_(expected) {}

    void ArriveAndWait() //worker等待
    {
        std::unique_lock<std::mutex> lock(mutex_);

        ++ready_;

        if (ready_ == expected_)
        {
            condition_.notify_all();
        }

        condition_.wait(lock, [this] { return open_; });
    }

    void WaitUntilReady()
    {
        std::unique_lock<std::mutex>lock(mutex_);

        condition_.wait(lock, [this] { return ready_ == expected_; });
    }

    void Open()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            open_ = true;
        }

        condition_.notify_all();
    }


private:
    int expected_;
    int ready_ = 0;
    bool open_ = false;

    std::condition_variable condition_;
    std::mutex mutex_;

};

void prepare_call(
    MprpcController& controller,
    RPC::LoginResponds& response)
{
    controller.Reset();
    controller.SetTimeoutMs(5000);
    response.Clear();
}

std::string BuildPayload(std::size_t size)
{
    std::string payload(size, '\0');

    for (std::size_t i = 0; i < size; ++i)
    {
        payload[i] = static_cast<char>('a' + i % 26);
    }

    return payload;
}

void FillLoginRequest(RPC::LoginRequest& request, const std::string& payload)
{
     const std::size_t split = payload.size() / 2;

    request.set_name(payload.substr(0, split));

    request.set_pwd(payload.substr(split));
}

bool IsSuccess(
    MprpcController& controller,
    RPC::LoginResponds& response,
    const std::string& expected_payload)
{
    if (controller.Failed())
    {
        return false;
    }

    if (response.res().errorcode() != 0)
    {
        return false;
    }

    return response.res().errormsg() == expected_payload;
}


void RunWorker(MpRpcChannel& channel, int request_count,
    Worker& worker, StartGate& gate, const std::string& payload)
{
    RPC::UserServiceRpc_Stub stub(&channel);
    RPC::LoginRequest request;
    FillLoginRequest(request, payload);


    RPC::LoginResponds response;
    MprpcController controller;

    gate.ArriveAndWait();

    for (int i = 0; i < request_count; ++i)  //正式进行稳态测试
    {
 
        controller.Reset();
        controller.SetTimeoutMs(5000);
        response.Clear();

        const auto begin = std::chrono::steady_clock::now();
        stub.Login(&controller, &request, &response, nullptr);
    
        const auto end = std::chrono::steady_clock::now();
        const auto time_pass = std::chrono::duration_cast
            <std::chrono::microseconds>(end - begin).count();

        if (!IsSuccess(controller, response, payload))
        {
            ++worker.failed_count;
            continue;
        }


        ++worker.success_count;
        worker.latencies.push_back(time_pass);
        worker.total_us += time_pass;

    }

}


int main(int argc, char** argv)
{
    MpRpcApplication::Init(argc, argv);
    const int request_count = MpRpcApplication::BenchmarkRequests();
    const int warm_up = MpRpcApplication::WarmUpCounts();
    const int concurrency = MpRpcApplication::BenchmarkConcurrency();
    const int pool_size = MpRpcApplication::BenchmarkPoolSize();
    const int payload_size = MpRpcApplication::BenchmarkPayloadSize();

    const std::string payload = BuildPayload(static_cast<int>(payload_size));

    MpRpcChannel channel(static_cast<std::size_t>(pool_size));
    RPC::UserServiceRpc_Stub stub(&channel);
    RPC::LoginRequest request;
    FillLoginRequest(request, payload);

    RPC::LoginResponds response;
    MprpcController controller;
    MicrosecondsRep min_us = std::numeric_limits<MicrosecondsRep>::max();
    MicrosecondsRep total_us = 0;
    MicrosecondsRep max_us = 0;
    controller.SetTimeoutMs(5000);

    MicrosecondsRep cold_start;
    std::vector<MicrosecondsRep> latencies;
    std::string cold_start_error;
    bool cold_start_success = false;

    int warmup_failed = 0;
    int failed_count = 0;
    int success_count = 0;

    auto percentile_us = [](const std::vector<MicrosecondsRep>& values,
        double percentile)->MicrosecondsRep  //计算延迟分布的lambda
        {
            if (values.empty())
            {
                return 0;
            }

            std::vector<MicrosecondsRep> sorted(values);
            std::sort(sorted.begin(), sorted.end());

            //最小排名法
            size_t rank = static_cast<size_t>(std::ceil(sorted.size() * percentile));
            rank = std::max<size_t>(rank, 1);
            rank = std::min<size_t>(rank, sorted.size());
            return sorted[rank - 1];
        };

    prepare_call(controller, response);
    const auto begin = std::chrono::steady_clock::now();
    stub.Login(&controller, &request, &response, nullptr);
    const auto end = std::chrono::steady_clock::now();

    cold_start = std::chrono::duration_cast<std::chrono::microseconds>
        (end - begin).count();  //冷启动时长

    if (IsSuccess(controller, response, payload))
    {
        cold_start_success = true;
    }
    else if (controller.Failed())
    {
        cold_start_error = controller.ErrorText();
    }
    else
    {
        cold_start_error = response.res().errormsg();
    }

    
    for (int i = 0; i < warm_up; ++i)   //测试前预热
    {
        prepare_call(controller, response);
        stub.Login(&controller, &request, &response, nullptr);
        if (!IsSuccess(controller, response, payload))
        {
            ++warmup_failed;
        }
    }
        
  
    const int worker_count = std::min(concurrency, request_count);
    std::vector<Worker> workers(worker_count);
    std::vector<std::thread> threads;
    threads.reserve(worker_count);
    StartGate gate(worker_count);

    const int base_count = request_count / worker_count;
    const int remainder = request_count % worker_count;

    for (int i = 0; i < worker_count; ++i)
    {
        const int current_count =
        base_count + (i < remainder ? 1 : 0);

        threads.emplace_back(
            RunWorker,
            std::ref(channel),
            current_count,
            std::ref(workers[i]),
            std::ref(gate),
            std::cref(payload));
    }

    gate.WaitUntilReady();

    const auto total_begin = std::chrono::steady_clock::now();

    gate.Open();

    for (auto& thread : threads)
    {
        thread.join();
    }

    const auto total_end = std::chrono::steady_clock::now();
    // 汇总并发请求
    for (const auto& w : workers)
    {
        success_count += w.success_count;
        failed_count += w.failed_count;
        total_us += w.total_us;

        latencies.insert(
            latencies.end(),
            w.latencies.begin(),
            w.latencies.end());

        for (const auto latency : w.latencies)
        {
            min_us = std::min(min_us, latency);
            max_us = std::max(max_us, latency);
        }
    }

    const double elapsed_seconds =std::chrono::duration<double>(total_end - total_begin).count();
    const double total_qps = static_cast<double>(request_count) / (elapsed_seconds);
    const double success_qps = static_cast<double>(success_count) / elapsed_seconds;


    if (cold_start_success)
    {
        std::cout << "cold_start: "
              << cold_start
              << " us\n";
    }
    else
    {
        std::cout << "cold_start: failed after "
            << cold_start
            << " us, reason="
            << cold_start_error
            << '\n';
    }
    
    std::cout << "success: " << success_count << "/"<<request_count<<'\n';
    if (success_count == 0)
    {
        std::cout << "no success" << '\n';
    }
    else
    {
        std::cout << "avg: " << total_us / success_count << " us\n";
        std::cout << "min: " << min_us << " us\n";
        std::cout << "max: " << max_us << " us\n";
        std::cout << "p50: "
            << percentile_us(latencies, 0.50)
            << " us\n";

        std::cout << "p95: "
            << percentile_us(latencies, 0.95)
            << " us\n";

        std::cout << "p99: "
            << percentile_us(latencies, 0.99)
            << " us\n";

        std::cout << "payload_bytes: "
          << payload.size()
          << '\n';

        std::cout << "pool_size: "
          << pool_size
          << '\n';
    }

    std::cout << "TOTAL_QPS: " << total_qps << '\n';
    std::cout << "SUCCESS_QPS: " << success_qps << '\n';
        
    if (warm_up > 0)
    {
        std::cout << "warm_up failed: " << warmup_failed << "\n";
    }
}