#pragma once

#include "lockqueue.h"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <string>
#include <thread>



enum LogLevel
{
    INFO, //普通信息
    ERROR, //错误信息
};

class Logger
{
public:
    static Logger& GetInstance();
    void Log(LogLevel level, std::string msg); //写入日志
private:
    struct LogRecord
    {
        LogLevel level;
        std::chrono::system_clock::time_point timestamp;
        std::string message;
    };

    static constexpr size_t kQueueCapacity = 8192;

    LockQueue<LogRecord> m_lockQueue{kQueueCapacity}; //日志缓冲队列
    std::thread m_writeThread;
    
    void WriteLoop();
    Logger();
    Logger(const Logger&) = delete;
    Logger(Logger&&) = delete;
    ~Logger();
};

#define RPC_LOG_INFO(logmsgformat, ...)                       \
do                                                        \
{                                                         \
    Logger& logger=Logger::GetInstance();                 \
    char msg[1024];                                       \
    std::snprintf(msg, 1024, logmsgformat, ##__VA_ARGS__);\
    logger.Log(LogLevel::INFO, msg);                      \                                          
} while (0);                                              



#define RPC_LOG_ERR(logmsgformat, ...)                        \
do                                                        \
{                                                         \
    Logger& logger=Logger::GetInstance();                 \
    char msg[1024];                                       \
    std::snprintf(msg, 1024, logmsgformat, ##__VA_ARGS__);\
    logger.Log(LogLevel::ERROR, msg);                     \       
} while (0);                                              