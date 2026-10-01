#include "logger.h"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <utility>

Logger& Logger::GetInstance()
{
    static Logger logger;
    return logger;
}

void Logger::Log(LogLevel level, std::string msg) //写入日志
{
    LogRecord record;
    record.level = level;
    record.timestamp = std::chrono::system_clock::now();
    record.message = std::move(msg);
    m_lockQueue.push(record);
}

Logger::Logger() :m_writeThread(&Logger::WriteLoop, this)
{
}
void Logger::WriteLoop()
{
    LogRecord record;
    FILE* file = nullptr;
    std::string current_date;

    auto close_file = [&]()
        {
            if (file != nullptr)
            {
                std::fflush(file);
                std::fclose(file);
                file = nullptr;
            }

            current_date.clear();
        };

    while (m_lockQueue.pop(record))
    {
        const std::time_t timestamp = std::chrono::system_clock::to_time_t(record.timestamp);
        tm local_time{};
        if (localtime_r(&timestamp, &local_time) == nullptr)
        {
            fprintf(stderr, "logger: failed to convert timestamp\n");
            continue;
        }

        char date_buffer[32];
        if (strftime(date_buffer, sizeof(date_buffer),
            "%Y-%m-%d", &local_time) == 0)
        {
            fprintf(stderr, "logger: failed to format date\n");
            continue;
        }

        const std::string record_date(date_buffer);

        if (file == nullptr || record_date != current_date)
        {
            close_file();
            char filename[64];
            const int written = std::snprintf(filename,sizeof(filename), "%s_log.txt", record_date.c_str());

            if (written < 0 || static_cast<size_t>(written) >= sizeof(filename))
            {
                std::fprintf(stderr, "logger: log filename is too long\n");
                continue;
            }

            file = fopen(filename, "a");
            if (file == nullptr)
            {
                std::fprintf(stderr, "logger: failed to open %s\n", filename);
                continue;
            }

            current_date = record_date;
        }

        char time_buffer[64]{};
        if (strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S", &local_time) == 0)
        {
            std::fprintf(stderr, "logger: failed to format timestamp\n");
            continue;
        }

        const char* level_text = record.level == LogLevel::INFO ? "info" : "error";
        std::fprintf(file, "%s [%s] %s\n", time_buffer, level_text, record.message.c_str());

        if (record.level == LogLevel::ERROR)
        {
            std::fflush(file);
        }
    }

    close_file();
}

Logger::~Logger()
{
    m_lockQueue.close();

    if (m_writeThread.joinable())
    {
        m_writeThread.join();
    }
}