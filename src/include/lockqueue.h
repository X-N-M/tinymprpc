#pragma once

#include <cstddef>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>

template<class T>
class LockQueue
{
public:

    explicit LockQueue(size_t capacity) : m_capacity(capacity)
    {
        
    }
    
    bool push(const T& msg)
    {
        {

            std::lock_guard<std::mutex> lk(m_mutex);
            if (m_closed)
            {
                return false;
            }

            if (m_queue.size() >= m_capacity)
            {
                ++m_dropped_count;
                return false;
            }
            
            m_queue.push(std::move(msg));
            
        }

        m_convar.notify_one();
        return true;
    }

    bool pop(T& value)
    {
        std::unique_lock<std::mutex> lk(m_mutex);

        m_convar.wait(lk, [this]
            {
                return m_closed || !m_queue.empty();
            });
        
        if (m_queue.empty())
        {
            return false;
        }

        value = std::move(m_queue.front());
        m_queue.pop();
        return true;

    }

    void close()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
        }

        m_convar.notify_all();
    }


private:
    std::mutex m_mutex;
    std::queue<T> m_queue;
    std::condition_variable m_convar;

    const size_t m_capacity;
    bool m_closed{ false };
    uint64_t m_dropped_count{ 0 };
};