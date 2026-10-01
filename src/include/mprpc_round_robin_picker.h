#pragma once

#include "mprpc_client_connection.h"
#include <unordered_map>
#include <vector>
class RoundRobinPicker
{
public:
    bool Pick(const std::string& route, const std::vector<RpcEndpoint>& endpoints,
        RpcEndpoint& selected, std::string& error);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, size_t> next_index_;
};