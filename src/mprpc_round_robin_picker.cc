#include "mprpc_round_robin_picker.h"
#include <cstddef>
#include <mutex>


bool RoundRobinPicker::Pick(const std::string& route, const std::vector<RpcEndpoint>& endpoints,
    RpcEndpoint& selected, std::string& error)
{
    error.clear();

    if(route.empty())
    {
        error = "RPC route is empty";
        return false;
    }

    if (endpoints.empty())
    {
        error = "no available RPC endpoint";
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    size_t& next = next_index_[route];
    const size_t index = next % endpoints.size();

    selected = endpoints[index];
    next = (index + 1) % endpoints.size();

    return true;
}