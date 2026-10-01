#pragma once
#include <unordered_map>
#include <string>


class MpRpcConfig
{
public:
    void LoadFromConfig(const char* config_file);

    std::string Load(std::string key);
    
private:
    std::unordered_map<std::string, std::string> config_map;
};