#include "mprpc_config.h"
#include <cstdlib>
#include <iostream>

void Trim(std::string& tmp);

void MpRpcConfig::LoadFromConfig(const char* config_file)
{
    FILE* fp = fopen(config_file, "r");
    if (fp == nullptr)
    {
        std::cout << "config_file not found" << std::endl;
        exit(EXIT_FAILURE);
    }

    while (!feof(fp))
    {
        char buf[512];
        fgets(buf, 512, fp);
        std::string tmp(buf);

        Trim(tmp);

        if (tmp.empty() || tmp[0] == '#')
        {
            continue;
        }

        int idx = tmp.find("=");
        if (idx == -1)
        {
            continue;
        }

        std::string key;
        std::string value;

        key = tmp.substr(0, idx);
        Trim(key);

        int endidx = tmp.find('\n');

        value = tmp.substr(idx + 1, endidx - idx - 1);
        Trim(value);

        config_map.insert({ key, value });
    }

    fclose(fp);
}

std::string MpRpcConfig::Load(std::string key)
{
    auto it = config_map.find(key);
    if (it == config_map.end())
    {
        return "";
    }

    return it->second;
}

void Trim(std::string &tmp)
{
    int idx = tmp.find_first_not_of(" ");
    // 前面有空格
    if (idx != -1)
    {
        tmp = tmp.substr(idx, tmp.size() - idx);
    }

    //后面有空格
    idx = tmp.find_last_not_of(" ");
    if (idx != -1)
    {
        tmp = tmp.substr(0, idx + 1);
    }
}
