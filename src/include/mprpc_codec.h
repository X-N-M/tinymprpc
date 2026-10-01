#pragma once

#include <cstddef>
#include <cstring>
#include <cstdint>
#include <array>
#include <muduo/net/Buffer.h>
#include <string>

enum class DecodeStatus
{
    kFrameReady,
    kNeedMoreData,
    kProtocolError
};

enum class RpcMessageType :uint8_t
{
    kUnknown=0,
    kRequest = 1,
    kResponse = 2
};

struct RpcFrame
{
    RpcMessageType message_type{RpcMessageType::kUnknown};
    uint16_t flags{0};
    uint64_t request_id{0};
    std::string meta;
    std::string payload;

};

class RpcCodec
{
public:
    static bool Encode(const RpcFrame& frame, std::string* output);

    static DecodeStatus Decode(muduo::net::Buffer* buffer, RpcFrame* frame, std::string& error);
private:
    static constexpr std::array<uint8_t, 4> kRpcMagic{'M', 'R', 'P', 'C'};
    static constexpr uint8_t kRpcVersion = 1;
    static constexpr size_t kFixedHeaderSize = 24;
    static constexpr uint32_t kMaxMetaSize = 64 * 1024;
    static constexpr uint32_t kMaxPayloadSize = 4 * 1024 * 1024;
};