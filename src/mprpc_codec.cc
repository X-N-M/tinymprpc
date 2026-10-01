#include "mprpc_codec.h"
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>

template<class Uint>
void AppendBigEndian(std::string& out, Uint value)
{
    for (int i = sizeof(value) * 8 - 8; i >= 0; i -= 8)
    {
        out.push_back(static_cast<char>((value >> i) & 0xFF));
    }
}

template<class Uint>
Uint ReadBigEndian(std::string_view data, size_t offset)
{
    Uint value = 0;
    for (size_t i = 0; i < sizeof(Uint); ++i)
    {
        value = static_cast<Uint>((value << 8) | static_cast<unsigned char>(data[offset + i]));
    }
    return value;
}

bool RpcCodec::Encode(const RpcFrame& frame, std::string* output)
{
    if (output == nullptr)
    {
        return false;
    }

    if (frame.message_type != RpcMessageType::kRequest && frame.message_type != RpcMessageType::kResponse)
    {
        return false;
    }
    if (frame.flags != 0)
    {
        return false;
    }
    // 限制单帧大小避免异常请求
    if (frame.meta.size() > kMaxMetaSize || frame.payload.size() > kMaxPayloadSize)
    {
        return false;
    }

    output->clear();
    output->reserve(kFixedHeaderSize + frame.meta.size() + frame.payload.size());

    for (uint8_t byte : kRpcMagic)
    {
        output->push_back(static_cast<char>(byte));
    }

    output->push_back(static_cast<char>(kRpcVersion));
    output->push_back(static_cast<char>(frame.message_type));

    AppendBigEndian<uint16_t>(*output, frame.flags);
    AppendBigEndian<uint32_t>(*output, static_cast<uint32_t>(frame.meta.size()));
    AppendBigEndian<uint32_t>(*output, static_cast<uint32_t>(frame.payload.size()));
    AppendBigEndian<uint64_t>(*output, frame.request_id);

    output->append(frame.meta);
    output->append(frame.payload);


    return true;
}

DecodeStatus RpcCodec::Decode(muduo::net::Buffer* buffer, RpcFrame* frame, std::string& error)
{
    error.clear();

    if (buffer == nullptr || frame == nullptr)
    {
        error = "buffer or frame is null";
        return DecodeStatus::kProtocolError;
    }

    if (buffer->readableBytes() < kFixedHeaderSize)
    {
        return DecodeStatus::kNeedMoreData;
    }

    const char* header = buffer->peek();
    std::string_view header_view(header, kFixedHeaderSize);

    size_t offset = 0; //维护一个本地游标

    for (size_t i = 0; i < kRpcMagic.size(); ++i)
    {
        const auto data = static_cast<uint8_t>(header_view[i]);
        if (data != kRpcMagic[i])
        {
            error = "invalid RPC magic";
            return DecodeStatus::kProtocolError;
        }

        ++offset;
    }
    const uint8_t version = ReadBigEndian<uint8_t>(header_view, offset);
    offset += sizeof(uint8_t);
    if (version != kRpcVersion)
    {
        error = "unsupport RPC version";
        return DecodeStatus::kProtocolError;
    }

    // 协议安全性检查
    const uint8_t message_type = ReadBigEndian<uint8_t>(header_view, offset);
    offset += sizeof(uint8_t);

    const uint8_t request_type =
    static_cast<uint8_t>(RpcMessageType::kRequest);

    const uint8_t response_type =
    static_cast<uint8_t>(RpcMessageType::kResponse);

    if (message_type != request_type &&
        message_type != response_type)
    {
        error = "invalid RPC message type";
        return DecodeStatus::kProtocolError;
    }

    const uint16_t flags = ReadBigEndian<uint16_t>(header_view, offset);
    offset += sizeof(uint16_t);

    if (flags != 0)
    {
        error = "unsupported RPC flags";
        return DecodeStatus::kProtocolError;
    }

    const uint32_t meta_size = ReadBigEndian<uint32_t>(header_view, offset);
    offset += sizeof(uint32_t);

    const uint32_t payload_size = ReadBigEndian<uint32_t>(header_view, offset);
    offset += sizeof(uint32_t);

    const uint64_t request_id = ReadBigEndian<uint64_t>(header_view, offset);
    offset += sizeof(uint64_t);

    if (offset != kFixedHeaderSize)
    {
        error = "invalid fixed header layout";
        return DecodeStatus::kProtocolError;
    }

    if (meta_size > kMaxMetaSize)
    {
        error = "RPC meta is too large";
        return DecodeStatus::kProtocolError;
    }

    if (payload_size > kMaxPayloadSize)
    {
        error = "RPC payload is too large";
        return DecodeStatus::kProtocolError;
    }

    const uint64_t total_size = static_cast<uint64_t>(kFixedHeaderSize) + static_cast<uint64_t>(meta_size) + static_cast<uint64_t>(payload_size);

    if (static_cast<uint64_t>(buffer->readableBytes()) < total_size)
    {
        return DecodeStatus::kNeedMoreData;
    }

    const char* frame_begin = header;
    const char* meta_begin = frame_begin + kFixedHeaderSize;
    const char* payload_begin = meta_begin + meta_size;

    std::string decoded_meta(meta_begin, meta_size);
    std::string decoded_payload(payload_begin, payload_size);

    frame->message_type = static_cast<RpcMessageType>(message_type);
    frame->flags = flags;
    frame->request_id = request_id;
    frame->meta = std::move(decoded_meta);
    frame->payload = std::move(decoded_payload);

    buffer->retrieve(static_cast<size_t>(total_size));

    return DecodeStatus::kFrameReady;
}
