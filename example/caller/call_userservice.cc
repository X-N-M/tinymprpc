#include <iostream>
#include "mprpc_app.h"
#include "user.pb.h"
#include "mprpc_channel.h"
int main(int argc, char** argv)
{
    MpRpcApplication::Init(argc, argv);

    RPC::UserServiceRpc_Stub stub(new MpRpcChannel());

    RPC::LoginRequest request;
    request.set_name("香奈美");
    request.set_pwd("123456");

    RPC::LoginResponds response;
    stub.Login(nullptr, &request, &response, nullptr);

    if (response.res().errorcode() == 0)
    {
        std::cout << "Login is success:" << response.sucess() << std::endl;
    }
    else
    {
        std::cout << "Login is error:" << response.res().errormsg() << std::endl;
    }

    return 0;
}