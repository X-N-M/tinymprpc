#include "user.pb.h"
#include <iostream>
#include<string>
#include "mprpc_app.h"
#include "mprpc_provider.h"



class UserService : public RPC::UserServiceRpc
{
    bool Login(std::string name, std::string pwd)
    {
        // std::cout << name << std::endl;
        // std::cout << pwd << std::endl;   注释掉防止终端输出性能损耗
        return true;
    }

    void Login(::google::protobuf::RpcController* controller,
                       const ::RPC::LoginRequest* request,
                       ::RPC::LoginResponds* response,
        ::google::protobuf::Closure* done)
    {
        //获取信息
        std::string name = request->name();
        std::string pwd = request->pwd();
        std::string request_payload = name + pwd;
        // 本地业务
        bool login_res=Login(name, pwd);

        RPC::ResultCode* code = response->mutable_res();
        code->set_errorcode(0);
        code->set_errormsg(request_payload);

        response->set_sucess(login_res);

        done->Run();

    }
};

int main(int argc, char** argv)
{
    MpRpcApplication::Init(argc, argv);

    RpcProvider provider;

    provider.NotifyService(new UserService());

    provider.Run();

    return 0;
}