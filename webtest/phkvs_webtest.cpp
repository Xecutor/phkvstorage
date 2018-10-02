#include <iostream>
#include "web_server.hpp"
#include "json_rpc_service.hpp"

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

class phkvs_web_test_app {
public:
    enum {
        version_major = 0,
        version_minor = 1,
    };

    bool init(int argc, const char* const argv[])
    {

        boost::filesystem::path default_path;

        namespace po = boost::program_options;
        po::options_description options("Options");
        options.add_options()
                ("help", "This help page")
                ("web_host,h", po::value<std::string>(&m_web_config.address)->default_value(m_web_config.address))
                ("web_port,p", po::value<uint16_t>(&m_web_config.port)->default_value(m_web_config.port))
                ("default_path,u", po::value<boost::filesystem::path>(&default_path)->default_value("."))
                ("debug,d", "Enable debug output of web server")
                ("open,o", "Open default browser on start");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, options), vm);
        po::notify(vm);

        if(vm.count("help"))
        {
            std::cout << options << std::endl;
            std::cout << "Long options can be specified in phkvs_webtest.ini" << std::endl;
            return false;
        }

        if(vm.count("debug"))
        {
            m_web_config.debug = true;
        }


        m_web_server.init(m_web_config);
        json_rpc_service::config json_rpc_config;
        json_rpc_config.default_path = default_path;
        m_jsonrpc_svc.init(phkvs::PHKVStorage::create(phkvs::PHKVStorage::Options()), json_rpc_config);

        if(vm.count("open"))
        {
            system(fmt::format("start \"\" http://{}:{}/", m_web_config.address, m_web_config.port).c_str());
        }

        m_web_server.registerWsHandler("/json_ws",
                std::bind(&phkvs_web_test_app::json_rpc_handler, this, std::placeholders::_1, std::placeholders::_2));
        return true;
    }

    void start()
    {
        m_web_server.run();
    }

    void shutdown()
    {
        m_web_server.shutdown();
    }

private:
    web_server::config m_web_config;
    web_server m_web_server;
    json_rpc_service m_jsonrpc_svc;


    void json_rpc_handler(const std::string& request, ws_responder& responder)
    {
        rapidjson::Document req;
        rapidjson::StringStream ss(request.c_str());
        req.ParseStream(ss);

        auto errorHandler = [&responder, &req](int code, const char* message) {
            rapidjson::Document resp(rapidjson::kObjectType);

            auto id = req.FindMember("id");
            if(id != req.MemberEnd())
            {
                resp.AddMember("id", id->value.GetInt(), resp.GetAllocator());
            }
            resp.AddMember("jsonrpc", "2.0", resp.GetAllocator());
            auto& error = resp.AddMember("error", rapidjson::kObjectType, resp.GetAllocator());
            error.AddMember("code", code, resp.GetAllocator());
            rapidjson::Value v(message, resp.GetAllocator());
            error.AddMember("message", rapidjson::Value(message, resp.GetAllocator()), resp.GetAllocator());

            rapidjson::StringBuffer sbuf;
            rapidjson::Writer<rapidjson::StringBuffer> writer(sbuf);
            resp.Accept(writer);

            responder.respond(sbuf.GetString());
        };

        try
        {
            if(req.HasParseError())
            {
                throw json_rpc_exception(json_rpc_error::parse_error, "Parse error");
            }
            auto method = req.FindMember("method");
            auto params = req.FindMember("params");
            auto id = req.FindMember("id");
            if(method == req.MemberEnd() ||
               params == req.MemberEnd() ||
               id == req.MemberEnd() ||
               !method->value.IsString() ||
               !id->value.IsInt())
            {
                throw json_rpc_exception(json_rpc_error::invalid_request, "Invalid method format");
            }
            json_rpc_method_params mparams(params->value);
            auto result = m_jsonrpc_svc.callMethod(method->value.GetString(), mparams);

            rapidjson::Document resp(rapidjson::kObjectType);

            resp.AddMember("id", id->value.GetInt(), resp.GetAllocator());
            resp.AddMember("jsonrpc", "2.0", resp.GetAllocator());
            rapidjson::Value res(result.get_document(), resp.GetAllocator());

            resp.AddMember("result", res, resp.GetAllocator());

            rapidjson::StringBuffer sbuf;
            rapidjson::Writer<rapidjson::StringBuffer> writer(sbuf);
            resp.Accept(writer);

            responder.respond(sbuf.GetString());

        }
        catch(json_rpc_exception& e)
        {
            errorHandler(e.get_error(), e.what());
        }
        catch(std::exception& e)
        {
            errorHandler(static_cast<int>(json_rpc_error::internal_error), e.what());
        }

    }
};


int main(int argc, char* argv[])
{
    std::cout << "PHKVS Web Test v" << phkvs_web_test_app::version_major << '.' << phkvs_web_test_app::version_minor
              << std::endl;
    try
    {
        phkvs_web_test_app app;
        if(!app.init(argc, argv))
        {
            return EXIT_FAILURE;
        }
        app.start();
        app.shutdown();

        return EXIT_SUCCESS;
    }
    catch(std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
