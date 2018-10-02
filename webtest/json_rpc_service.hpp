#pragma once

#include <string>
#include <stdexcept>
#include <map>

#include <boost/filesystem.hpp>
#include <rapidjson/document.h>

#include "PHKVStorage.hpp"

enum class json_rpc_error : int{
    parse_error = -32700,
    invalid_request = -32600,
    method_not_found = -32601,
    invalid_params = -32602,
    internal_error = -32603,

    file_open_error = -32000,
    table_not_found = -32001,
    file_parse_error= -32002
};

class json_rpc_exception:public std::runtime_error{
public:
    json_rpc_exception(json_rpc_error error, std::string message):std::runtime_error(std::move(message)), m_error(error)
    {
    }
    int get_error()const noexcept
    {
        return static_cast<int>(m_error);
    }
protected:
    json_rpc_error m_error;
};

class json_rpc_method_params{
public:

    json_rpc_method_params(const rapidjson::Value& params):m_params(params)
    {
    }

    bool getBool(const char* name)const;
    bool getBoolDefault(const char* name, bool defaultValue)const;
    int getInt(const char* name)const;
    int getIntDefault(const char* name, int defaultValue)const;
    const char* getString(const char* name)const;
    const char* getStringDefault(const char* name, const char* defaultValue)const;

    rapidjson::Value::ConstArray getArray(const char* name, rapidjson::Type expectedType = rapidjson::kNullType)const;
    const rapidjson::Value& getObject(const char* name)const;

protected:
    const rapidjson::Value& m_params;
};

class json_rpc_result_value{
public:
protected:
    rapidjson::Document& m_result;
    rapidjson::Value& m_value;
};

class json_rpc_result{
public:
    rapidjson::Document& get_document()
    {
        return m_result;
    }
    template <class T>
    void push_back(T&& item)
    {
        if ( m_result.GetType() != rapidjson::kArrayType ) {
            m_result.SetArray();
        }
        m_result.PushBack(item, m_result.GetAllocator());
    }
    template <size_t N, class T>
    void addMember(const char (&name)[N], T value)
    {
        if ( m_result.GetType() != rapidjson::kObjectType ) {
            m_result.SetObject();
        }
        m_result.GetObject().AddMember(name, value, m_result.GetAllocator());
    }
protected:
    rapidjson::Document m_result;
};

class json_rpc_service{
public:
    json_rpc_service()=default;
    struct config{
        boost::filesystem::path default_path;
    };
    void init(phkvs::PHKVStorage::UniquePtr&& storage, const config& config);
    json_rpc_result callMethod(const char* methodName, const json_rpc_method_params& params)const;
protected:

    boost::filesystem::path m_default_path;
    using method_type = std::function<json_rpc_result(const json_rpc_method_params&)>;
    struct method_record{
        method_type method;
    };
    void registerMethod(const char* name, method_type);
    std::map<std::string, method_record> m_methods;

    phkvs::PHKVStorage::UniquePtr m_storage;

    json_rpc_result get_volumes_list_method(const json_rpc_method_params& params);
    json_rpc_result create_and_mount_volume_method(const json_rpc_method_params& params);
    json_rpc_result mount_volume_method(const json_rpc_method_params& params);

//    json_rpc_result get_db_list_method(const json_rpc_method_params& params);
//
//    json_rpc_result select_method(const json_rpc_method_params& params);
//    json_rpc_result insert_method(const json_rpc_method_params& params);
//    json_rpc_result update_method(const json_rpc_method_params& params);
//    json_rpc_result delete_method(const json_rpc_method_params& params);

    std::map<std::string, std::vector<std::string>> m_dbmap;
};
