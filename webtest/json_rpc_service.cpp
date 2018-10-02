#include "json_rpc_service.hpp"
#include <rapidjson/filereadstream.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/prettywriter.h>
#include <fmt/format.h>

bool json_rpc_method_params::getBool(const char* name) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd() || !member->value.IsBool()) {
        throw json_rpc_exception(json_rpc_error::invalid_params, name);
    }
    return member->value.GetBool();
}

bool json_rpc_method_params::getBoolDefault(const char* name, bool defaultValue) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd()) {
        if (!member->value.IsBool()) {
            throw json_rpc_exception(json_rpc_error::invalid_params, name);
        }
        return defaultValue;
    }
    return member->value.GetBool();
}

int json_rpc_method_params::getInt(const char* name) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd() || !member->value.IsInt()) {
        throw json_rpc_exception(json_rpc_error::invalid_params, name);
    }
    return member->value.GetInt();
}

int json_rpc_method_params::getIntDefault(const char* name, int defaultValue) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd()) {
        if (!member->value.IsInt()) {
            throw json_rpc_exception(json_rpc_error::invalid_params, name);
        }
        return defaultValue;
    }
    return member->value.GetInt();
}

const char* json_rpc_method_params::getString(const char* name) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd() || !member->value.IsString()) {
        throw json_rpc_exception(json_rpc_error::invalid_params, name);
    }
    return member->value.GetString();
}

const char* json_rpc_method_params::getStringDefault(const char* name, const char* defaultValue) const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd()) {
        if (!member->value.IsString()) {
            throw json_rpc_exception(json_rpc_error::invalid_params, name);
        }
        return defaultValue;
    }
    return member->value.GetString();
}

rapidjson::Value::ConstArray json_rpc_method_params::getArray(const char* name, rapidjson::Type expectedType)const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd() || !member->value.IsArray()) {
        throw json_rpc_exception(json_rpc_error::invalid_params, name);
    }
    if (expectedType != rapidjson::kNullType) {
        for(auto& v:member->value.GetArray()) {
            if(v.GetType()!=expectedType) {
                throw json_rpc_exception(json_rpc_error::invalid_params, name);
            }
        }
    }
    return member->value.GetArray();
}

const rapidjson::Value& json_rpc_method_params::getObject(const char* name)const
{
    auto member = m_params.FindMember(name);
    if (member == m_params.MemberEnd() || !member->value.IsObject()) {
        throw json_rpc_exception(json_rpc_error::invalid_params, name);
    }
    return member->value;
}

void json_rpc_service::init(phkvs::PHKVStorage::UniquePtr&& storage, const config& config)
{
    m_storage = std::move(storage);
    m_default_path = config.default_path;

    registerMethod("get_volumes_list",[this](const json_rpc_method_params& params){return get_volumes_list_method(params);});
    registerMethod("create_and_mount_volume",[this](const json_rpc_method_params& params){return create_and_mount_volume_method(params);});
    registerMethod("mount_volume",[this](const json_rpc_method_params& params){return mount_volume_method(params);});

//    registerMethod("get_db_list",[this](const json_rpc_method_params& params){return get_db_list_method(params);});
//    registerMethod("select",[this](const json_rpc_method_params& params){return select_method(params);});
//    registerMethod("insert",[this](const json_rpc_method_params& params){return insert_method(params);});
//    registerMethod("update",[this](const json_rpc_method_params& params){return update_method(params);});
//    registerMethod("delete",[this](const json_rpc_method_params& params){return delete_method(params);});

//    prepare_db_map();
}

json_rpc_result json_rpc_service::callMethod(const char* methodName, const json_rpc_method_params& params)const
{
    auto it = m_methods.find(methodName);
    if (it == m_methods.end()) {
        throw json_rpc_exception(json_rpc_error::method_not_found, methodName);
    }
    return it->second.method(params);
}

//void json_rpc_service::prepare_db_map()
//{
//    rapidjson::Document dbs;
//    load_json_file("database.json", dbs);
//    for (auto& db : dbs.GetArray()) {
//        json_rpc_method_params dbparam(db);
//        std::string dbName = dbparam.getString("TableName");
//        std::vector<std::string> files;
//        for (auto& file : dbparam.getArray("JSON")) {
//            json_rpc_method_params fileparam(file);
//            files.push_back(fileparam.getString("File"));
//        }
//        m_dbmap.emplace(std::move(dbName), std::move(files));
//    }
//}
//
//void json_rpc_service::load_json_file(const std::string& filename, rapidjson::Document& doc)
//{
//    auto path = m_data_path / filename;
//    FILE* f = fopen(path.string().c_str(), "rb");
//    if (!f) {
//        throw json_rpc_exception(json_rpc_error::file_open_error, path.string().c_str());
//    }
//    std::unique_ptr<FILE, decltype(&fclose)> fguard(f, &fclose);
//    char buf[4096];
//    rapidjson::FileReadStream frs(f, buf, sizeof(buf));
//    doc.ParseStream<rapidjson::kParseCommentsFlag|rapidjson::kParseTrailingCommasFlag>(frs);
//    if(doc.HasParseError()) {
//        throw json_rpc_exception(json_rpc_error::file_parse_error, fmt::format("File '{}', error at offset {}", filename, doc.GetErrorOffset()));
//    }
//}
//
//void json_rpc_service::store_json_file(const std::string& filename, const rapidjson::Document& doc)
//{
//    auto path = m_data_path / filename;
//    if ( m_perform_backup ) {
//        auto backup_path_main = path;
//        int idx = 0;
//
//        if ( !m_backup_path.empty() ) {
//            if ( m_backup_path.is_relative() ) {
//                backup_path_main = path;
//            }
//            else {
//                backup_path_main = m_backup_path / filename;
//            }
//            auto filename_only = backup_path_main.filename();
//            backup_path_main.remove_filename();
//            if ( m_backup_path.is_relative() ) {
//                backup_path_main /= m_backup_path;
//            }
//            if ( !boost::filesystem::exists(backup_path_main) ) {
//                boost::filesystem::create_directories(backup_path_main);
//            }
//            backup_path_main /= filename_only;
//        }
//
//        boost::filesystem::path backup_path;
//
//        do {
//            backup_path = backup_path_main;
//            auto ext = backup_path.extension();
//            backup_path.replace_extension(fmt::format("{:03}", idx));
//            backup_path += ext;
//            ++idx;
//        } while ( boost::filesystem::exists(backup_path) );
//
//        //printf("rename '%s' to '%s'\n", path.string().c_str(), backup_path.string().c_str());
//        boost::filesystem::rename(path, backup_path);
//    }
//
//    FILE* f = fopen(path.string().c_str(), "wb");
//    if (!f) {
//        throw json_rpc_exception(json_rpc_error::file_open_error, path.string().c_str());
//    }
//    std::unique_ptr<FILE, decltype(&fclose)> fguard(f, &fclose);
//
//    char buf[4096];
//    rapidjson::FileWriteStream fws(f, buf, sizeof(buf));
//    rapidjson::PrettyWriter<rapidjson::FileWriteStream> writer(fws);
//    writer.SetIndent('\t', 1);
//    doc.Accept(writer);
//
//}

void json_rpc_service::registerMethod(const char* name, method_type function)
{
    m_methods.emplace(name, method_record{function});
}

json_rpc_result json_rpc_service::get_volumes_list_method(const json_rpc_method_params& params)
{
    auto volumes = m_storage->getMountVolumesInfo();
    json_rpc_result result;
    auto& arr = result.get_document().SetArray();
    auto& a = result.get_document().GetAllocator();
    for(auto& vol:volumes)
    {
//        boost::filesystem::path volumePath;
//        std::string volumeName;
//        std::string mountPointPath;
//        VolumeId volumeId;
        rapidjson::Value item;
        item.SetObject();
        item.AddMember("volumePath", vol.volumePath.string(), a);
        item.AddMember("volumeName", vol.volumeName, a);
        item.AddMember("mountPointPath", vol.mountPointPath, a);
        item.AddMember("volumeId", vol.volumeId, a);
        arr.PushBack(item, a);
    }
    return result;
}

json_rpc_result json_rpc_service::create_and_mount_volume_method(const json_rpc_method_params& params)
{
    boost::filesystem::path volumePath = params.getString("volumePath");
    auto volumeName = params.getString("volumeName");
    auto mountPointPath = params.getString("mountPointPath");
    if(volumePath.is_relative())
    {
        volumePath = m_default_path / volumePath;
    }
    fmt::print("volumePath={}, volumeName={}, mountPointPath={}", volumePath.string(), volumeName, mountPointPath);
    auto volId = m_storage->createAndMountVolume(volumePath, volumeName, mountPointPath);
    json_rpc_result result;
    result.get_document().AddMember("volumeId", volId, result.get_document().GetAllocator());
    return result;
}

json_rpc_result json_rpc_service::mount_volume_method(const json_rpc_method_params& params)
{
    boost::filesystem::path volumePath = params.getString("volumePath");
    auto volumeName = params.getString("volumeName");
    auto mountPointPath = params.getString("mountPointPath");
    if(volumePath.is_relative())
    {
        volumePath = m_default_path / volumePath;
    }
    auto volId = m_storage->mountVolume(volumePath, volumeName, mountPointPath);
    json_rpc_result result;
    result.get_document().AddMember("volumeId", volId, result.get_document().GetAllocator());
    return result;
}

//json_rpc_result json_rpc_service::get_db_list_method(const json_rpc_method_params& params)
//{
//    json_rpc_result result;
//    load_json_file("database.json", result.get_document());
//    return result;
//}

//json_rpc_result json_rpc_service::select_method(const json_rpc_method_params& params)
//{
//    auto tablename = params.getString("table");
//    auto filter = params.getStringDefault("ID", nullptr);
//    auto it = m_dbmap.find(tablename);
//    if(it==m_dbmap.end()) {
//        throw json_rpc_exception(json_rpc_error::table_not_found, tablename);
//    }
//    json_rpc_result result;
//    result.get_document().SetArray();
//    auto& a = result.get_document().GetAllocator();
//    for(auto& file:it->second) {
//        rapidjson::Document tbl(&a);
//        load_json_file(file, tbl);
//        result.get_document().GetArray().Reserve(result.get_document().GetArray().Size() + tbl.GetArray().Size(), a);
//        for(auto& item:tbl.GetArray()) {
//            if ( filter && item.GetObject().FindMember("ID")->value != filter ) {
//                continue;
//            }
//            //rapidjson::Value itemCopy(item, result.get_document().GetAllocator());
//            item.AddMember("_filename", file, result.get_document().GetAllocator());
//            result.push_back(std::move(item));
//        }
//    }
//    return result;
//}
//
//json_rpc_result json_rpc_service::insert_method(const json_rpc_method_params& params)
//{
//    rapidjson::Document doc;
//    const std::string filename = params.getString("_filename");
//    load_json_file(filename, doc);
//    rapidjson::Value obj(params.getObject("object"), doc.GetAllocator());
//    doc.GetArray().PushBack(obj, doc.GetAllocator());
//    store_json_file(filename, doc);
//    json_rpc_result res;
//    res.addMember("status", 1);
//    return res;
//}
//
//json_rpc_result json_rpc_service::update_method(const json_rpc_method_params& params)
//{
//    rapidjson::Document doc;
//    const std::string filename = params.getString("_filename");
//    load_json_file(filename, doc);
//    rapidjson::Value obj(params.getObject("object"), doc.GetAllocator());
//
//    auto& ID = obj["ID"];
//
//    for(auto& item:doc.GetArray()) {
//        if ( item["ID"] == ID ) {
//            item = obj;
//            break;
//        }
//    }
//    store_json_file(filename, doc);
//    json_rpc_result res;
//    res.addMember("status", 1);
//    return res;
//}
//
//json_rpc_result json_rpc_service::delete_method(const json_rpc_method_params& params)
//{
//    rapidjson::Document doc;
//    const std::string filename = params.getString("_filename");
//    load_json_file(filename, doc);
//
//    auto ID = params.getStringDefault("ID", nullptr);
//
//    int status = 0;
//
//    if(ID) {
//        for(auto it = doc.GetArray().begin(), end = doc.GetArray().end();it!=end;++it) {
//            auto& item = *it;
//            if ( item["ID"] == ID ) {
//                doc.GetArray().Erase(it);
//                status = 1;
//                break;
//            }
//        }
//    }
//    else {
//        int idx = params.getInt("idx");
//        doc.GetArray().Erase(doc.GetArray().begin() + idx);
//        status = 1;
//    }
//    store_json_file(filename, doc);
//    json_rpc_result res;
//    res.addMember("status", status);
//    return res;
//}
