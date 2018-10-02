cmake_minimum_required(VERSION 3.0)

find_package(Boost 1.67 REQUIRED COMPONENTS system date_time filesystem locale program_options)
find_package(OpenSSL REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(fmt REQUIRED)

add_executable(phkvs_webtest 
    phkvs_webtest.cpp
    web_server.cpp
    json_rpc_service.cpp)

target_include_directories(phkvs_webtest PRIVATE ${RAPIDJSON_INCLUDE_DIRS})
target_compile_definitions(phkvs_webtest PRIVATE RAPIDJSON_HAS_STDSTRING=1)

target_link_libraries(
    phkvs_webtest
    Boost::system
    Boost::date_time
    Boost::filesystem
    Boost::locale
    Boost::program_options

    fmt::fmt

    OpenSSL::SSL
    OpenSSL::Crypto

    phkvstorage
)

if(WIN32)
  target_compile_definitions(phkvs_webtest PRIVATE _WIN32_WINNT=0x0601 _CRT_SECURE_NO_WARNINGS=1 _SCL_SECURE_NO_WARNINGS=1)
endif()
