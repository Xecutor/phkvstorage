cmake_minimum_required(VERSION 3.10)

project(HKVStorage)

cmake_policy(SET CMP0003 NEW)

if(NOT VCPKG_ROOT AND DEFINED ENV{VCPKG_ROOT})
    set(VCPKG_ROOT $ENV{VCPKG_ROOT})
endif()

if(VCPKG_ROOT)
    include(${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
endif()

set(CMAKE_CXX_STANDARD 14)

if(MSVC)
    add_definitions(-D_WIN32_WINNT=0x0601) #win7
endif()

set(Boost_USE_STATIC_RUNTIME OFF)
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)

find_package(Boost 1.67 REQUIRED COMPONENTS system filesystem)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

#target_link_libraries(buf PRIVATE Boost::system fmt::fmt spdlog::spdlog)

add_library(phkvstorage FileSystem.cpp SmallToMediumFileStorage.cpp SmallToMediumFileStorage.hpp FileVersion.hpp UIntArrayHexFormatter.hpp FileOpsHelpers.hpp BigFileStorage.cpp test_bigfilestorage.hpp PHKVStorage.cpp PHKVStorage.hpp StorageVolume.cpp StorageVolume.hpp KeyPathUtil.hpp BlockSkipList.cpp BlockSkipList.hpp)

target_link_libraries(phkvstorage Boost::system Boost::filesystem fmt::fmt spdlog::spdlog)
target_include_directories(phkvstorage PUBLIC ${PROJECT_SOURCE_DIR})

add_subdirectory(tests)

enable_testing()
add_test(NAME simpletest COMMAND test_simple)
add_test(NAME bufferstest COMMAND test_buffers)
add_test(NAME filetest COMMAND test_file)
add_test(NAME stmfilestoragetest COMMAND test_stmfilestorage)
add_test(NAME bigfilestoragetest COMMAND test_bigfilestorage)
