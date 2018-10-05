add_executable(phkvs_benchmark phkvs_benchmark.cpp)
target_link_libraries(phkvs_benchmark PRIVATE phkvstorage)

if(UNIX)
    target_link_libraries(phkvs_benchmark PRIVATE pthread)
endif()
