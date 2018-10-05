#include <chrono>
#include <thread>
#include <functional>

#include <PHKVStorage.hpp>

#include <fmt/printf.h>
#include <StringViewFormatter.hpp>

using phkvs::PHKVStorage;

void executeBenchmark(boost::string_view benchName, const std::function<void()>& benchFunc)
{
    fmt::print("===============================\n");
    fmt::print("Starting benchmark '{}'\n", benchName);
    auto start = std::chrono::high_resolution_clock::now();
    benchFunc();
    auto duration = std::chrono::high_resolution_clock::now() - start;
    fmt::print("Benchmark '{}' executed in {} ms\n", benchName,
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
}

int main()
{
    try
    {
        {
            const size_t NVolumes = 1024;
            const size_t NKeys = 1000;
            const size_t NLookups = 1000;
            {
                PHKVStorage::Options opt;
                opt.cachePoolSize = 200000;
                auto storage = phkvs::PHKVStorage::create(opt);
                executeBenchmark(fmt::format("create {} volumes", NVolumes), [&storage, NVolumes]() {
                    for(size_t i = 0; i < NVolumes; ++i)
                    {
                        storage->createAndMountVolume(".", fmt::format("volume{}", i), fmt::format("/vol{}", i));
                    }
                });

                executeBenchmark(fmt::format("insert {} int values into each of {} volumes", NKeys, NVolumes),
                        [&storage, NVolumes, NKeys]() {
                            for(size_t i = 0; i < NVolumes; ++i)
                            {
                                for(size_t j = 0; j < NKeys; ++j)
                                {
                                    storage->store(fmt::format("/vol{}/key{}", i, j), static_cast<uint64_t>(i * j));
                                }
                            }
                        });

                executeBenchmark(fmt::format("lookup {} int values in each of {} volumes", NKeys, NVolumes),
                        [&storage, NVolumes, NKeys]() {
                            for(size_t i = 0; i < NVolumes; ++i)
                            {
                                for(size_t j = 0; j < NKeys; ++j)
                                {
                                    auto valOpt = storage->lookup(fmt::format("/vol{}/key{}", i, j));
                                    if(!valOpt)
                                    {
                                        throw std::runtime_error(fmt::format("key '/vol{}/key{}' not found", i, j));
                                    }
                                    auto value = boost::get<uint64_t>(*valOpt);
                                    if(value != i * j)
                                    {
                                        throw std::runtime_error(
                                                fmt::format("Value of key '/vol{}/key{}' expected {}, found {}", i, j,
                                                        i * j,
                                                        value));
                                    }
                                }
                            }
                        });
                executeBenchmark(fmt::format("lookup {} int values in 1 volume {} times", NKeys, NLookups),
                        [&storage, NVolumes, NKeys, NLookups]() {
                            size_t i = NVolumes / 2;
                            for(size_t k = 0; k < NLookups; ++k)
                            {
                                for(size_t j = 0; j < NKeys; ++j)
                                {
                                    auto valOpt = storage->lookup(fmt::format("/vol{}/key{}", i, j));
                                    if(!valOpt)
                                    {
                                        throw std::runtime_error(fmt::format("key '/vol{}/key{}' not found", i, j));
                                    }
                                    auto value = boost::get<uint64_t>(*valOpt);
                                    if(value != i * j)
                                    {
                                        throw std::runtime_error(
                                                fmt::format("Value of key '/vol{}/key{}' expected {}, found {}", i, j,
                                                        i * j,
                                                        value));
                                    }
                                }
                            }
                        });
                executeBenchmark(
                        fmt::format("lookup {} int values in each of {} volumes concurrently in {} threads", NKeys,
                                NVolumes, std::thread::hardware_concurrency()),
                        [&storage, NVolumes, NKeys]() {
                            std::vector<std::thread> thr;
                            for(size_t t = 0; t < std::thread::hardware_concurrency(); ++t)
                            {
                                thr.emplace_back([&storage, NVolumes, NKeys, t]() {
                                    size_t hcnt = std::thread::hardware_concurrency();
                                    for(size_t i = t; i < NVolumes; i += hcnt)
                                    {
                                        for(size_t j = 0; j < NKeys; ++j)
                                        {
                                            auto valOpt = storage->lookup(fmt::format("/vol{}/key{}", i, j));
                                            if(!valOpt)
                                            {
                                                throw std::runtime_error(
                                                        fmt::format("key '/vol{}/key{}' not found", i, j));
                                            }
                                            auto value = boost::get<uint64_t>(*valOpt);
                                            if(value != i * j)
                                            {
                                                throw std::runtime_error(
                                                        fmt::format("Value of key '/vol{}/key{}' expected {}, found {}",
                                                                i, j, i * j, value));
                                            }
                                        }
                                    }
                                });
                            }
                            for(auto& t:thr)
                            {
                                t.join();
                            }
                        });
            }
            for(size_t i = 0; i < NVolumes; ++i)
            {
                PHKVStorage::deleteVolume(".", fmt::format("volume{}", i));
            }
        }
    }
    catch(std::exception& e)
    {
        fmt::print("Exception during benchmark:{}\n", e.what());
    }
}
