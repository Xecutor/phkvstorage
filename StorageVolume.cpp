//
// Created by konst on 12-Sep-18.
//

#include "StorageVolume.hpp"

namespace phkvs{

struct StorageVolume::PrivateKey{};

std::unique_ptr<StorageVolume>
StorageVolume::open(FileSystem::UniqueFilePtr&& mainFile, FileSystem::UniqueFilePtr&& stmFile,
                           FileSystem::UniqueFilePtr&& bigFile)
{
    PrivateKey pkey;
    auto rv = std::make_unique<StorageVolume>(pkey, std::move(mainFile));
    rv->m_stmStorage = SmallToMediumFileStorage::open(std::move(stmFile));
    rv->m_bigStorage = BigFileStorage::open(std::move(bigFile));
    return rv;
}

std::unique_ptr<StorageVolume>
StorageVolume::create(FileSystem::UniqueFilePtr&& mainFile, FileSystem::UniqueFilePtr&& stmFile,
                      FileSystem::UniqueFilePtr&& bigFile)
{
    PrivateKey pkey;
    auto rv = std::make_unique<StorageVolume>(pkey, std::move(mainFile));
    rv->m_stmStorage = SmallToMediumFileStorage::create(std::move(stmFile));
    rv->m_bigStorage = BigFileStorage::create(std::move(bigFile));
    return rv;
}

}