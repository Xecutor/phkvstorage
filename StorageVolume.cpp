#include "StorageVolume.hpp"

#include <chrono>
#include <random>
#include <thread>

#include "UIntArrayHexFormatter.hpp"
#include "KeyPathUtil.hpp"
#include "FileOpsHelpers.hpp"
#include "StringViewFormatter.hpp"
#include "FileMagic.hpp"
#include "FileVersion.hpp"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>

namespace phkvs {


namespace {

uint64_t nowInMilliseconds()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

const char* s_loggingCategory = "StorageVolume";

class StorageVolumeImpl : public StorageVolume {
public:
    StorageVolumeImpl(FileSystem::UniqueFilePtr&& mainFile,
                      SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                      BigFileStorage::UniquePtr&& bigFileStorage);

    void store(boost::string_view keyPath, const ValueType& value) override;

    void storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime) override;

    boost::optional<ValueType> lookup(boost::string_view keyPath) override;

    void eraseKey(boost::string_view keyPath) override;

    void eraseDirRecursive(boost::string_view dirPath) override;

    boost::optional<std::vector<DirEntry>> getDirEntries(boost::string_view dirPath) override;

    void dump(const std::function<void(const std::string&)>& out) override;

    void openImpl();

    void createImpl();

private:
    using OffsetType = IRandomAccessFile::OffsetType;

    static const FileMagic s_magic;
    static const FileVersion s_currentVersion;

    static constexpr size_t k_headerSize = FileMagic::binSize() + FileVersion::binSize() +
                                           sizeof(OffsetType) + sizeof(OffsetType);
    static constexpr size_t k_rootListOffset = k_headerSize;
    static constexpr size_t k_inplaceSize = 16;
    static constexpr size_t k_entriesPerNode = 16;
    static constexpr size_t k_maxListHeight = 16;

    void store(boost::string_view keyPath, const ValueType& value, uint64_t expTime);

    struct KeyInfo {
        std::string value;
        OffsetType offset = 0;

        static constexpr size_t binSize()
        {
            //up to 16 chars string is inplace
            //or size + offset for longer strings
            return 8 + 8;
        }
    };

    void storeKey(OutputBinBuffer& out, KeyInfo& key);

    void loadKey(InputBinBuffer& in, bool isInplace, KeyInfo& key);

    void loadInplaceString(InputBinBuffer& in, std::string& value);

    void loadInplaceVector(InputBinBuffer& in, std::vector<uint8_t>& value);

    struct ValueInfo {
        ValueType value;
        OffsetType offset = 0;
        size_t previousSize = 0;

        static constexpr size_t binSize()
        {
            //up to 16 chars string is inplace
            //or size + offset for bigger data
            return 8 + 8;
        }

    };

    void storeValue(OutputBinBuffer& out, ValueInfo& value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, uint8_t value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, uint16_t value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, uint32_t value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, uint64_t value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, float value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, double value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, const std::string& value);

    void storeValueType(OutputBinBuffer& out, ValueInfo& info, const std::vector<uint8_t>& value);

    enum class ValueTypeIndex {
        idx_uint8_t = 0,
        idx_uint16_t = 1,
        idx_uint32_t = 2,
        idx_uint64_t = 3,
        idx_float = 4,
        idx_double = 5,
        idx_string = 6,
        idx_vector = 7
    };

    struct ValueTypeIndexVisitor {
        constexpr ValueTypeIndex operator()(uint8_t) const
        {
            return ValueTypeIndex::idx_uint8_t;
        }

        constexpr ValueTypeIndex operator()(uint16_t) const
        {
            return ValueTypeIndex::idx_uint16_t;
        }

        constexpr ValueTypeIndex operator()(uint32_t) const
        {
            return ValueTypeIndex::idx_uint32_t;
        }

        constexpr ValueTypeIndex operator()(uint64_t) const
        {
            return ValueTypeIndex::idx_uint64_t;
        }

        constexpr ValueTypeIndex operator()(float) const
        {
            return ValueTypeIndex::idx_float;
        }

        constexpr ValueTypeIndex operator()(double) const
        {
            return ValueTypeIndex::idx_double;
        }

        constexpr ValueTypeIndex operator()(const std::string&) const
        {
            return ValueTypeIndex::idx_string;
        }

        constexpr ValueTypeIndex operator()(const std::vector<uint8_t>&) const
        {
            return ValueTypeIndex::idx_vector;
        }
    };

    struct ValueTypeLengthVisitor {
        template<class T>
        size_t operator()(T) const
        {
            return sizeof(T);
        }

        size_t operator()(const std::string& value) const
        {
            return value.length();
        }

        size_t operator()(const std::vector<uint8_t>& value)
        {
            return value.size();
        }
    };

    static size_t calcValueLength(ValueInfo& info);

    void loadValue(InputBinBuffer& in, ValueTypeIndex typeIndex, bool isInplace, ValueInfo& value);

    void loadValueString(InputBinBuffer& in, bool isInplace, ValueInfo& value);

    void loadValueVector(InputBinBuffer& in, bool isInplace, ValueInfo& value);

    static bool isInplaceLength(size_t length)
    {
        return length <= k_inplaceSize;
    }

    static bool isInplaceValueLength(size_t length)
    {
        return length < k_inplaceSize;
    }

    static bool isSmallToMediumLenght(size_t length)
    {
        return length <= SmallToMediumFileStorage::maxDataSize();
    }

    enum class EntryFlags : uint8_t {
        dir = 0x80,
        inplaceKey = 0x40,
        inplaceValue = 0x20,
        valueTypeMask = 0x0f
    };

    struct Entry {
        EntryType type;
        uint64_t expirationDateTime;
        KeyInfo key;
        ValueInfo value;

        void setDir(std::string name, OffsetType offset)
        {
            type = EntryType::dir;
            expirationDateTime = 0;
            key.value = std::move(name);
            value.value = offset;
        }

        void setValue(std::string newName, ValueType newValue, uint64_t expDate = 0)
        {
            type = EntryType::key;
            expirationDateTime = expDate;
            key.value = std::move(newName);
            value.value = std::move(newValue);
        }

        static constexpr size_t binSize()
        {
            /*
             * bit 7 - 0 key 1 dir
             * bit 6 - 1 inplace key 0 external storage
             * bit 5 - 1 inplace value 0 external storage
             * bit 4 - reserved
             * bits 0..3 - value type (16 types).
             *
             * 0 - uint8_t
             * 1 - uint16_t
             * 2 - uint32_t
             * 3 - uint64_t
             * 4 - float
             * 5 - double
             * 6 - string
             * 7 - blob/vector
             */
            return 1 /*flags*/ + 8 /*exp date*/ + KeyInfo::binSize() + ValueInfo::binSize();
        }
    };

    struct EntryKeyComparator {
        bool operator()(const Entry& left, const Entry& right) const
        {
            return left.key.value < right.key.value;
        }

        bool operator()(const Entry& left, const boost::string_view& right) const
        {
            return left.key.value < right;
        }

        bool operator()(const boost::string_view& left, const Entry& right) const
        {
            return left < right.key.value;
        }
    };

    void storeEntry(OutputBinBuffer& out, Entry& entry);

    void loadEntry(InputBinBuffer& in, Entry& entry);

    void loadEntryKey(InputBinBuffer& in, std::string& key);

    void freeEntry(Entry& entry);

    using EntriesVector = std::vector<Entry>;
    using NextsVector = std::vector<OffsetType>;

    struct SkipListNode {
        NextsVector nexts;
        OffsetType nextOffset = 0;
        EntriesVector entries;

        static constexpr size_t binSize()
        {
            return 1/*next size*/ + sizeof(OffsetType) + 1/*entries*/ + k_entriesPerNode * Entry::binSize();
        }

        static constexpr size_t binHeadSize()
        {
            return 1/*next size*/ + sizeof(OffsetType);
        }
    };

    OffsetType allocateSkipListHeadNode();

    OffsetType createSkipListHeadNode();

    OffsetType allocateSkipListNode();

    void freeSkipListHeadNode(OffsetType offset);

    void freeSkipListNode(OffsetType offset);

    void storeNode(OffsetType offset, SkipListNode& node);

    void loadNode(OffsetType offset, SkipListNode& node);

    void loadHeadNode(OffsetType offset, SkipListNode& node);

    void storeHeadNode(OffsetType offset, SkipListNode& node);

    void storeNode(OutputBinBuffer& out, SkipListNode& node);

    void loadNode(InputBinBuffer& in, SkipListNode& node);

    void loadHeadNode(InputBinBuffer& in, SkipListNode& node);

    void storeHeadNode(OutputBinBuffer& out, SkipListNode& node);

    OffsetType loadNodeNexts(InputBinBuffer& in, uint8_t nextsCount, NextsVector& nexts);

    OffsetType storeNodeNexts(OutputBinBuffer& out, OffsetType offset, const NextsVector& nexts);

    enum class EdgeKey {
        none,
        first,
        last
    };

    void loadNodeNextsAndEdgeKey(OffsetType offset, NextsVector& nexts, EdgeKey whichKey, std::string& key);

    using ListPath = std::array<OffsetType, k_maxListHeight>;

    void findPath(OffsetType headOffset, ListPath& path, const boost::string_view& key);

    size_t generateNewLevel();

    void listInsert(OffsetType headOffset, Entry&& entry);

    bool listLookup(OffsetType headOffset, const boost::string_view& key, Entry& entry);

    void listErase(OffsetType head, EntryType type, const boost::string_view& key);

    void listEraseRecursive(OffsetType nodeHeadOffset);

    void listGetContent(OffsetType nodeHeadOffset, std::vector<DirEntry>& entries);

    OffsetType followPath(const std::vector<boost::string_view>& path);

    void dumpList(OffsetType headOffset, size_t indent, const std::function<void(const std::string&)>& out);

    FileSystem::UniqueFilePtr m_mainFile;
    OffsetType m_firstFreeListNode = 0;
    OffsetType m_firstFreeHeadListNode = 0;
    SmallToMediumFileStorage::UniquePtr m_stmStorage;
    BigFileStorage::UniquePtr m_bigStorage;

    std::string m_lastDir;
    OffsetType m_lastDirHeadOffset = 0;

    std::mt19937 m_random;

    using LoggerType = decltype(spdlog::get({}));

    LoggerType& getLogger();

    LoggerType m_log;
};

const FileMagic StorageVolumeImpl::s_magic = {{'P', 'H', 'V', 'L'}};
const FileVersion StorageVolumeImpl::s_currentVersion = {0x0001, 0x0000};

StorageVolumeImpl::StorageVolumeImpl(FileSystem::UniqueFilePtr&& mainFile,
                                     SmallToMediumFileStorage::UniquePtr&& stmFileStorage,
                                     BigFileStorage::UniquePtr&& bigFileStorage) :
        m_mainFile(std::move(mainFile)),
        m_stmStorage(std::move(stmFileStorage)),
        m_bigStorage(std::move(bigFileStorage))
{
    std::hash<std::thread::id> hasher;
    std::seed_seq seed{
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()),
            hasher(std::this_thread::get_id())
    };
    m_random.seed(seed);
}


void StorageVolumeImpl::openImpl()
{
    auto fileSize = m_mainFile->seekEnd();
    if(fileSize < k_headerSize)
    {
        throw std::runtime_error(
                fmt::format("StorageVolume::open: Unexpected file size of {}:{}",
                        m_mainFile->getFilename().string(), fileSize));
    }
    std::array<uint8_t, k_headerSize> headerData{};
    auto buf = boost::asio::buffer(headerData);
    m_mainFile->seek(0);
    m_mainFile->read(buf);
    InputBinBuffer in(buf);

    FileMagic magic;
    magic.deserialize(in);
    if(magic != s_magic)
    {
        throw std::runtime_error(
                fmt::format("StorageVolume::open: invalid magic in file {}. Expected {}, but found {}",
                        m_mainFile->getFilename().string(), s_magic, magic));
    }

    FileVersion version{0, 0};
    version.deserialize(in);
    if(version != s_currentVersion)
    {
        throw std::runtime_error(
                fmt::format("StorageVolume::open: invalid version of file {}. Expected {}, but found {}",
                        m_mainFile->getFilename().string(), s_magic, magic));
    }
    m_firstFreeHeadListNode = in.readU64();
    m_firstFreeListNode = in.readU64();
}

void StorageVolumeImpl::createImpl()
{
    auto fileSize = m_mainFile->seekEnd();
    if(fileSize != 0)
    {
        throw std::runtime_error(fmt::format("StorageVolume::create: file {} must be empty, but size={}",
                m_mainFile->getFilename().string(), fileSize));
    }
    std::array<uint8_t, k_headerSize + SkipListNode::binSize()> headerData{};
    auto buf = boost::asio::buffer(headerData);
    OutputBinBuffer out(buf);
    s_magic.serialize(out);
    s_currentVersion.serialize(out);
    out.writeU64(0);
    out.writeU64(0);
    SkipListNode rootNode;
    rootNode.nexts.resize(k_maxListHeight);
    storeHeadNode(out, rootNode);
    m_mainFile->write(buf);
}

StorageVolumeImpl::LoggerType& StorageVolumeImpl::getLogger()
{
    if(m_log)
    {
        return m_log;
    }
    m_log = spdlog::get(s_loggingCategory);
    if(!m_log)
    {
        m_log = spdlog::default_factory::create<spdlog::sinks::null_sink_mt>(s_loggingCategory);
    }
    return m_log;
}

void StorageVolumeImpl::storeKey(OutputBinBuffer& out, KeyInfo& key)
{
    if(isInplaceLength(key.value.length()))
    {
        std::array<uint8_t, k_inplaceSize> data{};
        std::copy(key.value.begin(), key.value.end(), data.begin());
        out.writeArray(data);
        return;
    }

    if(isSmallToMediumLenght(key.value.length()))
    {
        if(key.offset == 0)
        {
            key.offset = m_stmStorage->allocateAndWrite(boost::asio::buffer(key.value));
        }
    }
    else //long key
    {
        if(key.offset == 0)
        {
            key.offset = m_bigStorage->allocateAndWrite(boost::asio::buffer(key.value));
        }
    }
    out.writeU64(key.value.length());
    out.writeU64(key.offset);
}

void StorageVolumeImpl::loadKey(InputBinBuffer& in, bool isInplace, KeyInfo& key)
{
    if(isInplace)
    {
        loadInplaceString(in, key.value);
        return;
    }
    auto keyLength = static_cast<size_t>(in.readU64());
    //some sane key length limit check?
    key.value.resize(keyLength);

    key.offset = in.readU64();
    if(isSmallToMediumLenght(keyLength))
    {
        m_stmStorage->read(key.offset, boost::asio::buffer(key.value));
    }
    else
    {
        m_bigStorage->read(key.offset, boost::asio::buffer(key.value));
    }
}

void StorageVolumeImpl::loadInplaceString(InputBinBuffer& in, std::string& value)
{
    std::array<uint8_t, k_inplaceSize> data{};
    in.readArray(data);
    //If there is no zero, all 16 bytes are used, find
    //will return end, end - begin = 16, all 16 bytes will be assigned to string.
    auto zeroPos = std::find(data.begin(), data.end(), 0) - data.begin();
    value.assign(reinterpret_cast<const char*>(data.data()), zeroPos);
}

void StorageVolumeImpl::loadInplaceVector(InputBinBuffer& in, std::vector<uint8_t>& value)
{
    std::array<uint8_t, k_inplaceSize> data{};
    in.readArray(data);
    uint8_t size = data[0];
    if(size >= k_inplaceSize)
    {
        throw std::runtime_error(fmt::format("StorageVolume:: corrupted inplace vector value, size={}", size));
    }
    value.clear();
    value.insert(value.begin(), data.begin() + 1, data.begin() + 1 + size);

}

void StorageVolumeImpl::storeValue(OutputBinBuffer& out, ValueInfo& value)
{
    size_t sizeBefore = out.remainingSpace();
    apply_visitor([this, &value, &out](const auto& valueType) {
        storeValueType(out, value, valueType);
    }, value.value);
    size_t bytesWritten = sizeBefore - out.remainingSpace();
    if(bytesWritten < k_inplaceSize)
    {
        out.fill(k_inplaceSize - bytesWritten);
    }
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, uint8_t value)
{
    out.writeU8(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, uint16_t value)
{
    out.writeU16(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, uint32_t value)
{
    out.writeU32(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, uint64_t value)
{
    out.writeU64(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, float value)
{
    out.writeFloat(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, double value)
{
    out.writeDouble(value);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, const std::string& value)
{
    size_t oldSize = info.previousSize;
    size_t newSize = value.length();
    if(oldSize != 0 && (isInplaceValueLength(oldSize) != isInplaceValueLength(newSize) ||
                        isSmallToMediumLenght(oldSize) != isSmallToMediumLenght(newSize)))
    {
        if(isInplaceValueLength(oldSize))
        {
            //do nothing
        }
        else if(isSmallToMediumLenght(oldSize))
        {
            m_stmStorage->freeSlot(info.offset, oldSize);
        }
        else
        {
            m_bigStorage->free(info.offset);
        }
        info.offset = 0;
    }
    if(isInplaceValueLength(newSize))
    {
        std::array<uint8_t, k_inplaceSize> data{};
        std::copy(value.begin(), value.end(), data.begin());
        out.writeArray(data);
        return;
    }
    if(isSmallToMediumLenght(newSize))
    {
        if(info.offset)
        {
            info.offset = m_stmStorage->overwrite(info.offset, oldSize, boost::asio::buffer(value));
        }
        else
        {
            info.offset = m_stmStorage->allocateAndWrite(boost::asio::buffer(value));
        }
    }
    else
    {
        if(info.offset)
        {
            m_bigStorage->overwrite(info.offset, boost::asio::buffer(value));
        }
        else
        {
            info.offset = m_bigStorage->allocateAndWrite(boost::asio::buffer(value));
        }
    }
    out.writeU64(value.length());
    out.writeU64(info.offset);
}

void StorageVolumeImpl::storeValueType(OutputBinBuffer& out, ValueInfo& info, const std::vector<uint8_t>& value)
{
    size_t oldSize = info.previousSize;
    size_t newSize = value.size();
    if(oldSize != 0 && (isInplaceValueLength(oldSize) != isInplaceValueLength(newSize) ||
                        isSmallToMediumLenght(oldSize) != isSmallToMediumLenght(newSize)))
    {
        if(isInplaceValueLength(oldSize))
        {
            //do nothing
        }
        else if(isSmallToMediumLenght(oldSize))
        {
            m_stmStorage->freeSlot(info.offset, oldSize);
        }
        else
        {
            m_bigStorage->free(info.offset);
        }
        info.offset = 0;
    }
    if(isInplaceValueLength(newSize))
    {
        std::array<uint8_t, k_inplaceSize> data{};
        data[0] = static_cast<uint8_t>(newSize);
        std::copy(value.begin(), value.end(), data.begin() + 1);
        out.writeArray(data);
        return;
    }
    if(isSmallToMediumLenght(newSize))
    {
        if(info.offset)
        {
            info.offset = m_stmStorage->overwrite(info.offset, oldSize, boost::asio::buffer(value));
        }
        else
        {
            info.offset = m_stmStorage->allocateAndWrite(boost::asio::buffer(value));
        }
    }
    else
    {
        if(info.offset)
        {
            m_bigStorage->overwrite(info.offset, boost::asio::buffer(value));
        }
        else
        {
            info.offset = m_bigStorage->allocateAndWrite(boost::asio::buffer(value));
        }
    }
    out.writeU64(value.size());
    out.writeU64(info.offset);
}

size_t StorageVolumeImpl::calcValueLength(ValueInfo& info)
{
    return boost::apply_visitor(ValueTypeLengthVisitor(), info.value);
}

void StorageVolumeImpl::loadValue(InputBinBuffer& in, ValueTypeIndex typeIndex, bool isInplace, ValueInfo& value)
{
    using VTI = ValueTypeIndex;
    size_t sizeBefore = in.remainingSpace();
    switch(typeIndex)
    {
        case VTI::idx_uint8_t:
            value.value = in.readU8();
            break;
        case VTI::idx_uint16_t:
            value.value = in.readU16();
            break;
        case VTI::idx_uint32_t:
            value.value = in.readU32();
            break;
        case VTI::idx_uint64_t:
            value.value = in.readU64();
            break;
        case VTI::idx_float:
            value.value = in.readFloat();
            break;
        case VTI::idx_double:
            value.value = in.readDouble();
            break;
        case VTI::idx_string:
            loadValueString(in, isInplace, value);
            break;
        case VTI::idx_vector:
            loadValueVector(in, isInplace, value);
            break;
        default:
            throw std::runtime_error(fmt::format("StorageVolume:: Corrupted file, invalid value type index:{}",
                    static_cast<uint8_t>(typeIndex)));
    }
    size_t bytesRead = sizeBefore - in.remainingSpace();
    if(bytesRead < k_inplaceSize)
    {
        in.skip(k_inplaceSize - bytesRead);
    }
}

void StorageVolumeImpl::loadValueString(InputBinBuffer& in, bool isInplace, ValueInfo& value)
{
    if(isInplace)
    {
        value.value = std::string{};
        loadInplaceString(in, boost::get<std::string&>(value.value));
        return;
    }
    auto length = static_cast<size_t>(in.readU64());
    value.value = std::string(length, ' ');
    value.previousSize = length;
    value.offset = in.readU64();
    if(isSmallToMediumLenght(length))
    {
        m_stmStorage->read(value.offset, boost::asio::buffer(boost::get<std::string&>(value.value)));
    }
    else
    {
        m_bigStorage->read(value.offset, boost::asio::buffer(boost::get<std::string&>(value.value)));
    }
}

void StorageVolumeImpl::loadValueVector(InputBinBuffer& in, bool isInplace, ValueInfo& value)
{
    if(isInplace)
    {
        value.value = std::vector<uint8_t>{};
        loadInplaceVector(in, boost::get<std::vector<uint8_t>&>(value.value));
        return;
    }
    auto length = static_cast<size_t>(in.readU64());
    value.value = std::vector<uint8_t>(length, 0);
    value.previousSize = length;
    value.offset = in.readU64();
    if(isSmallToMediumLenght(length))
    {
        m_stmStorage->read(value.offset, boost::asio::buffer(boost::get<std::vector<uint8_t>&>(value.value)));
    }
    else
    {
        m_bigStorage->read(value.offset, boost::asio::buffer(boost::get<std::vector<uint8_t>&>(value.value)));
    }
}

void StorageVolumeImpl::storeEntry(OutputBinBuffer& out, Entry& entry)
{
    uint8_t flags = 0;
    if(entry.type == EntryType::dir)
    {
        flags |= static_cast<uint8_t>(EntryFlags::dir);
    }
    if(isInplaceLength(entry.key.value.length()))
    {
        flags |= static_cast<uint8_t>(EntryFlags::inplaceKey);
    }

    {
        auto valueLength = calcValueLength(entry.value);
        if(isInplaceValueLength(valueLength))
        {
            flags |= static_cast<uint8_t>(EntryFlags::inplaceValue);
        }
    }
    flags |= static_cast<uint8_t>(boost::apply_visitor(ValueTypeIndexVisitor(), entry.value.value));
    out.writeU8(flags);
    out.writeU64(entry.expirationDateTime);
    storeKey(out, entry.key);
    storeValue(out, entry.value);
}

void StorageVolumeImpl::loadEntry(InputBinBuffer& in, Entry& entry)
{
    uint8_t flags = in.readU8();
    if(flags & static_cast<uint8_t>(EntryFlags::dir))
    {
        entry.type = EntryType::dir;
    }
    else
    {
        entry.type = EntryType::key;
    }
    entry.expirationDateTime = in.readU64();

    bool inplaceKey = (flags & static_cast<uint8_t>(EntryFlags::inplaceKey)) != 0;
    loadKey(in, inplaceKey, entry.key);
    bool inplaceValue = (flags & static_cast<uint8_t>(EntryFlags::inplaceValue)) != 0;
    auto typeIndex = static_cast<ValueTypeIndex>(flags & static_cast<uint8_t>(EntryFlags::valueTypeMask));
    loadValue(in, typeIndex, inplaceValue, entry.value);
}

void StorageVolumeImpl::loadEntryKey(InputBinBuffer& in, std::string& key)
{
    uint8_t flags = in.readU8();
    in.skip(8);//expiration date
    bool inplaceKey = (flags & static_cast<uint8_t>(EntryFlags::inplaceKey)) != 0;
    KeyInfo info;
    loadKey(in, inplaceKey, info);
    key = std::move(info.value);
    in.skip(16);//value
}

void StorageVolumeImpl::freeEntry(Entry& entry)
{
    size_t keyLength = entry.key.value.length();
    if(isInplaceLength(keyLength))
    {
        //do nothing
    }
    else if(isSmallToMediumLenght(keyLength))
    {
        m_stmStorage->freeSlot(entry.key.offset, keyLength);
    }
    else //big
    {
        m_bigStorage->free(entry.key.offset);
    }
    if(entry.type == EntryType::key)
    {
        size_t valueLength = calcValueLength(entry.value);
        if(isInplaceValueLength(valueLength))
        {
            //do nothing
        }
        else if(isSmallToMediumLenght(valueLength))
        {
            m_stmStorage->freeSlot(entry.value.offset, valueLength);
        }
        else //big
        {
            m_bigStorage->free(entry.value.offset);
        }
    }
}

StorageVolumeImpl::OffsetType StorageVolumeImpl::allocateSkipListHeadNode()
{
    if(m_firstFreeHeadListNode)
    {
        OffsetType rv = m_firstFreeHeadListNode;
        readUIntAt(*m_mainFile, m_firstFreeHeadListNode, m_firstFreeHeadListNode);
        return rv;
    }
    return m_mainFile->seekEnd();
}

StorageVolumeImpl::OffsetType StorageVolumeImpl::createSkipListHeadNode()
{
    OffsetType offset = allocateSkipListHeadNode();
    SkipListNode headNode;
    headNode.nexts.resize(k_maxListHeight, 0);
    storeHeadNode(offset, headNode);
    return offset;
}


StorageVolumeImpl::OffsetType StorageVolumeImpl::allocateSkipListNode()
{
    if(m_firstFreeListNode)
    {
        OffsetType rv = m_firstFreeListNode;
        readUIntAt(*m_mainFile, m_firstFreeListNode, m_firstFreeListNode);
        return rv;
    }
    return m_mainFile->seekEnd();
}

void StorageVolumeImpl::freeSkipListHeadNode(OffsetType offset)
{
    writeUIntAt(*m_mainFile, offset, m_firstFreeHeadListNode);
    m_firstFreeHeadListNode = offset;
}

void StorageVolumeImpl::freeSkipListNode(OffsetType offset)
{
    writeUIntAt(*m_mainFile, offset, m_firstFreeListNode);
    m_firstFreeListNode = offset;
}


void StorageVolumeImpl::storeNode(OffsetType offset, StorageVolumeImpl::SkipListNode& node)
{
    std::array<uint8_t, SkipListNode::binSize()> data{};
    auto buf = boost::asio::buffer(data);
    OutputBinBuffer out(buf);
    storeNode(out, node);
    m_mainFile->seek(offset);
    m_mainFile->write(buf);
}

void StorageVolumeImpl::storeNode(OutputBinBuffer& out, SkipListNode& node)
{
    out.writeU8(static_cast<uint8_t>(node.nexts.size()));
    node.nextOffset = storeNodeNexts(out, node.nextOffset, node.nexts);
    out.writeU8(static_cast<uint8_t>(node.entries.size()));
    for(auto& entry:node.entries)
    {
        storeEntry(out, entry);
    }
    if(node.entries.size() < k_entriesPerNode)
    {
        out.fill((k_entriesPerNode - node.entries.size()) * Entry::binSize());
    }
}

void StorageVolumeImpl::store(boost::string_view keyPath, const StorageVolumeImpl::ValueType& value)
{
    store(keyPath, value, 0);
}

void StorageVolumeImpl::storeExpiring(boost::string_view keyPath, const ValueType& value, TimePoint expTime)
{
    store(keyPath, value, std::chrono::duration_cast<std::chrono::milliseconds>(expTime.time_since_epoch()).count());
}

void StorageVolumeImpl::store(boost::string_view keyPath, const StorageVolumeImpl::ValueType& value, uint64_t expTime)
{
    auto path = splitKeyPath(keyPath);
    if(path.empty() || path.back().length() == 0)
    {
        throw std::runtime_error("StorageVolume::store:Key or key path cannot be empty.");
    }
    auto key = path.back();
    path.pop_back();
    OffsetType offset = k_rootListOffset;
    if(m_lastDirHeadOffset != 0 && keyPath.compare(0, keyPath.length() - key.length(), m_lastDir) == 0)
    {
        offset = m_lastDirHeadOffset;
    }
    else
    {
        for(auto& dir:path)
        {
            Entry entry;
            if(!listLookup(offset, dir, entry))
            {
                OffsetType newDirOffset = createSkipListHeadNode();
                entry.setDir(std::string(dir.data(), dir.length()), newDirOffset);
                listInsert(offset, std::move(entry));
                offset = newDirOffset;
            }
            else
            {
                if(entry.type != EntryType::dir)
                {
                    throw std::runtime_error(
                            fmt::format("StorageVolume::store: path entry {} is not a dir.", dir));
                }
                offset = boost::get<uint64_t>(entry.value.value);
            }
        }
        m_lastDir.assign(keyPath.data(), 0, keyPath.length() - key.length());
        m_lastDirHeadOffset = offset;
    }
    Entry keyEntry;
    keyEntry.setValue(std::string(key.data(), key.length()), value);
    keyEntry.expirationDateTime = expTime;
    listInsert(offset, std::move(keyEntry));
}

boost::optional<StorageVolumeImpl::ValueType> StorageVolumeImpl::lookup(boost::string_view keyPath)
{
    auto path = splitKeyPath(keyPath);
    if(path.empty() || path.back().length() == 0)
    {
        return {};
    }
    auto key = path.back();
    path.pop_back();
    OffsetType offset;
    if(m_lastDirHeadOffset != 0 && keyPath.compare(0, keyPath.length() - key.length(), m_lastDir) == 0)
    {
        offset = m_lastDirHeadOffset;
    }
    else
    {
        offset = followPath(path);
        if(!offset)
        {
            return {};
        }
        m_lastDir.assign(keyPath.data(), 0, keyPath.length() - key.length());
        m_lastDirHeadOffset = offset;
    }
    Entry keyEntry;
    if(!listLookup(offset, key, keyEntry))
    {
        return {};
    }
    if(keyEntry.expirationDateTime != 0 && keyEntry.expirationDateTime < nowInMilliseconds())
    {
        return {};
    }
    return {keyEntry.value.value};
}

void StorageVolumeImpl::eraseKey(boost::string_view keyPath)
{
    auto path = splitKeyPath(keyPath);
    if(path.empty() || path.back().length() == 0)
    {
        return;
    }
    auto key = path.back();
    path.pop_back();
    OffsetType offset = followPath(path);
    if(!offset)
    {
        return;
    }
    listErase(offset, EntryType::key, key);
}

void StorageVolumeImpl::eraseDirRecursive(boost::string_view dirPath)
{
    auto path = splitKeyPath(dirPath);
    if(path.empty() || path.back().length() == 0)
    {
        return;
    }
    m_lastDir.clear();
    m_lastDirHeadOffset = 0;
    auto dir = path.back();
    path.pop_back();
    OffsetType offset = followPath(path);
    if(!offset)
    {
        return;
    }
    Entry entry;
    if(!listLookup(offset, dir, entry))
    {
        return;
    }
    listEraseRecursive(boost::get<uint64_t>(entry.value.value));
    listErase(offset, EntryType::dir, dir);
}

boost::optional<std::vector<StorageVolumeImpl::DirEntry>> StorageVolumeImpl::getDirEntries(boost::string_view dirPath)
{
    auto path = splitKeyPath(dirPath);
    OffsetType offset = followPath(path);
    if(!offset)
    {
        return {};
    }
    std::vector<DirEntry> rv;
    listGetContent(offset, rv);
    return {std::move(rv)};
}

void StorageVolumeImpl::loadNode(OffsetType offset, SkipListNode& node)
{
    std::array<uint8_t, SkipListNode::binSize()> data{};
    m_mainFile->seek(offset);
    auto buf = boost::asio::buffer(data);
    m_mainFile->read(buf);
    InputBinBuffer in(buf);
    loadNode(in, node);
}

void StorageVolumeImpl::loadHeadNode(OffsetType offset, SkipListNode& node)
{
    std::array<uint8_t, SkipListNode::binHeadSize()> data{};
    m_mainFile->seek(offset);
    auto buf = boost::asio::buffer(data);
    m_mainFile->read(buf);
    InputBinBuffer in(buf);
    loadHeadNode(in, node);
}

void StorageVolumeImpl::storeHeadNode(OffsetType offset, SkipListNode& node)
{
    std::array<uint8_t, SkipListNode::binHeadSize()> data{};
    auto buf = boost::asio::buffer(data);
    OutputBinBuffer out(buf);
    storeHeadNode(out, node);
    m_mainFile->seek(offset);
    m_mainFile->write(buf);
}

void StorageVolumeImpl::loadNode(InputBinBuffer& in, SkipListNode& node)
{
    loadHeadNode(in, node);
    uint8_t entries = in.readU8();
    node.entries.resize(entries);
    for(uint8_t i = 0; i < entries; ++i)
    {
        loadEntry(in, node.entries[i]);
    }
    in.skip((k_entriesPerNode - entries) * Entry::binSize());
}

void StorageVolumeImpl::loadHeadNode(InputBinBuffer& in, SkipListNode& node)
{
    uint8_t nextsCount = in.readU8();
    node.nextOffset = loadNodeNexts(in, nextsCount, node.nexts);
}

void StorageVolumeImpl::storeHeadNode(OutputBinBuffer& out, SkipListNode& node)
{
    out.writeU8(static_cast<uint8_t>(node.nexts.size()));
    node.nextOffset = storeNodeNexts(out, node.nextOffset, node.nexts);
}

StorageVolumeImpl::OffsetType
StorageVolumeImpl::loadNodeNexts(InputBinBuffer& in, uint8_t nextsCount, NextsVector& nexts)
{
    nexts.resize(nextsCount);
    if(nextsCount == 1)
    {
        nexts[0] = in.readU64();
        return 0;
    }

    OffsetType nextsOffset = in.readU64();
    std::array<OffsetType, k_maxListHeight> data{};
    auto buf = boost::asio::buffer(data.data(), nextsCount * sizeof(OffsetType));
    m_stmStorage->read(nextsOffset, buf);
    InputBinBuffer offIn(buf);
    for(size_t i = 0; i < nextsCount; ++i)
    {
        nexts[i] = offIn.readU64();
    }
    return nextsOffset;
}

StorageVolumeImpl::OffsetType
StorageVolumeImpl::storeNodeNexts(OutputBinBuffer& out, OffsetType offset, const NextsVector& nexts)
{
    if(nexts.size() == 1)
    {
        out.writeU64(nexts[0]);
    }
    else
    {
        std::array<OffsetType, k_maxListHeight> data{};
        auto buf = boost::asio::buffer(data);
        OutputBinBuffer nextOut(buf);
        size_t startSize = nextOut.remainingSpace();
        for(auto nextOffset:nexts)
        {
            nextOut.writeU64(nextOffset);
        }
        size_t bytesWritten = startSize - nextOut.remainingSpace();
        buf = boost::asio::buffer(buf, bytesWritten);
        if(offset)
        {
            offset = m_stmStorage->overwrite(offset, bytesWritten, buf);
        }
        else
        {
            offset = m_stmStorage->allocateAndWrite(buf);
        }
        out.writeU64(offset);
    }
    return offset;
}

void
StorageVolumeImpl::loadNodeNextsAndEdgeKey(OffsetType offset, NextsVector& nexts, EdgeKey whichKey, std::string& key)
{
    std::array<uint8_t, SkipListNode::binSize()> data{};
    m_mainFile->seek(offset);
    auto buf = boost::asio::buffer(data);
    m_mainFile->read(buf);
    InputBinBuffer in(buf);
    uint8_t nextsCount = in.readU8();
    loadNodeNexts(in, nextsCount, nexts);
    uint8_t entries = in.readU8();
    if(whichKey == EdgeKey::first)
    {
        loadEntryKey(in, key);
        in.skip((k_entriesPerNode - 1) * Entry::binSize());
    }
    else if(whichKey == EdgeKey::last)
    {
        in.skip((entries - 1) * Entry::binSize());
        loadEntryKey(in, key);
        in.skip((k_entriesPerNode - entries) * Entry::binSize());
    }
    else // none
    {
        in.skip(k_entriesPerNode * Entry::binSize());
    }
}

void StorageVolumeImpl::findPath(OffsetType headOffset, ListPath& path, const boost::string_view& key)
{
    NextsVector currentNexts;
    {
        SkipListNode headNode;
        loadHeadNode(headOffset, headNode);
        currentNexts = headNode.nexts;
    }
    OffsetType offset = headOffset;

    NextsVector nextNexts;
    std::string nextLastKey;
    for(size_t level = currentNexts.size(); level-- > 0;)
    {
        while(currentNexts[level] != 0)
        {
            loadNodeNextsAndEdgeKey(currentNexts[level], nextNexts, EdgeKey::last, nextLastKey);
            if(key > nextLastKey)
            {
                offset = currentNexts[level];
                currentNexts = nextNexts;
            }
            else
            {
                break;
            }
        }
        path[level] = offset;
    }
}

size_t StorageVolumeImpl::generateNewLevel()
{
    uint32_t value = m_random();
    size_t newLevel = 1;
    while((value & 1) && newLevel < k_maxListHeight)
    {
        ++newLevel;
        value >>= 1;
    }
    return newLevel;
}

void StorageVolumeImpl::listInsert(OffsetType headOffset, Entry&& entry)
{
    ListPath path;
    findPath(headOffset, path, entry.key.value);

    SkipListNode node;
    OffsetType nodeOffset = path[0];
    loadHeadNode(nodeOffset, node);

    nodeOffset = node.nexts[0];
    if(!nodeOffset && path[0] != headOffset)
    {
        nodeOffset = path[0];
    }

    if(!nodeOffset)//empty list
    {
        SkipListNode newNode;
        OffsetType newNodeOffset = allocateSkipListNode();
        size_t newLevel = generateNewLevel();
        newNode.nexts.resize(newLevel);
        for(size_t i = 0; i < newLevel; ++i)
        {
            loadHeadNode(path[i], node);
            newNode.nexts[i] = node.nexts[i];
            node.nexts[i] = newNodeOffset;
            storeHeadNode(path[i], node);
        }
        newNode.entries.push_back(entry);
        storeNode(newNodeOffset, newNode);
        return;
    }

    loadNode(nodeOffset, node);

    getLogger()->debug("inserting {} into {} ... {} @ {}", entry.key.value,
            node.entries.front().key.value,
            node.entries.back().key.value, nodeOffset);


    auto it = std::lower_bound(node.entries.begin(), node.entries.end(), entry, EntryKeyComparator{});
    if(it != node.entries.end() && it->key.value == entry.key.value)
    {
        if(it->type != entry.type)
        {
            throw std::runtime_error(
                    fmt::format(
                            "StorageVolume::store: entry type cannot be changed (was {}, trying to overwrite with {}",
                            it->type == EntryType::dir ? "dir" : "key", entry.type == EntryType::dir ? "dir" : "key"));
        }
        it->key = std::move(entry.key);
        it->value.previousSize = calcValueLength(it->value);
        it->value.value = std::move(entry.value.value);
        storeNode(nodeOffset, node);
        return;
    }
    if(node.entries.size() < k_entriesPerNode)
    {
        node.entries.insert(it, std::move(entry));
        storeNode(nodeOffset, node);
        return;
    }
    SkipListNode newNode;
    OffsetType newNodeOffset = allocateSkipListNode();
    getLogger()->debug("newNodeOffset={}", newNodeOffset);

    if(it != node.entries.end())
    {
        auto middle = node.entries.begin() + node.entries.size() / 2;

        bool isInNewNode = it >= middle;
        size_t indexInNewNode = 0;
        if(isInNewNode)
        {
            indexInNewNode = (it - node.entries.begin()) - node.entries.size() / 2;
        }

        std::move(middle, node.entries.end(), std::back_inserter(newNode.entries));
        node.entries.erase(middle, node.entries.end());

        if(isInNewNode)
        {
            it = newNode.entries.begin() + indexInNewNode;
            newNode.entries.insert(it, std::move(entry));
        }
        else
        {
            node.entries.insert(it, std::move(entry));
        }
    }
    else
    {
        newNode.entries.push_back(std::move(entry));
    }

    size_t newLevel = generateNewLevel();
    newNode.nexts.resize(newLevel);
    SkipListNode tempNode;
    for(size_t i = 0; i < newLevel; ++i)
    {
        loadHeadNode(path[i], tempNode);
        if(tempNode.nexts[i] == nodeOffset || path[i] == nodeOffset)
        {
            newNode.nexts[i] = node.nexts[i];
            node.nexts[i] = newNodeOffset;
        }
        else
        {
            newNode.nexts[i] = tempNode.nexts[i];
            tempNode.nexts[i] = newNodeOffset;
            storeHeadNode(path[i], tempNode);
        }
//        if(tempNode.nexts[i] == nodeOffset)
//        {
//            newNode.nexts[i] = node.nexts[i];
//            node.nexts[i] = newNodeOffset;
//        }
//        else
//        {
//            if(path[i] == nodeOffset)
//            {
//                newNode.nexts[i] = node.nexts[i];
//                node.nexts[i] = newNodeOffset;
//            }
//            else
//            {
//                newNode.nexts[i] = tempNode.nexts[i];
//                tempNode.nexts[i] = newNodeOffset;
//                storeHeadNode(path[i], tempNode);
//            }
//        }
    }
    storeNode(nodeOffset, node);
    storeNode(newNodeOffset, newNode);
}

bool StorageVolumeImpl::listLookup(OffsetType headOffset, const boost::string_view& key, Entry& entry)
{
    SkipListNode node;
    loadHeadNode(headOffset, node);
    NextsVector currentNexts = std::move(node.nexts);
    NextsVector nextNexts;
    std::string nextFirstKey;
    OffsetType nodeOffset = currentNexts[0];
    for(size_t level = currentNexts.size(); level-- > 0;)
    {
        while(currentNexts[level])
        {
            loadNodeNextsAndEdgeKey(currentNexts[level], nextNexts, EdgeKey::first, nextFirstKey);
            if(key >= nextFirstKey)
            {
                nodeOffset = currentNexts[level];
                currentNexts = nextNexts;
            }
            else
            {
                break;
            }
        }
    }
    if(!nodeOffset)
    {
        return false;
    }
    loadNode(nodeOffset, node);
    auto it = std::lower_bound(node.entries.begin(), node.entries.end(), key, EntryKeyComparator{});
    if(it == node.entries.end() || it->key.value != key)
    {
        return false;
    }
    entry = std::move(*it);
    return true;
}

void StorageVolumeImpl::listErase(OffsetType headOffset, EntryType type, const boost::string_view& key)
{
    SkipListNode node;
    ListPath path;
    findPath(headOffset, path, key);
    loadHeadNode(path[0], node);
    OffsetType nodeOffset = node.nexts[0];
    if(!nodeOffset)
    {
        return;
    }
    loadNode(nodeOffset, node);
    auto it = std::lower_bound(node.entries.begin(), node.entries.end(), key, EntryKeyComparator());
    if(it == node.entries.end() || it->key.value != key)
    {
        return;
    }
    if(it->type != type)
    {
        throw std::runtime_error(fmt::format("StorageVolume::erase attempt to erase key {} of invalid type", key));
    }
    freeEntry(*it);
    node.entries.erase(it);
    if(node.entries.empty())
    {
        SkipListNode tmpNode;
        for(size_t i = 0; i < node.nexts.size(); ++i)
        {
            loadHeadNode(path[i], tmpNode);
            tmpNode.nexts[i] = node.nexts[i];
            storeHeadNode(path[i], tmpNode);
        }
        if(node.nexts.size() > 1)
        {
            m_stmStorage->freeSlot(node.nextOffset, node.nexts.size() * sizeof(OffsetType));
        }
        freeSkipListNode(nodeOffset);
    }
    else
    {
        storeNode(nodeOffset, node);
    }
}

void StorageVolumeImpl::listEraseRecursive(OffsetType nodeHeadOffset)
{
    SkipListNode node;
    loadHeadNode(nodeHeadOffset, node);
    OffsetType offset = node.nexts[0];
    m_stmStorage->freeSlot(node.nextOffset, node.nexts.size() * sizeof(OffsetType));
    while(offset)
    {
        loadNode(offset, node);
        for(auto& entry:node.entries)
        {
            if(entry.type == EntryType::dir)
            {
                listEraseRecursive(boost::get<uint64_t>(entry.value.value));
                listErase(offset, EntryType::dir, entry.key.value);
            }
            freeEntry(entry);
        }
        freeSkipListHeadNode(offset);
        if(node.nexts.size() > 1)
        {
            m_stmStorage->freeSlot(node.nextOffset, node.nexts.size() * sizeof(OffsetType));
        }
        offset = node.nexts[0];
    }
    freeSkipListHeadNode(nodeHeadOffset);
}

void StorageVolumeImpl::listGetContent(OffsetType nodeHeadOffset, std::vector<DirEntry>& entries)
{
    SkipListNode node;
    loadHeadNode(nodeHeadOffset, node);
    OffsetType offset = node.nexts[0];
    auto now = nowInMilliseconds();
    while(offset)
    {
        loadNode(offset, node);
        for(auto& entry:node.entries)
        {
            if(entry.expirationDateTime != 0 && entry.expirationDateTime < now)
            {
                continue;
            }
            entries.push_back({entry.type, entry.key.value});
        }
        offset = node.nexts[0];
    }
}

StorageVolumeImpl::OffsetType StorageVolumeImpl::followPath(const std::vector<boost::string_view>& path)
{
    OffsetType offset = k_rootListOffset;
    for(auto& dir:path)
    {
        Entry entry;
        if(!listLookup(offset, dir, entry))
        {
            return 0;
        }
        else
        {
            if(entry.type != EntryType::dir)
            {
                throw std::runtime_error(
                        fmt::format("StorageVolume:: entry '{}' is not a dir.", dir));
            }
            offset = boost::get<uint64_t>(entry.value.value);
        }
    }
    return offset;
}

void StorageVolumeImpl::dump(const std::function<void(const std::string&)>& out)
{
    dumpList(k_rootListOffset, 0, out);
}

void
StorageVolumeImpl::dumpList(OffsetType headOffset, size_t indent, const std::function<void(const std::string&)>& out)
{
    SkipListNode node;
    loadHeadNode(headOffset, node);
    OffsetType offset = node.nexts[0];
    while(offset)
    {
        loadNode(offset, node);
        out(fmt::format("node@{}, height:{}:[\n", offset, node.nexts.size()));
        for(auto& e:node.entries)
        {
            if(e.type == EntryType::key)
            {
                out(fmt::format("{:>{}}'{}':{},\n", "", indent, e.key.value, ""));
            }
            else
            {
                out(fmt::format("{:>{}}'{}':{{,\n", "", indent, e.key.value));
                dumpList(boost::get<uint64_t>(e.value.value), indent + 2, out);
                out(fmt::format("{:>{}}}},\n", "", indent));
            }
        }
        out("]\n");
        offset = node.nexts[0];
    }
}

}

StorageVolume::UniquePtr StorageVolume::open(FileSystem::UniqueFilePtr&& mainFile,
                                             std::unique_ptr<SmallToMediumFileStorage>&& stmFileStorage,
                                             std::unique_ptr<BigFileStorage>&& bigFileStorage)
{
    auto rv = std::make_unique<StorageVolumeImpl>(std::move(mainFile), std::move(stmFileStorage),
            std::move(bigFileStorage));

    rv->openImpl();

    return rv;
}

std::unique_ptr<StorageVolume>
StorageVolume::create(FileSystem::UniqueFilePtr&& mainFile,
                      std::unique_ptr<SmallToMediumFileStorage>&& stmFileStorage,
                      std::unique_ptr<BigFileStorage>&& bigFileStorage)
{
    auto rv = std::make_unique<StorageVolumeImpl>(std::move(mainFile), std::move(stmFileStorage),
            std::move(bigFileStorage));

    rv->createImpl();

    return rv;
}

void StorageVolume::initFileLogger(const boost::filesystem::path& filePath, size_t maxSize, size_t maxFiles)
{
    if(!spdlog::get(s_loggingCategory))
    {
        auto log = spdlog::default_factory::create<spdlog::sinks::rotating_file_sink_mt>(s_loggingCategory,
                filePath.string(),
                maxSize, maxFiles);
        log->set_level(spdlog::level::debug);
    }
}

void StorageVolume::initStdoutLogger()
{
    if(!spdlog::get(s_loggingCategory))
    {
        auto log = spdlog::stdout_color_mt(s_loggingCategory);
        log->set_level(spdlog::level::debug);
    }
}

}
