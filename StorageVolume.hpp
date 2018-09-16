#pragma once

#include <string>
#include <random>

#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>

#include "IRandomAccessFile.hpp"
#include "FileSystem.hpp"
#include "SmallToMediumFileStorage.hpp"
#include "test_bigfilestorage.hpp"


namespace phkvs{

class StorageVolume{
public:
    using ValueType = boost::variant<uint8_t, uint16_t, uint32_t, uint64_t,
        float, double, std::string, std::vector<uint8_t>>;

    static std::unique_ptr<StorageVolume> open(FileSystem::UniqueFilePtr&& mainFile,
                                               FileSystem::UniqueFilePtr&& stmFile,
                                               FileSystem::UniqueFilePtr&& bigFile);
    static std::unique_ptr<StorageVolume> create(FileSystem::UniqueFilePtr&& mainFile,
                                                 FileSystem::UniqueFilePtr&& stmFile,
                                                 FileSystem::UniqueFilePtr&& bigFile);

    void store(const std::string& keyPath, const ValueType& value);
    boost::optional<ValueType> lookup(const std::string& keyPath);

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
    static constexpr size_t k_listBlockSize = 16;

    void openImpl();
    void createImpl();

    enum class EntryType {
        key,
        dir
    };

    struct KeyInfo{
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

    struct ValueInfo{
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

    enum class ValueTypeIndex{
        idx_uint8_t = 0,
        idx_uint16_t = 1,
        idx_uint32_t = 2,
        idx_uint64_t = 3,
        idx_float = 4,
        idx_double = 5,
        idx_string = 6,
        idx_vector = 7
    };

    struct ValueTypeIndexVisitor{
        constexpr ValueTypeIndex operator()(uint8_t)const
        {
            return ValueTypeIndex::idx_uint8_t;
        }
        constexpr ValueTypeIndex operator()(uint16_t)const
        {
            return ValueTypeIndex::idx_uint16_t;
        }
        constexpr ValueTypeIndex operator()(uint32_t)const
        {
            return ValueTypeIndex::idx_uint32_t;
        }
        constexpr ValueTypeIndex operator()(uint64_t)const
        {
            return ValueTypeIndex::idx_uint64_t;
        }
        constexpr ValueTypeIndex operator()(float)const
        {
            return ValueTypeIndex::idx_float;
        }
        constexpr ValueTypeIndex operator()(double)const
        {
            return ValueTypeIndex::idx_double;
        }
        constexpr ValueTypeIndex operator()(const std::string&)const
        {
            return ValueTypeIndex::idx_string;
        }
        constexpr ValueTypeIndex operator()(const std::vector<uint8_t>&)const
        {
            return ValueTypeIndex::idx_vector;
        }
    };
    struct ValueTypeLengthVisitor {
        template <class T>
        size_t operator()(T)const
        {
            return sizeof(T);
        }
        size_t operator()(const std::string& value)const
        {
            return value.length();
        }
        size_t operator()(const std::vector<uint8_t>& value)
        {
            return value.size();
        }
    };

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

    enum class EntryFlags : uint8_t{
        dir = 0x80,
        inplaceKey = 0x40,
        inplaceValue = 0x20,
        valueTypeMask = 0x0f
    };

    struct Entry{
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

    struct EntryKeyComparator{
        bool operator()(const Entry& left, const Entry& right)const
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

    using EntriesVector = std::vector<Entry>;
    using NextsVector = std::vector<OffsetType>;

    struct SkipListNode{
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
    enum class EdgeKey{
        none,
        first,
        last
    };
    void loadNodeNextsAndEdgeKey(OffsetType offset, NextsVector& nexts, EdgeKey whichKey, std::string& key);

    using ListPath = std::array<OffsetType, k_listBlockSize>;

    void findPath(OffsetType headOffset, ListPath& path, const boost::string_view& key);

    size_t generateNewLevel();

    void listInsert(OffsetType headOffset, Entry&& entry);
    bool listLookup(OffsetType headOffset, const boost::string_view& key, Entry& entry);

    FileSystem::UniqueFilePtr m_mainFile;
    OffsetType m_firstFreeListNode = 0;
    OffsetType m_firstFreeHeadListNode = 0;
    std::unique_ptr<SmallToMediumFileStorage> m_stmStorage;
    std::unique_ptr<BigFileStorage> m_bigStorage;

    std::mt19937 m_random;


    struct PrivateKey;
public:
    StorageVolume(PrivateKey&, FileSystem::UniqueFilePtr&& mainFile):m_mainFile(std::move(mainFile)){}
};

}
