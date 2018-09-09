#pragma once

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>

class FilesCleanupFixture : public ::testing::Test{
public:
    ~FilesCleanupFixture() override
    {
        for(auto& filename : m_files)
        {
            boost::system::error_code ec;
            boost::filesystem::remove(filename, ec);
        }
    }

    void addToCleanup(const boost::filesystem::path& filename)
    {
        m_files.push_back(filename);
    }
private:
    std::vector<boost::filesystem::path> m_files;
};