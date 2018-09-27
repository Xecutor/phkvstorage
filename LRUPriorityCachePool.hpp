#pragma once

#include <functional>
#include <deque>
#include <array>
#include <stdint.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>

namespace phkvs {

template<class V, uint8_t MXP>
class LRUPriorityCachePool {
public:
    struct PoolNode {
        boost::intrusive::list_member_hook<> listHook;
        V value;
        uint8_t prio;
    };

    LRUPriorityCachePool(size_t maxItems, std::function<void(PoolNode*)> reuseNotify) :
            m_maxItems(maxItems), m_reuseNotify(reuseNotify)
    {
    }

    LRUPriorityCachePool(const LRUPriorityCachePool&) = delete;

    PoolNode* allocate(uint8_t prio)
    {
        if(prio >= MXP)
        {
            return nullptr;
        }
        if(!m_freeItems.empty())
        {
            auto rv = m_freeItems.front();
            m_freeItems.pop_front();
            return rv;
        }
        if(m_mainPool.size() < m_maxItems)
        {
            m_mainPool.emplace_back();
            return &m_mainPool.back();
        }
        for(uint8_t idx = MXP; idx-- > 0;)
        {
            if(!m_prioLists[idx].empty())
            {
                auto rv = m_prioLists[idx].front();
                m_reuseNotify(rv);
                m_prioLists[idx].pop_front();
                rv->prio = prio;
                m_prioLists[prio].push_back(rv);
                return rv;
            }
        }
        return nullptr;
    }

    void touch(PoolNode* node)
    {
        m_prioLists[node->prio].erase(node);
        m_prioLists[node->prio].push_back(node);
    }

    void free(PoolNode* node)
    {
        m_prioLists[node->prio].erase(node);
        m_freeItems.push_back(node);
    }

private:

    using PoolNodeHookOption = boost::intrusive::member_hook<PoolNode, boost::intrusive::list_member_hook<>, &PoolNode::listHook>;
    using PoolList = boost::intrusive::list<PoolNode, PoolNodeHookOption>;
    std::array<PoolList, MXP> m_prioLists;
    size_t m_maxItems;
    std::function<void(PoolNode*)> m_reuseNotify;
    PoolList m_freeItems;
    std::deque<PoolNode> m_mainPool;
};

}
