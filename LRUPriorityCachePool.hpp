#pragma once

#include <functional>
#include <deque>
#include <array>
#include <stdint.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>

namespace phkvs {

template<class V,
        boost::intrusive::list_member_hook<> (V::*listNodePtr),
        uint8_t (V::*prioPtr),
        uint8_t MXP>
class LRUPriorityCachePool {
public:
//    struct PoolNode {
//        boost::intrusive::list_member_hook<> listHook;
//        uint8_t prio;
//    };

    LRUPriorityCachePool(size_t maxItems, std::function<void(V*)> reuseNotify) :
            m_maxItems(maxItems), m_reuseNotify(reuseNotify)
    {
    }

    ~LRUPriorityCachePool()
    {
        m_freeItems.clear();
        for(size_t i=0;i<MXP;++i)
        {
            m_prioLists[i].clear();
        }
    }

    LRUPriorityCachePool(const LRUPriorityCachePool&) = delete;

    V* allocate(uint8_t prio)
    {
        if(prio >= MXP)
        {
            return nullptr;
        }
        if(!m_freeItems.empty())
        {
            auto& rv = m_freeItems.front();
            m_freeItems.pop_front();
            m_prioLists[prio].push_back(rv);
            return &rv;
        }
        if(m_mainPool.size() < m_maxItems)
        {
            m_mainPool.emplace_back();
            m_prioLists[prio].push_back(m_mainPool.back());
            return &m_mainPool.back();
        }
        for(uint8_t idx = MXP; idx-- > 0;)
        {
            if(!m_prioLists[idx].empty())
            {
                auto& rv = m_prioLists[idx].front();
                m_reuseNotify(&rv);
                m_prioLists[idx].pop_front();
                rv.*prioPtr = prio;
                m_prioLists[prio].push_back(rv);
                return &rv;
            }
        }
        return nullptr;
    }

    void touch(V* node)
    {
        uint8_t prio = node->*prioPtr;
        m_prioLists[prio].erase(m_prioLists[prio].iterator_to(*node));
        m_prioLists[prio].push_back(*node);
    }

    void free(V* node)
    {
        uint8_t prio = node->*prioPtr;
        m_prioLists[prio].erase(m_prioLists[prio].iterator_to(*node));
        m_freeItems.push_back(node);
    }

private:

    using PoolNodeHookOption = boost::intrusive::member_hook<V, boost::intrusive::list_member_hook<>, listNodePtr>;
    using PoolList = boost::intrusive::list<V, PoolNodeHookOption>;
    std::array<PoolList, MXP> m_prioLists;
    size_t m_maxItems;
    std::function<void(V*)> m_reuseNotify;
    PoolList m_freeItems;
    std::deque<V> m_mainPool;
};

}
