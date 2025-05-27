#pragma once
//---------------------------------------------------------------------------
#include <vector>
#include "infra/SmallVec.hpp"
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
class UnionFind {
    struct Entry {
        unsigned parent = 0;
        unsigned rank = 0;
    };
    /// The entries
    SmallVec<Entry, 16> entries;

    public:
    unsigned find(unsigned v) {
        if (v >= entries.size())
            return v;
        while (entries[v].parent != v) {
            entries[v].parent = entries[entries[v].parent].parent;
            v = entries[v].parent;
        }
        return v;
    }
    unsigned merge(unsigned a, unsigned b) {
        a = find(a);
        b = find(b);
        if (a == b)
            return a;

        // Grow
        while (std::max(a, b) >= entries.size())
            entries.push_back({unsigned(entries.size()), 1});

        if (entries[a].rank > entries[b].rank)
            std::swap(a, b);

        entries[a].parent = b;
        entries[b].rank += entries[a].rank;
        return b;
    }
};
//---------------------------------------------------------------------------
}