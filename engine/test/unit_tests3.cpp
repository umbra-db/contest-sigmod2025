#include "infra/SmallVec.hpp"
#include "infra/helper/BitOps.hpp"
#include "plan.h"
#include "storage/BitLogic.hpp"
#include <algorithm>
#include <array>
#include <catch2/catch_test_macros.hpp>

using namespace std;
using namespace engine;

void testBits(uint64_t bits) {
    array<uint8_t, 71> offsets{};
    auto sz = BitLogic::bitsToOffsets(offsets.data(), bits) - offsets.data();
    REQUIRE(sz == engine::popcount(bits));
    uint64_t cur = bits;
    for (int i = 0; i < sz; i++) {
        REQUIRE(offsets[i] == countr_zero(cur));
        cur &= cur - 1;
    }
    REQUIRE(cur == 0);
}

TEST_CASE("BitLogic") {
    array<uint8_t, 71> bits{};

    testBits(5);
    testBits(0x8000000000000000);
    testBits(0x8000000000000001);
    testBits(0x19c0003040502);
}

TEST_CASE("SmallVec") {
    SmallVec<uint8_t> smallVec{};
    smallVec.push_back(3);
    auto x = smallVec.emplace_back(2);
    REQUIRE(x == 2);
    REQUIRE(smallVec[0] == 3);
    REQUIRE(smallVec.size() == 2);

    for (auto i = 0; i < 10; i++) {
        smallVec.push_back(i);
        REQUIRE(smallVec[i + 2] == i);
        REQUIRE(smallVec.size() == 3 + i);
    }

    for (auto i = 0; i < 12; i++)
        smallVec.pop_back();

    REQUIRE(smallVec.size() == 0);

    std::vector<uint8_t> stdVec;
    for (auto i : {7, 3, 1, 6, 2, 9, 4, 8, 2, 3, 10, 6}) {
        smallVec.push_back(i);
        stdVec.push_back(i);
        REQUIRE(smallVec.back() == i);
    }

    // Can we sort?
    std::sort(smallVec.begin(), smallVec.end());
    REQUIRE(std::is_sorted(smallVec.begin(), smallVec.end()));

    // Sort the standard vector and compare
    std::sort(stdVec.begin(), stdVec.end());
    for (auto i = 0; i < stdVec.size(); i++)
        REQUIRE(smallVec[i] == stdVec[i]);

    stdVec.erase(stdVec.begin(), stdVec.end());

    smallVec.reserve(512);
    REQUIRE(smallVec.capacity() >= 512);

    smallVec.erase(smallVec.begin(), smallVec.end());
    REQUIRE(smallVec.empty());
}
