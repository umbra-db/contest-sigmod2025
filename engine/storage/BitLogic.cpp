#include "storage/BitLogic.hpp"
#include "infra/Util.hpp"
#include "infra/helper/BitOps.hpp"
#include <array>
#include <cassert>
#include <iostream>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
static constexpr size_t precomputeBits = 8;
static constexpr size_t precomputeCount = 1ull << precomputeBits;
static constexpr uint64_t precomputeMask = (1ull << precomputeBits) - 1;
//---------------------------------------------------------------------------
//std::array<uint16_t, precomputeCount + 1> computeOffsets() noexcept {
//    std::array<uint16_t, precomputeCount + 1> result{};
//    result[0] = 0;
//    for (size_t i = 0; i < precomputeCount; ++i) {
//        result[i + 1] = popcount(i) + result[i];
//    }
//    assert(result.back() == 8 * precomputeCount / 2);
//    std::cout << "std::array<uint16_t, precomputeCount + 1> offsets = {";
//    for (auto v : result) {
//        std::cout << v << ", ";
//    }
//    std::cout << "}" << std::endl;
//    return result;
//}
//---------------------------------------------------------------------------
constexpr std::array<uint16_t, precomputeCount + 1> offsets{0, 0, 1, 2, 4, 5, 7, 9, 12, 13, 15, 17, 20, 22, 25, 28, 32, 33, 35, 37, 40, 42, 45, 48, 52, 54, 57, 60, 64, 67, 71, 75, 80, 81, 83, 85, 88, 90, 93, 96, 100, 102, 105, 108, 112, 115, 119, 123, 128, 130, 133, 136, 140, 143, 147, 151, 156, 159, 163, 167, 172, 176, 181, 186, 192, 193, 195, 197, 200, 202, 205, 208, 212, 214, 217, 220, 224, 227, 231, 235, 240, 242, 245, 248, 252, 255, 259, 263, 268, 271, 275, 279, 284, 288, 293, 298, 304, 306, 309, 312, 316, 319, 323, 327, 332, 335, 339, 343, 348, 352, 357, 362, 368, 371, 375, 379, 384, 388, 393, 398, 404, 408, 413, 418, 424, 429, 435, 441, 448, 449, 451, 453, 456, 458, 461, 464, 468, 470, 473, 476, 480, 483, 487, 491, 496, 498, 501, 504, 508, 511, 515, 519, 524, 527, 531, 535, 540, 544, 549, 554, 560, 562, 565, 568, 572, 575, 579, 583, 588, 591, 595, 599, 604, 608, 613, 618, 624, 627, 631, 635, 640, 644, 649, 654, 660, 664, 669, 674, 680, 685, 691, 697, 704, 706, 709, 712, 716, 719, 723, 727, 732, 735, 739, 743, 748, 752, 757, 762, 768, 771, 775, 779, 784, 788, 793, 798, 804, 808, 813, 818, 824, 829, 835, 841, 848, 851, 855, 859, 864, 868, 873, 878, 884, 888, 893, 898, 904, 909, 915, 921, 928, 932, 937, 942, 948, 953, 959, 965, 972, 977, 983, 989, 996, 1002, 1009, 1016, 1024, };
//---------------------------------------------------------------------------
//std::array<uint8_t, 8 * precomputeCount / 2> computeBits() noexcept {
//    std::array<uint8_t, 8 * precomputeCount / 2> result{};
//    for (size_t i = 0; i < precomputeCount; ++i) {
//        auto bits = i;
//        for (size_t j = offsets[i]; j < offsets[i + 1]; ++j) {
//            result[j] = countr_zero(bits);
//            bits &= bits - 1;
//        }
//    }
//    std::cout << "std::array<uint8_t, 8 * precomputeCount / 2> bitPos = {";
//    for (auto v : result) {
//        std::cout << static_cast<int>(v) << ", ";
//    }
//    std::cout << "}" << std::endl;
//    return result;
//}
//---------------------------------------------------------------------------
constexpr std::array<uint8_t, 8 * precomputeCount / 2> bitPos{0, 1, 0, 1, 2, 0, 2, 1, 2, 0, 1, 2, 3, 0, 3, 1, 3, 0, 1, 3, 2, 3, 0, 2, 3, 1, 2, 3, 0, 1, 2, 3, 4, 0, 4, 1, 4, 0, 1, 4, 2, 4, 0, 2, 4, 1, 2, 4, 0, 1, 2, 4, 3, 4, 0, 3, 4, 1, 3, 4, 0, 1, 3, 4, 2, 3, 4, 0, 2, 3, 4, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 0, 5, 1, 5, 0, 1, 5, 2, 5, 0, 2, 5, 1, 2, 5, 0, 1, 2, 5, 3, 5, 0, 3, 5, 1, 3, 5, 0, 1, 3, 5, 2, 3, 5, 0, 2, 3, 5, 1, 2, 3, 5, 0, 1, 2, 3, 5, 4, 5, 0, 4, 5, 1, 4, 5, 0, 1, 4, 5, 2, 4, 5, 0, 2, 4, 5, 1, 2, 4, 5, 0, 1, 2, 4, 5, 3, 4, 5, 0, 3, 4, 5, 1, 3, 4, 5, 0, 1, 3, 4, 5, 2, 3, 4, 5, 0, 2, 3, 4, 5, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 0, 6, 1, 6, 0, 1, 6, 2, 6, 0, 2, 6, 1, 2, 6, 0, 1, 2, 6, 3, 6, 0, 3, 6, 1, 3, 6, 0, 1, 3, 6, 2, 3, 6, 0, 2, 3, 6, 1, 2, 3, 6, 0, 1, 2, 3, 6, 4, 6, 0, 4, 6, 1, 4, 6, 0, 1, 4, 6, 2, 4, 6, 0, 2, 4, 6, 1, 2, 4, 6, 0, 1, 2, 4, 6, 3, 4, 6, 0, 3, 4, 6, 1, 3, 4, 6, 0, 1, 3, 4, 6, 2, 3, 4, 6, 0, 2, 3, 4, 6, 1, 2, 3, 4, 6, 0, 1, 2, 3, 4, 6, 5, 6, 0, 5, 6, 1, 5, 6, 0, 1, 5, 6, 2, 5, 6, 0, 2, 5, 6, 1, 2, 5, 6, 0, 1, 2, 5, 6, 3, 5, 6, 0, 3, 5, 6, 1, 3, 5, 6, 0, 1, 3, 5, 6, 2, 3, 5, 6, 0, 2, 3, 5, 6, 1, 2, 3, 5, 6, 0, 1, 2, 3, 5, 6, 4, 5, 6, 0, 4, 5, 6, 1, 4, 5, 6, 0, 1, 4, 5, 6, 2, 4, 5, 6, 0, 2, 4, 5, 6, 1, 2, 4, 5, 6, 0, 1, 2, 4, 5, 6, 3, 4, 5, 6, 0, 3, 4, 5, 6, 1, 3, 4, 5, 6, 0, 1, 3, 4, 5, 6, 2, 3, 4, 5, 6, 0, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6, 7, 0, 7, 1, 7, 0, 1, 7, 2, 7, 0, 2, 7, 1, 2, 7, 0, 1, 2, 7, 3, 7, 0, 3, 7, 1, 3, 7, 0, 1, 3, 7, 2, 3, 7, 0, 2, 3, 7, 1, 2, 3, 7, 0, 1, 2, 3, 7, 4, 7, 0, 4, 7, 1, 4, 7, 0, 1, 4, 7, 2, 4, 7, 0, 2, 4, 7, 1, 2, 4, 7, 0, 1, 2, 4, 7, 3, 4, 7, 0, 3, 4, 7, 1, 3, 4, 7, 0, 1, 3, 4, 7, 2, 3, 4, 7, 0, 2, 3, 4, 7, 1, 2, 3, 4, 7, 0, 1, 2, 3, 4, 7, 5, 7, 0, 5, 7, 1, 5, 7, 0, 1, 5, 7, 2, 5, 7, 0, 2, 5, 7, 1, 2, 5, 7, 0, 1, 2, 5, 7, 3, 5, 7, 0, 3, 5, 7, 1, 3, 5, 7, 0, 1, 3, 5, 7, 2, 3, 5, 7, 0, 2, 3, 5, 7, 1, 2, 3, 5, 7, 0, 1, 2, 3, 5, 7, 4, 5, 7, 0, 4, 5, 7, 1, 4, 5, 7, 0, 1, 4, 5, 7, 2, 4, 5, 7, 0, 2, 4, 5, 7, 1, 2, 4, 5, 7, 0, 1, 2, 4, 5, 7, 3, 4, 5, 7, 0, 3, 4, 5, 7, 1, 3, 4, 5, 7, 0, 1, 3, 4, 5, 7, 2, 3, 4, 5, 7, 0, 2, 3, 4, 5, 7, 1, 2, 3, 4, 5, 7, 0, 1, 2, 3, 4, 5, 7, 6, 7, 0, 6, 7, 1, 6, 7, 0, 1, 6, 7, 2, 6, 7, 0, 2, 6, 7, 1, 2, 6, 7, 0, 1, 2, 6, 7, 3, 6, 7, 0, 3, 6, 7, 1, 3, 6, 7, 0, 1, 3, 6, 7, 2, 3, 6, 7, 0, 2, 3, 6, 7, 1, 2, 3, 6, 7, 0, 1, 2, 3, 6, 7, 4, 6, 7, 0, 4, 6, 7, 1, 4, 6, 7, 0, 1, 4, 6, 7, 2, 4, 6, 7, 0, 2, 4, 6, 7, 1, 2, 4, 6, 7, 0, 1, 2, 4, 6, 7, 3, 4, 6, 7, 0, 3, 4, 6, 7, 1, 3, 4, 6, 7, 0, 1, 3, 4, 6, 7, 2, 3, 4, 6, 7, 0, 2, 3, 4, 6, 7, 1, 2, 3, 4, 6, 7, 0, 1, 2, 3, 4, 6, 7, 5, 6, 7, 0, 5, 6, 7, 1, 5, 6, 7, 0, 1, 5, 6, 7, 2, 5, 6, 7, 0, 2, 5, 6, 7, 1, 2, 5, 6, 7, 0, 1, 2, 5, 6, 7, 3, 5, 6, 7, 0, 3, 5, 6, 7, 1, 3, 5, 6, 7, 0, 1, 3, 5, 6, 7, 2, 3, 5, 6, 7, 0, 2, 3, 5, 6, 7, 1, 2, 3, 5, 6, 7, 0, 1, 2, 3, 5, 6, 7, 4, 5, 6, 7, 0, 4, 5, 6, 7, 1, 4, 5, 6, 7, 0, 1, 4, 5, 6, 7, 2, 4, 5, 6, 7, 0, 2, 4, 5, 6, 7, 1, 2, 4, 5, 6, 7, 0, 1, 2, 4, 5, 6, 7, 3, 4, 5, 6, 7, 0, 3, 4, 5, 6, 7, 1, 3, 4, 5, 6, 7, 0, 1, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6, 7, 0, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, };
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
uint8_t* BitLogic::bitsToOffsets(uint8_t* target, uint64_t bits) noexcept {
    uint8_t* dst = target;
    unalignedStore(dst, unalignedLoad<uint64_t>(bitPos.data() + offsets[bits & precomputeMask]));
    dst += offsets[(bits & precomputeMask) + 1] - offsets[bits & precomputeMask];
    bits >>= precomputeBits;
    size_t base = precomputeBits * 0x0101010101010101;
    while (bits) {
        unalignedStore(dst, unalignedLoad<uint64_t>(bitPos.data() + offsets[bits & precomputeMask]) + base);
        dst += offsets[(bits & precomputeMask) + 1] - offsets[bits & precomputeMask];
        bits >>= precomputeBits;
        base += precomputeBits * 0x0101010101010101;
    }
    return dst;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
