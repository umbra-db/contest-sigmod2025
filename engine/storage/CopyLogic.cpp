#include "storage/CopyLogic.hpp"
#include "infra/helper/BitOps.hpp"
#include "query/RuntimeValue.hpp"
#include "storage/BitLogic.hpp"
#include "storage/StringPtr.hpp"
#include <array>
#include <cassert>
#include <cstring>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
template <typename SrcT, typename ComputeValueT>
static void extractor(uint64_t* dst, const SrcT* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples, ComputeValueT&& computeValue) noexcept {
    if (!numTuples)
        return;

    // Fast path, only single bit
    if ((srcOffsets & (srcOffsets - 1)) == 0) {
        // Even if this is only a single bit, dstOffsets may be empty
        std::fill(dst, dst + numTuples, RuntimeValue::nullValue);
        if (srcOffsets) {
            assert(engine::has_single_bit(srcOffsets));
            assert(engine::has_single_bit(dstOffsets));
            auto srcPos = engine::countr_zero(srcOffsets);
            auto dstPos = engine::countr_zero(dstOffsets);
            static_assert(RuntimeValue::nullValue == ~0ull);
            auto value = computeValue(src + srcPos);
            dst[dstPos] = value;
        }
        return;
    }
    // Fast path, all bits sets
    if (srcOffsets == ~0ull) {
        assert(dstOffsets == ~0ull);
        assert(numTuples == 64);
        for (size_t i = 0; i < 64; i++) {
            auto value = computeValue(src + i);
            dst[i] = value;
        }
        return;
    }

    assert(engine::popcount(srcOffsets) == engine::popcount(dstOffsets));
    assert(dstOffsets != ~0ull);

    auto srcDense = BitLogic::isDense(srcOffsets);
    auto dstDense = BitLogic::isDense(dstOffsets);

    if (srcDense && dstDense) {
        auto [srcPos, srcEnd] = BitLogic::getDenseIterators(srcOffsets);
        auto [dstPos, dstEnd] = BitLogic::getDenseIterators(dstOffsets);
        if (dstPos.index != 0)
            std::fill(dst, dst + dstPos.index, RuntimeValue::nullValue);
        for (; srcPos != srcEnd; ++srcPos, ++dstPos)
            dst[*dstPos] = computeValue(src + *srcPos);
        if (dstEnd.index != numTuples)
            std::fill(dst + dstEnd.index, dst + numTuples, RuntimeValue::nullValue);
        return;
    }

    std::array<uint8_t, 71> srcBuffer, dstBuffer;
    if (dstDense) {
        auto [srcPos, srcEnd] = BitLogic::getSparseIterators(srcBuffer.data(), srcOffsets);
        auto [dstPos, dstEnd] = BitLogic::getDenseIterators(dstOffsets);
        if (dstPos.index != 0)
            std::fill(dst, dst + dstPos.index, RuntimeValue::nullValue);
        for (; dstPos != dstEnd; ++srcPos, ++dstPos)
            dst[*dstPos] = computeValue(src + *srcPos);
        if (dstEnd.index != numTuples)
            std::fill(dst + dstEnd.index, dst + numTuples, RuntimeValue::nullValue);
        return;
    }

    // Just fill entire destination with nulls
    std::fill(dst, dst + numTuples, RuntimeValue::nullValue);
    if (srcDense) {
        auto [srcPos, srcEnd] = BitLogic::getDenseIterators(srcOffsets);
        auto [dstPos, dstEnd] = BitLogic::getSparseIterators(dstBuffer.data(), dstOffsets);
        for (; srcPos != srcEnd; ++srcPos, ++dstPos)
            dst[*dstPos] = computeValue(src + *srcPos);
        return;
    }
    // Both are sparse
    auto [srcPos, srcEnd] = BitLogic::getSparseIterators(srcBuffer.data(), srcOffsets);
    auto [dstPos, dstEnd] = BitLogic::getSparseIterators(dstBuffer.data(), dstOffsets);
    for (; srcPos != srcEnd; ++srcPos, ++dstPos)
        dst[*dstPos] = computeValue(src + *srcPos);
}
//---------------------------------------------------------------------------
void CopyLogic::extractInt32(uint64_t* dst, const uint32_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples) noexcept {
    return extractor(dst, src, srcOffsets, dstOffsets, numTuples, [](const auto* value) { return *value; });
}
//---------------------------------------------------------------------------
void CopyLogic::extractInt64(uint64_t* dst, const uint64_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples) noexcept {
    return extractor(dst, src, srcOffsets, dstOffsets, numTuples, [](const auto* value) { return *value; });
}
//---------------------------------------------------------------------------
void CopyLogic::extractVarChar(uint64_t* dst, const uint16_t* src, uint64_t srcOffsets, uint64_t dstOffsets, size_t numTuples, const char* stringHead) noexcept {
    return extractor(dst, src, srcOffsets, dstOffsets, numTuples, [stringHead](const auto* value) {
        auto offset = *value;
        auto prevOffset = *(value - 1);
        assert(prevOffset <= offset);
        auto str = StringPtr::fromString(stringHead + prevOffset, static_cast<size_t>(offset - prevOffset));
        return str.val();
    });
}
//---------------------------------------------------------------------------
}
