//
// Created by xiuwenli on 10/5/21.
//

#include "encoding.h"

using namespace std;

namespace engine::builder {
        // common use
        const int NSELECTORS = 9;
        const int SELECTOR_BITS = 4;
        const int CODE_BITS = 28;
        const int MAX_VALUE = ((1UL << CODE_BITS) - 1);
        const int SELECTOR_MASK = 0x0000000F;

        static const struct {
            uint32_t nitems;
            uint32_t nbits;
            uint32_t nwaste;
        } selectors[NSELECTORS] = {
                {28,  1, 0},
                {14,  2, 0},
                { 9,  3, 1},
                { 7,  4, 0},
                { 5,  5, 3},
                { 4,  7, 0},
                { 3,  9, 1},
                { 2, 14, 0},
                { 1, 28, 0},
        };


    void delta_encode_v2(const vector<uint32_t> &nums, BitStream &bit_stream) {
        assert(nums.size() > 0);
        uint32_t base = nums[0];
        var_byte_encode(base, bit_stream);
        vector<uint32_t> gaps;
        for (size_t idx = 1; idx < nums.size(); ++idx) {
            uint32_t del = nums[idx] - nums[idx - 1];
            gaps.push_back(del);
        }
        simple9_encode(gaps, bit_stream);
    }

    void delta_decode_v2(BitStream &bit_stream, vector<uint32_t> &nums) {
        uint32_t base;
        var_byte_decode(bit_stream, base);
        nums.push_back(base);
        simple9_decode(bit_stream,nums);
        uint32_t pre = base;
        for (size_t idx = 1; idx < nums.size(); ++idx) {
            nums[idx] = nums[idx] + pre;
            pre = nums[idx];
        }
    }

    void simple9_encode(const vector<uint32_t> &nums, BitStream &bit_stream) {
        uint32_t selector, data, shift;
        var_byte_encode(nums.size(), bit_stream);
        size_t n = nums.size();
        size_t nbytes = 0;
        size_t nitems;
        size_t i;
        for (uint32_t index = 0; index < nums.size();) {
            for (selector = 0; selector < NSELECTORS; selector++) {
                data = selector;
                shift = SELECTOR_BITS;
                nitems = 0;

                for (i = index; i < nums.size(); i++) {
                    assert(nums[i] <= MAX_VALUE);

                    if (nitems == selectors[selector].nitems)
                        break;

                    if (nums[i] > (1UL << selectors[selector].nbits) - 1)
                        break;

                    data |= (nums[i] << shift);
                    shift += selectors[selector].nbits;
                    nitems++;
                }

                if (nitems == selectors[selector].nitems || index + nitems == n) {
                    bit_stream.put_int32(data);
                    nbytes += sizeof(data);
                    index += nitems;
                    break;
                }

            }
        }
    }

    void simple9_decode(BitStream &bit_stream, vector<uint32_t> &nums) {
        size_t n;
        var_byte_decode(bit_stream, n);
        if (n == 0 ) {
            return;
        }
        nums.resize(n);
        uint32_t index = 0;
        uint32_t data;
        uint32_t select;
        uint32_t mask;

        size_t nbytes = 0;
        size_t nitems = 0;
        size_t i;
        while (nitems < n) {
            data = bit_stream.get_int32();
            nbytes += sizeof data;
            select = data & SELECTOR_MASK;
            data >>= SELECTOR_BITS;
            mask = (1 << selectors[select].nbits) - 1;

            for (i = 0; i < selectors[select].nitems; i++) {
                nums[nitems] = data & mask;
                nitems++;

                if (nitems == n) {
                    break;
                }
                data >>= selectors[select].nbits;
            }
        }
    }
}


