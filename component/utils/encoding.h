//
// Created by xiuwenli on 10/5/21.
//

#ifndef INDEX_BUILDER_ENCODING_H
#define INDEX_BUILDER_ENCODING_H

#include "BitStream.h"
#include <iostream>

namespace engine {
    namespace builder {
        struct BitPackingHeader {
            uint64_t length;
            uint8_t width;
        };

        // encode
//        template<class T>
//        void var_byte_encode(T num, BitStream & bit_stream);
//        template<class T>
//        void bit_packing(const std::vector<T> & nums, BitStream & bit_stream);
//        template<class T>
//        void delta_encode(const std::vector<T> & nums, BitStream & bit_stream);
//
//        // decode
//        template<class T>
//        void var_byte_decode(BitStream & bit_stream, T & out);
//        template<class T>
//        void bit_packing_decode(BitStream & bit_stream, std::vector<T> &nums);
//        template<class T>
//        void delta_decode(BitStream & bit_stream, std::vector<T> &nums);
        template<class T>
        uint8_t cal_width(T num) {
            if (num == 0) {
                return 0;
            }
            uint8_t width = 0;
            while (num > 0) {
                width++;
                num >>= 1;
            }
            return width;
        }

        // definition for the header
        template<class T>
        void var_byte_encode(T num, BitStream & bit_stream) {
            uint8_t mask = 0x7f;
            uint8_t end_mask = 0x80;
            while (num > mask) {
                bit_stream.put_byte(num & mask);
                num = num >> 7;
            }
            bit_stream.put_byte(num | end_mask);
        }

        template<class T>
        void bit_packing(const std::vector<T> & nums, BitStream & bit_stream) {
            if (nums.empty()) {
                bit_stream.put_byte(0);
                return;
            }
            uint8_t width = 1;
            for (auto & num : nums) {
                uint8_t cur_w = cal_width(num);
                width = std::max(width, cur_w);
            }
            // header
            BitPackingHeader header;
            header.length = nums.size();
            header.width = width;

            bit_stream.put_byte(header.width);
            bit_stream.put_int64(header.length);

            if (width <= 8 * sizeof(int8_t)) {
                for (auto & num : nums) {
                    bit_stream.put_byte(static_cast<int8_t>(num));
                }
            } else if (width <= 8 * sizeof(int16_t)) {
                for (auto & num : nums) {
                    bit_stream.put_int16(static_cast<int16_t>(num));
                }
            } else if (width <= 8 * sizeof(int32_t)) {
                for (auto & num : nums) {
                    bit_stream.put_int32(static_cast<int32_t>(num));
                }
            } else {
                for (auto & num : nums) {
                    bit_stream.put_int64(static_cast<int64_t>(num));
                }
            }
        }

        template<class T>
        void delta_encode(const std::vector<T>& nums, BitStream & bit_stream) {
            uint64_t base = nums[0];
            var_byte_encode(base, bit_stream);
            std::vector<uint64_t> deltas;
            for (size_t idx = 1; idx < nums.size(); ++idx) {
                uint64_t del = nums[idx] - nums[idx - 1];
                deltas.push_back(del);
            }
            bit_packing(deltas, bit_stream);
        }

        template<class T>
        void var_byte_decode(BitStream &bit_stream, T & out) {
            out = 0;
            uint8_t mask = 0x7f;
            uint8_t end_mask = 0x80;
            uint8_t byte;
            uint8_t counter = 0;
            do {
                byte = bit_stream.get_byte();
                out |= ((byte & mask) << counter);
                counter += 7;
            } while( (byte & end_mask) == 0);
        }

        template<class T>
        void bit_packing_decode(BitStream &bit_stream, std::vector<T> &nums) {
            BitPackingHeader header;

            header.width = bit_stream.get_byte();
            if (header.width == 0) {
                return;
            }
            header.length = bit_stream.get_int64();

            if (header.width <= 8 * sizeof(int8_t)) {
                for (size_t i = 0; i < header.length; ++i) {
                    nums.push_back(bit_stream.get_byte());
                }
            } else if (header.width <= 8 * sizeof(int16_t)) {
                for (size_t i = 0; i < header.length; ++i) {
                    nums.push_back(bit_stream.get_int16());
                }
            } else if (header.width <= 8 * sizeof(int32_t)) {
                for (size_t i = 0; i < header.length; ++i) {
                    nums.push_back(bit_stream.get_int32());
                }
            } else {
                for (size_t i = 0; i < header.length; ++i) {
                    nums.push_back(bit_stream.get_int64());
                }
            }
        }

        template<class T>
        void delta_decode(BitStream & bit_stream, std::vector<T> &nums) {
            T base;
            var_byte_decode(bit_stream, base);
            nums.push_back(base);
            T pre = base;
            bit_packing_decode(bit_stream, nums);
            for (size_t i = 1; i < nums.size(); ++i) {
                nums[i] = nums[i] + pre;
                pre = nums[i];
            }
        }

        void delta_encode_v2(const std::vector<uint32_t> &nums, BitStream & bit_stream);
        void delta_decode_v2(BitStream & bit_stream,  std::vector<uint32_t> &nums);
        void simple9_encode(const std::vector<uint32_t> & nums, BitStream & bit_stream);
        void simple9_decode(BitStream & bit_stream, std::vector<uint32_t> & nums);
    }
}


#endif //INDEX_BUILDER_ENCODING_H
