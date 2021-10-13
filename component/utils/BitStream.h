//
// Created by xiuwenli on 10/5/21.
//

#ifndef INDEX_BUILDER_BITSTREAM_H
#define INDEX_BUILDER_BITSTREAM_H

#include <vector>
#include <cstdint>
#include <string>
#include <sstream>

namespace engine::builder {

    class BitStream {
    public:
        BitStream(): cur(0)
        { }

        ~BitStream() {
            stream.clear();
            stream.shrink_to_fit();
            cur = 0;
        }

        // resize
        void resize(uint64_t size) {
            stream.clear();
            stream.resize(size);
            cur = 0;
        }

        // reset the cursor
        void reset();

        // set buf
        void append_bitstream(const BitStream & src);
        void append_string_stream(std::stringstream & src);
        void append_byte_array(const char * arr, uint64_t size);

        // write with little endian
        void put_int64(uint64_t num);
        void put_int32(uint32_t num);
        void put_int16(uint16_t num);
        void put_byte(uint8_t num);
        void put_string(const std::string & src);
        void put_string_ascii(const std::string & src);
        void put_int_ascii(uint64_t num);

        const uint8_t * get_stream() {
            return (uint8_t *) stream.data();
        }
        const uint8_t * get_cur_stream() {
            return (uint8_t *) stream.data() + sizeof(uint8_t) * cur;
        }

        inline uint64_t get_length() {
            return stream.size();
        }

        // read
        uint64_t get_int64();
        uint32_t get_int32();
        uint16_t get_int16();
        uint8_t get_byte();
        void get_string(std::string & dest);
        void get_string_ascii(std::string & dest);
        uint64_t get_int_ascii();

        // load buf
        void load_buf(std::vector<uint8_t> & src, uint32_t size) {
            stream.clear();
            stream = src;
            cur = 0;
        }
    //private:
        uint64_t cur = 0; // for decoding
        std::vector<uint8_t> stream;
    };
}


#endif //INDEX_BUILDER_BITSTREAM_H
