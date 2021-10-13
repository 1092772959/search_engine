//
// Created by xiuwenli on 10/5/21.
//

#include "BitStream.h"
#include <iostream>

using namespace engine::builder;
using namespace std;

const uint8_t BYTE_MASK = 0xff;

void BitStream::put_int32(uint32_t num) {
    for (int i = 0; i < 4; ++i) {
        stream.push_back(num & BYTE_MASK);
        num >>= 8;
    }
}

void BitStream::put_int64(uint64_t num) {
    for (int i = 0; i < 8; ++i) {
        stream.push_back(num & BYTE_MASK);
        num >>= 8;
    }
}

void BitStream::put_byte(uint8_t num) {
    stream.push_back(num);
}

void BitStream::put_int16(uint16_t num) {
    for (int i = 0; i < 2; ++i) {
        stream.push_back(num & BYTE_MASK);
        num >>= 8;
    }
}

void BitStream::put_string(const string &src) {
    size_t size = src.size();
    put_int32(size);
    for (auto & c : src) {
        put_byte(c);
    }
}

void BitStream::append_bitstream(const BitStream & src) {
    stream.insert(stream.end(), src.stream.begin(), src.stream.end());
}

uint64_t BitStream::get_int64() {
    assert(cur + 7 < stream.size());
    uint64_t ret = 0;
    for (int i = 0; i < 8; ++i) {
        ret |= (stream[cur] << (i * 8));
        cur++;
    }
    return ret;
}

uint32_t BitStream::get_int32() {
    assert(cur + 3 < stream.size());
    uint32_t ret = 0;
    for (int i = 0; i < 4; ++i) {
        ret |= (stream[cur] << (i * 8));
        cur++;
    }
    return ret;
}

uint16_t BitStream::get_int16() {
    assert(cur + 1 < stream.size());
    uint16_t ret = 0;
    for (int i = 0; i < 2; ++i) {
        ret |= (stream[cur] << (i * 8));
        cur++;
    }
    return ret;
}

uint8_t BitStream::get_byte() {
    assert(cur < stream.size());
    return stream[cur++];
}

void BitStream::get_string(string &dest) {
    uint32_t size = get_int32();
    dest.clear();
    dest.resize(size);
    for (int i = 0; i < size; ++i) {
        dest[i] = get_byte();
    }
}

void BitStream::reset() {
    cur = 0;
}

void BitStream::append_string_stream(stringstream &src) {
    size_t size = src.tellp();
    stream.resize(size);
    src.read((char*)&stream, size);
}

void BitStream::put_int_ascii(uint64_t num) {
    if (num == 0) {
        stream.push_back('0');
    } else {
        vector<char> digits;
        for (; num; num /= 10) {
            char digit = (num % 10) + '0';
            digits.push_back(digit);
        }
        reverse(digits.begin(), digits.end());
        stream.insert(stream.end(), digits.begin(), digits.end());
    }
}

uint64_t BitStream::get_int_ascii() {
    uint64_t ret = 0;
    vector<char> digits;
    while(cur < stream.size() && isdigit(stream[cur])) {
        digits.push_back(stream[cur]);
        cur++;
    }
    for(auto it = digits.begin(); it != digits.end(); ++it) {
        ret *= 10;
        ret += (*it) - '0';
    }
    return ret;
}

void BitStream::put_string_ascii(const string &src) {
    stream.insert(stream.end(), src.begin(), src.end());
}

void BitStream::get_string_ascii(string &dest) {
    while (cur < stream.size()) {
        if(stream[cur] == ' ' || stream[cur] == '\n') {
            break;
        }
        dest.push_back(stream[cur]);
        cur++;
    }
}

void BitStream::append_byte_array(const char *arr, uint64_t len) {
    stream.insert(stream.end(), arr, arr + len);
}

