//
// Created by xiuwenli on 10/7/21.
//

#include "BlockEncoder.h"
#include "utils/encoding.h"
#include <sstream>
#include <iostream>

using namespace engine::builder;
using namespace std;

/*
 * header: BlockHeader
 *
 * body:
 * [list_1, list_2, ..., list_n]
 *
 * list:
 * [term, doc_id_list, freq_list]
 */
int BlockBinaryEncoder::encode_inter_block(const vector<Posting> &posting_table,
                                           BitStream & header_s,
                                           BitStream & body_s,
                                           uint32_t & header_size) {
    BlockHeader header = {0};

    for (auto const & posting : posting_table) {
        if(posting.doc_ids.empty()) {
            continue;
        }
        encode_inter_posting(posting.term, posting, body_s);

        // update header
        header.chunk_count++;
        header.chunk_offsets.push_back(body_s.get_length());
        header.last_doc_ids.push_back(posting.doc_ids.back());
    }
    header.block_size = body_s.get_length() * sizeof(char);

    // encode header
    encode_inter_header(header, header_s);
    header_size = header_s.get_length();
    return 0;
}

int BlockBinaryEncoder::encode_block_header(BlockHeader &header, BitStream &output) {
    var_byte_encode(header.block_size, output);
    var_byte_encode(header.chunk_count, output);
    var_byte_encode(header.last_chunk_length, output);
    delta_encode(header.chunk_offsets, output);
    for (unsigned int last_doc_id : header.last_doc_ids) {
        var_byte_encode(last_doc_id, output);
    }
    return 0;
}

int BlockBinaryEncoder::encode_inter_header(BlockHeader &header, BitStream &out) {
    var_byte_encode(header.block_size, out);
    var_byte_encode(header.chunk_count, out);
    delta_encode(header.chunk_offsets, out);
    return 0;
}

int BlockBinaryEncoder::decode_inter_header(BitStream &src, BlockHeader &header) {
    var_byte_decode(src, header.block_size);
    var_byte_decode(src, header.chunk_count);
    delta_decode(src, header.chunk_offsets);
    return 0;
}

int BlockBinaryEncoder::encode_inter_posting(const string & term,
                                       const Posting &posting,
                                       BitStream &output) {
    output.put_string(term);
    delta_encode(posting.doc_ids, output);
    bit_packing(posting.frequencies, output);
    return 0;
}

int BlockBinaryEncoder::decode_inter_block(BitStream &src, Block & inter_block) {
    decode_inter_header(src, inter_block.header_);
    for (size_t i = 0; i < inter_block.header_.chunk_count; ++i) {
        Posting posting;
        decode_inter_posting(src, posting);
        inter_block.postings.emplace_back(move(posting));
    }
    return 0;
}

int BlockBinaryEncoder::decode_block_header(BitStream &src, BlockHeader &header) {
    var_byte_decode(src, header.block_size);
    var_byte_decode(src, header.chunk_count);
    var_byte_decode(src, header.last_chunk_length);
    delta_decode(src, header.chunk_offsets);
    for (size_t i = 0; i < header.chunk_count; ++i) {
        uint32_t last_doc_id;
        var_byte_decode(src, last_doc_id);
        header.last_doc_ids.push_back(last_doc_id);
    }
    return 0;
}

int BlockBinaryEncoder::decode_inter_posting(BitStream &src, Posting &posting) {
    src.get_string(posting.term);
    delta_decode(src, posting.doc_ids);
    bit_packing_decode(src, posting.frequencies);
    return 0;
}

int BlockBinaryEncoder::encode_chunk(const Posting & posting, BitStream & out) {
    delta_encode(posting.doc_ids, out);
    bit_packing(posting.frequencies, out);
    return 0;
}

int BlockBinaryEncoder::decode_chunk(BitStream &src, Posting &posting) {
    delta_decode(src, posting.doc_ids);
    bit_packing_decode(src, posting.frequencies);
    return 0;
}

// for plain text encode/decode

int BlockPlainEncoder::encode_inter_block(const vector<Posting> &posting_table,
                                          BitStream & header_s,
                                          BitStream & body_s,
                                          uint32_t & header_size) {
    BlockHeader header = {0};

    for (auto const & posting : posting_table) {
        if (posting.doc_ids.size() == 0) {
            continue;
        }

        // output the plain text as a chunk
        encode_inter_posting(posting.term, posting, body_s);

        header.chunk_count++;
        header.chunk_offsets.push_back(body_s.get_length());
        //cout << "Encoder posting body size: " << body_stream.get_length() << endl;
        header.last_doc_ids.push_back(posting.doc_ids.back());
    }
    header.block_size = body_s.get_length(); // body size
    // encode header
    encode_block_header(header, header_s);
    // append body bit stream
    header_size = header_s.get_length();

    cout << "Encoder header size: " << header_size
        << ", body size: " << header.block_size
        << ", chunk count: " << header.chunk_count
        << endl;

    return 0;
}

int BlockPlainEncoder::encode_block_header(BlockHeader &header, BitStream &out) {
    out.put_int_ascii(header.block_size);
    out.put_byte(' ');
    out.put_int_ascii(header.chunk_count);
    out.put_byte('\n');
    for (auto const & offset : header.chunk_offsets) {
        out.put_int_ascii(offset);
        out.put_byte(' ');
    }
    out.put_byte('\n');
    for (auto const & last_doc_id : header.last_doc_ids) {
        out.put_int_ascii(last_doc_id);
        out.put_byte(' ');
    }
    out.put_byte('\n');
    return 0;
}

int BlockPlainEncoder::encode_inter_posting(const std::string &term,
                                            const Posting &posting,
                                            BitStream &out) {
    out.put_string_ascii(term);
    out.put_byte(' ');
    out.put_int_ascii(posting.doc_ids.size());
    out.put_byte('\n');
    for (auto const & doc_id : posting.doc_ids) {
        out.put_int_ascii(doc_id);
        out.put_byte(' ');
    }
    out.put_byte('\n');
    for (auto const & freq : posting.frequencies) {
        out.put_int_ascii(freq);
        out.put_byte(' ');
    }
    out.put_byte('\n');
    return 0;
}

int BlockPlainEncoder::decode_inter_block(BitStream &src, Block &block) {
    decode_inter_header(src, block.header_);
    for (uint32_t i = 0; i < block.header_.chunk_count; ++i) {
        Posting posting;
        decode_inter_posting(src, posting);
        block.postings.emplace_back(move(posting));
    }
    return 0;
}

int BlockPlainEncoder::decode_block_header(BitStream &src, BlockHeader &header) {
    header.block_size = src.get_int_ascii();
    src.get_byte();
    header.chunk_count = src.get_int_ascii();
    src.get_byte();
    for (uint32_t i = 0; i < header.chunk_count; ++i) {
        header.chunk_offsets.emplace_back(src.get_int_ascii());
        src.get_byte();
    }
    src.get_byte();
    for (uint32_t i = 0; i < header.chunk_count; ++i) {
        header.last_doc_ids.emplace_back(src.get_int_ascii());
        src.get_byte();
    }
    src.get_byte();
    return 0;
}

int BlockPlainEncoder::decode_inter_posting(BitStream &src, Posting &posting) {
    src.get_string_ascii(posting.term);
    src.get_byte();
    uint32_t list_size = src.get_int_ascii();
    src.get_byte();
    for (uint32_t i = 0; i < list_size; ++i) {
        posting.doc_ids.emplace_back(src.get_int_ascii());
        src.get_byte();
    }
    src.get_byte();
    for (uint32_t i = 0; i < list_size; ++i) {
        posting.frequencies.emplace_back(src.get_int_ascii());
        src.get_byte();
    }
    src.get_byte();
    return 0;
}

int BlockPlainEncoder::encode_chunk(const Posting &posting, BitStream &out) {
    assert(false);
    return 0;
}

int BlockPlainEncoder::decode_chunk(BitStream &src, Posting &posting) {
    assert(false);
    return 0;
}

int BlockPlainEncoder::encode_inter_header(BlockHeader &header, BitStream &out) {
    out.put_int_ascii(header.block_size);
    out.put_byte(' ');
    out.put_int_ascii(header.chunk_count);
    out.put_byte('\n');
    for (auto const & offset : header.chunk_offsets) {
        out.put_int_ascii(offset);
        out.put_byte(' ');
    }
    out.put_byte('\n');
    return 0;
}

int BlockPlainEncoder::decode_inter_header(BitStream & src, BlockHeader & header) {
    header.block_size = src.get_int_ascii();
    src.get_byte();
    header.chunk_count = src.get_int_ascii();
    src.get_byte();
    for (uint32_t i = 0; i < header.chunk_count; ++i) {
        header.chunk_offsets.emplace_back(src.get_int_ascii());
        src.get_byte();
    }
    src.get_byte();
}