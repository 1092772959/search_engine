//
// Created by xiuwenli on 10/31/21.
//

#include "QueryExecution.h"
#include "LexiconBuilder.h"
#include <utility>
#include <iostream>

using namespace engine::process;
using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;

QueryExecution::QueryExecution(string inverted_list_file,
                               string lexicon_file)
    : inverted_list_file_(move(inverted_list_file))
    , p_block_codec_(new BlockBinaryEncoder())
{
    assert(fs::exists(inverted_list_file_));
    assert(fs::exists(lexicon_file));

    LexiconEncoder encoder;
    LexiconHeader header;
    BitStream body;
    encoder.load(lexicon_file, header, body);
    encoder.decode(header, body, lexicons_);

    fd_inv_list_ = fopen(inverted_list_file_.c_str(), "r");
    assert(fd_inv_list_ != nullptr);
}

QueryExecution::~QueryExecution() {
    fclose(fd_inv_list_);
}

int QueryExecution::openList(const string &term, InvertedList &lp) {
    auto ptr = lexicons_.find(term);
    if (ptr == lexicons_.end()) {
        cout << "Contains no term: " << term << endl;
        return -1;
    }
    LexiconEntry & entry = ptr->second;
    lp.entry_ = entry;
    lp.term = term;

    lp.fd_inv_list_ = fopen(inverted_list_file_.c_str(), "r");
    assert (lp.fd_inv_list_ != nullptr);

    if (loadBlockHeader(entry.block_cursor_) != 0) {
        cerr << "Load header for offset: " << entry.block_cursor_
            << endl;
        return -1;
    }
    const BlockHeader * header =
            &block_header_dict_.find(entry.block_cursor_)->second;

    uint64_t offset = lp.entry_.block_cursor_ + header.header_size;
    if (lp.entry_.chunk_offset_ > 0) {
        offset += header.chunk_offsets[lp.entry_.chunk_offset_];
    }
    fseek(lp.fd_inv_list_, offset, SEEK_SET);
    // load meta data for this term
    int count = 0;
    int chunk_index = lp.entry_.chunk_offset_;
    while (count < lp.entry_.length_) {
        if (chunk_index == header->chunk_count - 1) {
            count += header->last_chunk_length;
            if (count == lp.entry_.length_) {
                break;
            }
        } else {

        }
    }
    return 0;
}


int QueryExecution::nextGEQ(InvertedList &lp, uint32_t k) {
    uint64_t block_header_offset = lp.entry_.block_cursor_;
    // TODO: binary search or galloping
    return 0;
}

int QueryExecution::loadBlockHeader(uint64_t block_offset) {
    if (block_header_dict_.find(block_offset) != block_header_dict_.end()) {
        return 0;
    }
    BitStream buf;
    BlockHeader header;
    fseek(fd_inv_list_, block_offset, SEEK_SET);
    // read from disk
    uint32_t header_size;
    fread(&header_size, sizeof(uint32_t), 1, fd_inv_list_);

    char * byte_buf = new char[header_size];
    fread(byte_buf, sizeof(char), header_size, fd_inv_list_);

    buf.append_byte_array(byte_buf, header_size);
    delete [] byte_buf;

    header.header_size = header_size;
    if (p_block_codec_->decode_block_header(buf, header) != 0) {
        cerr << "Decode header error from offset: " << block_offset << endl;
        return -1;
    }

    // memorize
    block_header_dict_[block_offset] = header;

    return 0;
}

int QueryExecution::loadAll(const string &term,
                            InvertedList & lp) {
    if (openList(term, lp) != 0) {
        cerr << "Open list error for term: " << term << endl;
        return -1;
    }

    auto header_ptr = block_header_dict_.find(lp.entry_.block_cursor_);
    const BlockHeader * header = &header_ptr->second;
    int length_count = 0;
    int chunk_index = lp.entry_.chunk_offset_;
    for ( ;length_count < lp.entry_.length_; ) {
        if (chunk_index == header->chunk_count) { // across the block
            // load the header of the next block
            uint64_t header_offset = lp.entry_.block_cursor_
                    + header->header_size + header->chunk_offsets.back();
            if (loadBlockHeader(header_offset) != 0) {
                cerr << "Load Block header during loading across-block inverted list, "
                    << "term: " << term << ", offset: " << header_offset
                    << endl;
                return -1;
            }
            header = &(block_header_dict_.find(header_offset)->second);
            chunk_index = 0;
        } else {
            uint32_t length = 0;
            if (chunk_index == 0) {
                length = header->chunk_offsets[chunk_index];
            } else {
                length = header->chunk_offsets[chunk_index] - header->chunk_offsets[chunk_index - 1];
            }
            // incr chunk index by one
            ++chunk_index;

            //cout << "Posting size: " << length << endl;
            BitStream buf;
            char * byte_buf = new char[length];
            fread(byte_buf, sizeof(char), length, lp.fd_inv_list_);
            buf.append_byte_array(byte_buf, length);
            delete []byte_buf;

            p_block_codec_->decode_chunk(buf, lp.chunk_cache_);
            length_count = lp.chunk_cache_.doc_ids.size();
        }
    }
    return 0;
}

int QueryExecution::closeList(InvertedList &lp) {
    fclose(lp.fd_inv_list_);
    return 0;
}

