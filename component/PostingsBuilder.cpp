//
// Created by xiuwenli on 10/2/21.
//

#include "PostingsBuilder.h"
#include <iostream>
#include <filesystem>
#include "utils/BitStream.h"
#include <chrono>

using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;
using namespace std::chrono;

int PostingsBuilder::add_posting(uint32_t doc_id, const string& term) {
    terms_buf_.emplace_back(make_pair(term, doc_id));
    byte_counter += sizeof(doc_id) + term.size();
    if (byte_counter >= max_block_size_) {
        if(int ret = dump(); ret != 0) {
            cerr << "Dump intermediate posting file error." << endl;
            return ret;
        }
    }
    return 0;
}
int PostingsBuilder::add_postings(uint32_t doc_id,
                                  vector<string> & terms) {
    if (terms.empty()) {
        return 0;
    }
    for (auto const & term : terms) {
        byte_counter += term.size() + sizeof(uint32_t);
        terms_buf_.emplace_back(make_pair(term, doc_id));
        if (byte_counter >= max_block_size_) {
            // dump to the disk
            int ret = dump();
            if (ret != 0) {
                cerr << "Dump intermediate posting file error." << endl;
                return ret;
            }
        }
    }

    return 0;
}

int PostingsBuilder::dump() {
    if (terms_buf_.empty()) {
        return -1;
    }
//    if (byte_counter < max_block_size_) {
//        return 0;
//    }

    BitStream header_s;
    BitStream body_s;
    uint32_t header_size;

    auto begin_ts = steady_clock::now();
    // sort the posting
    sort(terms_buf_.begin(), terms_buf_.end());
    auto sort_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();

    begin_ts = steady_clock::now();

    vector<Posting> output;
    for (auto & pr : terms_buf_) {
        if (output.empty() || output.back().term != pr.first) {
            output.emplace_back((Posting){pr.first, {pr.second}, {1}});
        } else if (output.back().doc_ids.back() == pr.second){
            ++output.back().frequencies.back();
        } else {
            output.back().doc_ids.push_back(pr.second);
            output.back().frequencies.push_back(1);
        }
    }
    size_t output_num = output.size();
    auto merge_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();

    begin_ts = steady_clock::now();
    p_block_encoder_->encode_inter_block(output, header_s, body_s, header_size);
    auto encode_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();

    // clear the posting table
    terms_buf_.clear();

    //cout << terms_buf_.capacity() << " " << output.capacity() << endl;

    begin_ts = steady_clock::now();
    // open fd
    if (!fs::exists(block_dir_)) {
        fs::create_directory(block_dir_);
    }
    fs::path dir(block_dir_);
    fs::path file(file_base_ + to_string(block_counter));
    fs::path block_file = dir / file;

    cout << block_file << ", header size: " << header_s.get_length()
         << ", body size: " << body_s.get_length()
         << ", list length: " << output_num
         << endl;

    FILE * fd = fopen(block_file.c_str(), "wb");
    assert(fd != nullptr);

    // write
    fwrite(&header_size, sizeof(uint32_t), 1, fd);
    fwrite(header_s.get_stream(), sizeof(uint8_t), header_s.get_length(), fd);
    fwrite(body_s.get_stream(), sizeof(uint8_t), body_s.get_length(), fd);
    fclose(fd);

    auto write_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();

    cout << block_file << ", sort elapse: " << sort_elapse << " s"
        << ", merge elapse: " << merge_elapse << " s"
        << ", encode elapse: " << encode_elapse << " s"
        << ", write elapse: " << write_elapse << " s"
        << endl;

    // update counter
    ++block_counter;
    byte_counter = 0;
    block_names.push_back(block_file);
    return 0;
}

// block loader with encoder

BlockLoader::BlockLoader(string block_filename, shared_ptr<BlockEncoder> p_block_encoder)
    : block_filename_(move(block_filename))
    , p_block_encoder_(p_block_encoder)
    , cur_posting(0)
{
    assert(fs::exists(block_filename_));
    fd = fopen(block_filename_.c_str(), "r");
    if (fd == nullptr) {
        cerr << "Open block file: " << block_filename_ << " error." << endl;
        abort();
    }
    // decode header
    fread(&header_.header_size, 1, sizeof(uint32_t), fd);

    char * byte_buf = new char[header_.header_size];
    fread(byte_buf, sizeof(char), header_.header_size, fd);
    buf_.append_byte_array(byte_buf, header_.header_size);
    delete [] byte_buf;

    p_block_encoder->decode_inter_header(buf_, header_);
    cout << "[Decoder] Header size: " << header_.header_size
         << ", chunk count: " << header_.chunk_count
         << ", block size: " << header_.block_size
         << endl;
    buf_.stream.clear();
    buf_.reset();
}

BlockLoader::~BlockLoader() {
    fclose(fd);
    fd = nullptr;
}

uint32_t BlockLoader::postings_size() {
    return header_.chunk_count - cur_posting;
}

int BlockLoader::load_posting(Posting &posting) {
    if (cur_posting == header_.chunk_count) {
        return -1;
    }
    uint32_t length;
    buf_.reset();
    buf_.stream.clear();
    if (cur_posting == 0) {
        length = header_.chunk_offsets[cur_posting];
    } else {
        length = header_.chunk_offsets[cur_posting]
                - header_.chunk_offsets[cur_posting - 1];
    }
    cur_posting++;
    //cout << "Posting size: " << length << endl;
    char * byte_buf = new char[length];
    fread(byte_buf, sizeof(char), length, fd);
    buf_.append_byte_array(byte_buf, length);
    delete []byte_buf;

    p_block_encoder_->decode_inter_posting(buf_, posting);
    return 0;
}

bool BlockLoader::empty() {
    return cur_posting == header_.chunk_count;
}
