//
// Created by xiuwenli on 10/7/21.
//

#include "InvertedListBuilder.h"
#include <utility>
#include <iostream>

using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;

BufferNode::BufferNode(std::string block_file,
                       shared_ptr<BlockEncoder> p_block_encoder,
                       uint32_t buf_size_limit)
    : block_file_(move(block_file))
    , buf_size_limit_(buf_size_limit)
    , buf_size_(0)
    , loader_(block_file_, p_block_encoder)
{
    load_from_disk();
}

bool BufferNode::operator < (const BufferNode &rhs) const {
    if (posting_buf.empty()) {
        return false;
    }
    if (rhs.posting_buf.empty()) {
        return true;
    }
    if (posting_buf.front().term == rhs.posting_buf.front().term) {
        return posting_buf.front().doc_ids.front() > rhs.posting_buf.front().doc_ids.front();
    }
    return posting_buf.front().term > rhs.posting_buf.front().term;
}


bool BufferNode::block_empty() {
    return loader_.empty();
}

bool BufferNode::cache_empty() {
    return posting_buf.empty();
}

int BufferNode::load_from_disk() {
    if (block_empty()) {
        return -1;
    }
    int ret = 0;
    while (!loader_.empty() && buf_size_ < buf_size_limit_) {
        Posting posting;
        ret = loader_.load_posting(posting);
        if (ret != 0) {
            cerr << "Load inter posting fails" << endl;
            abort();
        }
        posting_buf.emplace(move(posting));

        // update buf_size_
        buf_size_ += posting.term.size() + posting.doc_ids.size() * sizeof(uint32_t)
                + posting.frequencies.size() * sizeof(uint64_t);
    }
    return ret;
}

int BufferNode::pop_posting(Posting & dest) {
    int ret = 0;
    if (posting_buf.empty()) {
        ret = load_from_disk();
        if (ret != 0) {
            return ret;
        }
    }
    dest = posting_buf.front();
    posting_buf.pop(); // pop_front

    // update buf_size;

    buf_size_ -= dest.term.size() + dest.doc_ids.size() * sizeof(uint32_t)
                 + dest.frequencies.size() * sizeof(uint64_t);

    return 0;
}

// Invert list builder

InvertedListBuilder::InvertedListBuilder(string output_file,
                                         string lexicon_file,
                                         uint32_t in_buf_limit,
                                         uint32_t out_block_size,
                                         uint32_t out_chunk_size,
                                         shared_ptr<BlockEncoder> p_block_encoder,
                                         shared_ptr<BlockEncoder> p_out_encoder)
    : output_file_(std::move(output_file))
    , lexicon_file_(std::move(lexicon_file))
    , in_buf_limit_(in_buf_limit)
    , out_chunk_limit_(out_chunk_size)
    , out_buf_limit_(out_block_size)
    , out_buf_size_(0)
    , out_cursor_(0)
    , p_inter_block_encoder_(p_block_encoder)
    , p_out_encoder_(p_out_encoder)
    , p_lexicon_encoder_(new LexiconEncoder())
{
    cout << output_file_ << endl;
    out_fd = fopen(output_file_.c_str(), "w");
    assert (out_fd != nullptr);
}

InvertedListBuilder::~InvertedListBuilder() {
    fclose(out_fd);
}

int InvertedListBuilder::execute(const vector<Document> & doc_table,
                                 unordered_map<string, LexiconEntry> & lexicons) {
    int ret = 0;
    //unordered_map<string, LexiconEntry> lexicons;

    // calculate average document length
    float doc_size_sum = 0.0;
    for (const auto & doc : doc_table) {
        doc_size_sum += doc.doc_size_;
    }
    float average_doc_size = doc_size_sum / (1.0 * doc_table.size());

    // init block buffers
    vector<BufferNode> blocks;
    for (auto const & block_file : block_files_) {
        blocks.emplace_back(block_file, p_inter_block_encoder_, in_buf_limit_);
    }

    // compare function for the priority queue
    auto cmp = [&](const int & lhs, const int & rhs) {
        return blocks[lhs] < blocks[rhs];
    };
    priority_queue<int, vector<int>, decltype(cmp)> pq(cmp);
    for (int i = 0; i < (int)blocks.size(); ++i) {
        pq.push(i);
    }

    vector<Posting> output_buf;
    // merge sort
    while(!pq.empty()) {
        int idx = pq.top();
        pq.pop();
        auto & block = blocks[idx];
        // if this block is empty,
        if (block.cache_empty()) {
            if (block.block_empty()) {
                continue;
            }
            block.load_from_disk();
        }
        Posting posting;
        block.pop_posting(posting);

//        for (int i = 0; i < (int)posting.doc_ids.size(); ++i) {
//            float k1 = 1.2;
//            float b = 0.75;
//            float fdt = (float)posting.frequencies[i];
//            uint32_t doc_id = posting.doc_ids[i];
//            float K = k1 * ((1 - b) + b * doc_table[doc_id].doc_size_ / average_doc_size);
//            float score = log(((float)doc_table.size() - (float)lexicons[posting.term].length_ + 0.5)
//                            / ((float)lexicons[posting.term].length_ + 0.5)) * (k1 + 1) * fdt / (K + fdt);
//            posting.score_bm25.push_back(score);
//        }

        // append to output
        if (output_buf.empty() || output_buf.back().term != posting.term) {

            // update out_buf_size_

            out_buf_size_ += posting.term.size()
                    + posting.doc_ids.size() * sizeof(uint32_t)
                    + posting.frequencies.size() * sizeof(uint64_t);

            output_buf.emplace_back(move(posting));

        } else {
            // update out_buf_size_
            out_buf_size_ += posting.doc_ids.size() * sizeof(uint32_t)
                             + posting.frequencies.size() * sizeof(uint64_t);

            // merge
            Posting & pred = output_buf.back();
            if (pred.doc_ids.back() == posting.doc_ids.front()) {
                pred.frequencies.back() += posting.frequencies.front();
                pred.doc_ids.insert(pred.doc_ids.end(),
                                    posting.doc_ids.begin() + 1, posting.doc_ids.end());
                pred.frequencies.insert(pred.frequencies.end(),
                                        posting.frequencies.begin() + 1, posting.frequencies.end());
                cout << "Terms across intermediate blocks" << endl;
            } else {
                pred.doc_ids.insert(pred.doc_ids.end(),
                                    posting.doc_ids.begin(), posting.doc_ids.end());
                pred.frequencies.insert(pred.frequencies.end(),
                                        posting.frequencies.begin(), posting.frequencies.end());
            }
        }

        // put non-empty node into the heap
        if (!block.cache_empty() || !block.block_empty()) {
            pq.push(idx);
        }

        // check output buf size and dump
        if (out_buf_size_ >= out_buf_limit_) {
            ret = dump_output_block(output_buf, lexicons);
            if (ret != 0) {
                cerr << "Dump buffer into output file error: " << ret << endl;
                abort();
            }
        }
    }

    cout << "Left one block." << endl;

    // dump remaining inverted list in the buffer
    if (!output_buf.empty()) {
        ret = dump_output_block(output_buf, lexicons);
        if (ret != 0) {
            cerr << "Dump buffer into output file error: " << ret <<  endl;
            abort();
        }
    }

    cout << "Sorting finished" << endl;

    // dump lexicon table
    if (dump_lexicon_entries(lexicons) != 0) {
        cerr << "Dump lexicon table error: " << ret << endl;
        return -1;
    }
    cout << "finish lexicon" << endl;
    return ret;
}

int InvertedListBuilder::set_block_files(const vector<std::string> & block_files) {
    block_files_.insert(block_files_.end(),
                        block_files.begin(),
                        block_files.end());
    return 0;
}

int InvertedListBuilder::dump_output_block(vector<Posting> &buf,
                                           unordered_map<string, LexiconEntry> & lexicon) {
    int ret = 0;
    if (buf.empty()) {
        return -1;
    }

    // encode
    BlockHeader header = {0};

    uint32_t header_size;
    BitStream header_stream;
    BitStream body_stream;

    for (auto const & posting: buf) {
        // check chunk length
        uint64_t cur = 0;

        for ( ; cur < posting.doc_ids.size(); ) {
            uint64_t next = cur + out_chunk_limit_ > posting.doc_ids.size()
                    ? posting.doc_ids.size() : cur + out_chunk_limit_;

            // split into chunk
            Posting chunk;
            for (uint64_t i = cur; i < next; ++i) {
                chunk.doc_ids.push_back(posting.doc_ids[i]);
                chunk.frequencies.push_back(posting.frequencies[i]);
            }

            auto ptr = lexicon.find(posting.term);
            if (ptr->second.block_cursor_ == 0) {
                ptr->second.block_cursor_ = out_cursor_;
                ptr->second.chunk_offset_ = header.chunk_count;
            }

            // encode chunk
            p_out_encoder_->encode_chunk(chunk, body_stream);

            // update header
            header.last_chunk_length = chunk.doc_ids.size();
            ++header.chunk_count;
            header.chunk_offsets.push_back(body_stream.get_length());
            header.last_doc_ids.push_back(chunk.doc_ids.back());

            // update cur
            cur = next;
        }

    }
    p_out_encoder_->encode_block_header(header, header_stream);
    header_size = header_stream.get_length();

    // write to output file
    fwrite(&header_size, sizeof(uint32_t), 1, out_fd);
    fwrite(header_stream.get_stream(), sizeof(uint8_t), header_stream.get_length(), out_fd);
    fwrite(body_stream.get_stream(), sizeof(uint8_t), body_stream.get_length(), out_fd);

    out_cursor_ += sizeof(header_size) + header_stream.get_length() + body_stream.get_length();

    // update out_buf_size_
    out_buf_size_ = 0;
    buf.clear();

    return ret;
}

int InvertedListBuilder::dump_lexicon_entries(unordered_map<string, LexiconEntry> &lexicons) {
    // encode
    BitStream header_s;
    BitStream body_s;
    cout << "start to dump lexicon" << endl;
    if (p_lexicon_encoder_->encode(lexicons, header_s, body_s) != 0) {
        return -1;
    }
    cout << "Finish encode lexicon" << endl;
    cout << lexicon_file_ << endl;
    if (p_lexicon_encoder_->dump(header_s, body_s, lexicon_file_) != 0) {
        return -1;
    }
    cout << "Finish dump lexicon file" << endl;
    return 0;
}
