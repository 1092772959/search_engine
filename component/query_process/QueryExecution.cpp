//
// Created by xiuwenli on 10/31/21.
//

#include "QueryExecution.h"
#include "LexiconBuilder.h"
#include <utility>
#include <iostream>
#include <gflags/gflags.h>
#include <chrono>

DECLARE_int64(chunk_length);
DECLARE_uint32(MAX_DOC_ID);

using namespace engine::process;
using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;
using namespace std::chrono;

QueryExecution::QueryExecution(string inverted_list_file,
                               string lexicon_file,
                               string doc_table_file)
    : inverted_list_file_(move(inverted_list_file))
    , p_block_codec_(new BlockBinaryEncoder())
{
    assert(fs::exists(inverted_list_file_));
    assert(fs::exists(lexicon_file));

    // open inverted list file descriptor
    fd_inv_list_ = fopen(inverted_list_file_.c_str(), "r");
    assert(fd_inv_list_ != nullptr);
    delimiters_ = {' ', ',', '.', ';', ':', '"', '\'', '?', '/', '\\'};

    // load doc table
    DocTableBuilder doc_table_codec;
    doc_table_codec.load(std::move(doc_table_file), doc_table_);

    // precompute average doc length
    uint64_t sum = 0;
    for (const auto & doc : doc_table_) {
        sum += doc.doc_size_;
    }
    average_doc_length_ = sum * 1.0 / doc_table_.size();

    // load lexicon table
    LexiconEncoder encoder;
    LexiconHeader header;
    BitStream body;
    encoder.load(lexicon_file, header, body);
    encoder.decode(header, body, lexicons_);

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

    // load meta data for this term
    uint32_t count = 0;
    uint32_t chunk_index = lp.entry_.chunk_offset_;
    uint64_t block_body_offset = lp.entry_.block_cursor_ + header->header_size;

    while (count < lp.entry_.length_) {
        if (chunk_index == 0) {
            lp.chunk_byte_offsets.push_back(block_body_offset);
            lp.chunk_byte_lengths.push_back(header->chunk_offsets[chunk_index]);
        } else {
            lp.chunk_byte_offsets.push_back(block_body_offset + header->chunk_offsets[chunk_index - 1]);
            lp.chunk_byte_lengths.push_back(header->chunk_offsets[chunk_index]
                - header->chunk_offsets[chunk_index - 1]);
        }
        lp.last_doc_ids.push_back(header->last_doc_ids[chunk_index]);

        if (chunk_index == header->chunk_count - 1) { // last chunk
            cout << "Reach the end of the block" << endl;
            count += header->last_chunk_length;
            if (count == lp.entry_.length_) {
                break;
            } else { // next block
                cout << "Load from next block for term: " << term << endl;
                size_t next_block_offset = lp.entry_.block_cursor_ +
                        header->header_size + header->block_size; // size of a whole block
                if (loadBlockHeader(next_block_offset) != 0) {
                    cerr << "Load next block header during across-block list: " << term << endl;
                    return -1;
                }
                // move to the next block header
                header = &(block_header_dict_.find(next_block_offset)->second);
                block_body_offset = next_block_offset + header->header_size;
                chunk_index = 0;
            }
        } else {
            count += (int)FLAGS_chunk_length;
            chunk_index++;
        }
    }
    return 0;
}

int QueryExecution::nextGEQ(InvertedList &lp, uint32_t k, uint32_t & doc_id) {
    // binary search in the cache
    if (!lp.chunk_cache_.doc_ids.empty()) {
        auto it = lower_bound(lp.chunk_cache_.doc_ids.begin(), lp.chunk_cache_.doc_ids.end(),
                    k);
        if (it != lp.chunk_cache_.doc_ids.end()) {
            doc_id = *it;
            return 0;
        }
    }
    // search on the last_doc_ids
    int chunk_idx = lower_bound(lp.last_doc_ids.begin(), lp.last_doc_ids.end(), k)
             - lp.last_doc_ids.begin();

    if (chunk_idx == (int)lp.last_doc_ids.size()) {
        // has no id k
        doc_id = FLAGS_MAX_DOC_ID;
    } else {
        // load the chunk in the cache
        loadChunk(lp, chunk_idx);
        // recursive call
        nextGEQ(lp, k, doc_id);
    }
    return 0;
}

int QueryExecution::getFreq(InvertedList &lp, uint32_t doc_id, uint64_t & freq) {
    int pos = lower_bound(lp.chunk_cache_.doc_ids.begin(), lp.chunk_cache_.doc_ids.end(),
                          doc_id) - lp.chunk_cache_.doc_ids.begin();
    if (pos == (int)lp.chunk_cache_.doc_ids.size()) {
        return -1;
    }
    freq = lp.chunk_cache_.frequencies[pos];
    return 0;
}

void QueryExecution::clearListCache(InvertedList &lp) {
    lp.chunk_cache_.doc_ids.clear();
    lp.chunk_cache_.frequencies.clear();
}

int QueryExecution::loadChunk(InvertedList & lp, int index) {
    if (index < 0 && index >= (int)lp.chunk_byte_offsets.size()) {
        cerr << "Invalid chunk index to load: " << index << endl;
        abort();
    }
    // clean the cache
    clearListCache(lp);
    // load new chunk
    fseek(lp.fd_inv_list_, lp.chunk_byte_offsets[index], SEEK_SET);
    char * buf = new char[lp.chunk_byte_lengths[index]];
    fread(buf, sizeof(uint8_t), lp.chunk_byte_lengths[index], lp.fd_inv_list_);
    // decode
    BitStream src;
    src.append_byte_array(buf, lp.chunk_byte_lengths[index]);
    delete[] buf;
    p_block_codec_->decode_chunk(src, lp.chunk_cache_);
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

    header.header_size = header_size + sizeof(uint32_t);
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

    uint32_t i = 0;
    for (; lp.chunk_cache_.doc_ids.size() < lp.entry_.length_; ++i) {
        fseek(lp.fd_inv_list_, lp.chunk_byte_offsets[i], SEEK_SET);
        char * buf = new char[lp.chunk_byte_lengths[i]];
        fread(buf, sizeof(uint8_t), lp.chunk_byte_lengths[i], lp.fd_inv_list_);
        // decode
        BitStream src;
        src.append_byte_array(buf, lp.chunk_byte_lengths[i]);
        delete[] buf;
        Posting chunk;
        p_block_codec_->decode_chunk(src,  chunk);
        lp.chunk_cache_.doc_ids.insert(lp.chunk_cache_.doc_ids.end(),
                                       chunk.doc_ids.begin(), chunk.doc_ids.end());
        lp.chunk_cache_.frequencies.insert(lp.chunk_cache_.frequencies.end(),
                                           chunk.frequencies.begin(), chunk.frequencies.end());
    }
    return 0;
}

int QueryExecution::closeList(InvertedList &lp) {
    fclose(lp.fd_inv_list_);
    return 0;
}

void QueryExecution::word_segmentation(const string &query, vector<string> & terms) {
    string term;
    for (const char & c : query) {
        if (delimiters_.find(c) != delimiters_.end()) {
            if (!term.empty()) {
                terms.push_back(term);
                term.clear();
            }
        } else {
            term.push_back(c);
        }
    }
    if (!term.empty()) {
        terms.push_back(term);
    }
}

int QueryExecution::conjunctive_query(const string &query) {
    vector<string> terms;
    auto begin_ts = steady_clock::now();
    // word seg
    word_segmentation(query, terms);

    if (terms.size() == 0) {
        return -1;
    };

    vector<InvertedList> lps;
    for (auto & term : terms) {
        InvertedList lp;
        openList(term, lp);
        lps.emplace_back(move(lp));
    }
    uint32_t did = 0;
    int num = lps.size();
    if (num == 0) {
        cerr << "Cannot get result from query: " << query << endl;
        return -1;
    }
    auto cmp = [](const QueryResult* lhs, const QueryResult *rhs) {
        return lhs->score_sum > rhs->score_sum;
    };
    priority_queue<QueryResult *, vector<QueryResult *>, decltype(cmp)> pq(cmp);
    const int top_limit = 12;
    while (did < FLAGS_MAX_DOC_ID) {

        nextGEQ(lps[0], did, did);
        uint32_t d = 0;
        for (int i = 1; i < num; ++i) {
            nextGEQ(lps[i], did, d);
            if (d == did) {
                continue;
            } else {
                break;
            }
        }
        if (d > did) {
            did = d;
        } else {
            vector<uint64_t> freqs(num);
            QueryResult * result = new QueryResult();
            result->doc_id = did;
            for (int i = 0; i < num; ++i) {
                if (getFreq(lps[i], did, freqs[i]) != 0) {
                    cerr << "Internal error when getting frequency" << endl;
                    abort();
                }

                // compute bm25
                float score = compute_score(lps[i].term, did, freqs[i]);
//                float k1 = 1.2;
//                float b = 0.75;
//                float fdt = (float)freqs[i];
//                float K = k1 * ((1 - b) + b * doc_table_[did].doc_size_ / average_doc_length_);
//                float score = log(((float)doc_table_.size() - (float)lexicons_[lps[i].term].length_ + 0.5)
//                                  / ((float)lexicons_[lps[i].term].length_ + 0.5)) * (k1 + 1) * fdt / (K + fdt);
                result->score_sum += score;
                result->terms.push_back(lps[i].term);
                result->scores.push_back(score);
                result->freqs.push_back(freqs[i]);
            }

            if (pq.size() < top_limit) {
                pq.push(result);
            } else if (pq.size() == top_limit && pq.top()->score_sum < result->score_sum) {
                QueryResult * ptr = pq.top();
                delete ptr;
                pq.pop();
                pq.push(result);
            } else {
                delete result;
            }
            did++;
        }
    }

    vector<QueryResult *> results;
    while (!pq.empty()) {
        results.push_back(pq.top());
        pq.pop();
    }

    auto search_elapse = duration_cast<milliseconds>(steady_clock::now() - begin_ts).count();
    cout << "Search elapse: " << search_elapse << " ms" << endl;

    cout << "-----------------------------------" << endl;
    for (auto rit = results.rbegin(); rit != results.rend(); ++rit) {

        auto ptr = *rit;
        cout << "Doc id: " << ptr->doc_id << ", score sum: " << ptr->score_sum << endl;
        for (int i = 0; i < (int)ptr->scores.size(); ++i){
            cout << ptr->terms[i] << " | score: " << ptr->scores[i]
                << ", frequency: " << ptr->freqs[i] << endl;
        }
        cout << "-----------------------------------" << endl;
    }

    for (int i = 0; i < num; ++i) {
        closeList(lps[i]);
    }
    return 0;
}

int QueryExecution::disjunctive_query(const string &query) {
    // word seg
    auto begin_ts = steady_clock::now();

    vector<string> terms;
    word_segmentation(query, terms);

    // term-at-a-time
    unordered_map<uint32_t, QueryResult> union_table;
    for (auto & term : terms) {
        InvertedList lp;
        loadAll(term, lp);
        for (size_t i = 0; i < lp.chunk_cache_.doc_ids.size(); ++i) {
            const uint32_t did = lp.chunk_cache_.doc_ids[i];
            const uint64_t freq = lp.chunk_cache_.frequencies[i];
            float score = compute_score(term, did, freq);
            if (auto ptr = union_table.find(did); ptr == union_table.end()) {
                QueryResult result;
                result.score_sum = score;
                result.doc_id = did;
                result.freqs = {freq};
                result.terms = {term};
                union_table[did] = result;
            } else {
                ptr->second.score_sum += score;
                ptr->second.freqs.push_back(freq);
                ptr->second.terms.push_back(term);
            }
        }
        // close
        closeList(lp);
    }

    // chose top k
    const int top_limit = 20;
    auto cmp = [](const QueryResult* lhs, const QueryResult *rhs) {
        return lhs->score_sum > rhs->score_sum;
    };

    priority_queue<QueryResult *, vector<QueryResult *>, decltype(cmp)> pq(cmp);
    for (auto & [did, qr] : union_table) {
        if (pq.size() < top_limit) {
            pq.push(&qr);
        } else if (pq.size() == top_limit && pq.top()->score_sum < qr.score_sum) {
            pq.pop();
            pq.push(&qr);
        }
    }

    vector<QueryResult*> results;
    while (!pq.empty()) {
        results.push_back(pq.top());
        pq.pop();
    }

    auto search_elapse = duration_cast<milliseconds>(steady_clock::now() - begin_ts).count();
    cout << "Search elapse: " << search_elapse << " ms" << endl;

    cout << "-----------------------------------" << endl;
    for (auto rit = results.rbegin(); rit != results.rend(); ++rit) {

        auto ptr = *rit;
        cout << "Doc id: " << ptr->doc_id << ", score sum: " << ptr->score_sum << endl;
        for (int i = 0; i < (int)ptr->scores.size(); ++i) {
            cout << ptr->terms[i] << " | score: " << ptr->scores[i]
                 << ", frequency: " << ptr->freqs[i] << endl;
        }
        cout << "-----------------------------------" << endl;
    }
    return 0;
}

float QueryExecution::compute_score(const std::string & term,
                                    uint32_t did,
                                    uint64_t freq) {
    float k1 = 1.2;
    float b = 0.75;
    float fdt = (float)freq;
    float K = k1 * ((1 - b) + b * doc_table_[did].doc_size_ / average_doc_length_);
    float score = log(((float)doc_table_.size() - (float)lexicons_[term].length_ + 0.5)
                      / ((float)lexicons_[term].length_ + 0.5)) * (k1 + 1) * fdt / (K + fdt);
    return score;
}

