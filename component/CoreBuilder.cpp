//
// Created by xiuwenli on 10/2/21.
//

#include "CoreBuilder.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <filesystem>
#include <chrono>

using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;
using namespace std::chrono;

CoreBuilder::CoreBuilder(string filename, string inter_dir, string output_dir, int block_mode)
    : filename(move(filename))
{
    shared_ptr<BlockEncoder> p_encoder;
    switch(block_mode) {
        case 0:
            p_encoder = make_shared<BlockBinaryEncoder>(MAX_CHUNK_SIZE);
            //p_encoder = make_shared<BlockBinaryEncoder>(FLAGS_chunk_length);
            break;
        default:
            p_encoder = make_shared<BlockPlainEncoder>(MAX_CHUNK_SIZE);
            //p_encoder = make_shared<BlockPlainEncoder>(FLAGS_chunk_length);
            break;
    }
    p_posting_builder_ = make_shared<PostingsBuilder>(
            inter_dir, filename, MAX_BLOCK_SIZE, p_encoder);
//    p_posting_builder_ = make_shared<PostingsBuilder>(
//            inter_dir, filename, FLAGS_inter_buffer_size, p_encoder);


    // for final output
    shared_ptr<BlockEncoder> p_out_encoder =
            make_shared<BlockBinaryEncoder>(MAX_CHUNK_SIZE);

//    shared_ptr<BlockEncoder> p_out_encoder =
//            make_shared<BlockBinaryEncoder>(FLAGS_chunk_length);

    fs::path inverted_list_output = output_dir / fs::path("inverted_list");
    fs::path lexicon_table = output_dir / fs::path("lexicon_table");
    fs::path doc_table = output_dir / fs::path("doc_table");
    doc_table_file_ = doc_table;

    p_inverted_builder = make_shared<InvertedListBuilder>(
            inverted_list_output, lexicon_table, MAX_CACHE_SIZE, OUT_BLOCK_SIZE, OUT_CHUNK_SIZE,
            p_encoder, p_out_encoder);

//    p_inverted_builder = make_shared<InvertedListBuilder>(
//            inverted_list_output, lexicon_table,
//            FLAGS_merge_cache_size, FLAGS_output_buffer_size, FLAGS_chunk_length,
//            p_encoder, p_out_encoder);
}

void CoreBuilder::run() {
    std::ifstream is(filename, ifstream::binary);
    uint64_t doc_id_cur = 0;
    auto begin_ts = steady_clock::now();
    int ret;
    while (!is.eof()) {
        auto pre_ts = steady_clock::now();
        ParserResult result;
        int code = parser.parse(is, result, doc_id_cur,
                                p_posting_builder_);
        if (code == 1) {
            cout << "End of the file" << endl;
            break;
        } else if (code < 0) {
            abort();
        }
        //cout << "Counter: " << doc_id_cur << " Url: " << result.url_ << endl;
        if (doc_id_cur % 1000 == 0) {
            auto cur_ts = steady_clock::now();
            cout << "Counter: " << doc_id_cur
                << " elapsed: " << duration_cast<seconds>(cur_ts - begin_ts).count() << " s"
                << endl;
        }

        auto parser_elapse = duration_cast<microseconds>(
                steady_clock::now() - pre_ts).count();

        pre_ts = steady_clock::now();
        doc_table_builder_.addDoc(
                Document(doc_id_cur, result.url_, result.content_size_));
        //p_posting_builder_->add_postings(doc_id_cur, result.terms);

        auto cur_ts = steady_clock::now();
//        cout << "Counter: " << doc_id_cur << ", parser elapsed: " << parser_elapse << " ms"
//             << " add posting elapsed: " << chrono::duration_cast<chrono::microseconds>(cur_ts - pre_ts).count() << " ms"
//             << endl;

        // generate a new doc id
        ++doc_id_cur;
//        if (doc_id_cur == 100000) {
//            break;
//        }
    }
    is.close();

    // flush the remaining index postings to the disk

    ret = p_posting_builder_->dump();
    if (ret != 0) {
        cerr << "Dump postings failed" << endl;
        abort();
    }

    auto posting_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Create intermediate postings elapse: " << posting_elapse << " sec"
        << endl;

    begin_ts = steady_clock::now();

    doc_table_builder_.dump(doc_table_file_);

    // disk-based merge sort
    vector<string> block_names;
    p_posting_builder_->get_block_names(block_names);
    for (auto & file: block_names) {
        cout << file << endl;
    }
    p_inverted_builder->set_block_files(block_names);

    p_inverted_builder->execute();
    auto merge_and_dump_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Merge and dump elapse: " << merge_and_dump_elapse << " sec" << endl;

    // merge intermediate blocks
}