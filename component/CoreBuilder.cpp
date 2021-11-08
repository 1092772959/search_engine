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


void CoreBuilder::run(const std::string & filename,
                      const std::string & inter_dir,
                      const std::string & output_dir,
                      int block_mode) {

    fs::path fs_input_file(filename);
    string file_base = fs_input_file.filename();

    shared_ptr<BlockEncoder> p_encoder;
    vector<Document> doc_table;
    DocTableBuilder doc_table_builder;

    fs::path doc_table_file = output_dir / fs::path(file_base + "_doc_table");
    fs::path lexicon_inter_file = output_dir / fs::path(file_base + "_inter_lexicon_table");

    switch(block_mode) {
        case 0:
            p_encoder = make_shared<BlockBinaryEncoder>();
            break;
        default:
            p_encoder = make_shared<BlockPlainEncoder>();
            break;
    }
    PostingsBuilder posting_builder(inter_dir, filename, FLAGS_inter_buffer_size, p_encoder);

    std::ifstream is(filename, ifstream::binary);
    uint64_t doc_id_cur = 0;
    unordered_map<string, LexiconEntry> lexicon_table;
    HTMLParser parser;
    auto begin_ts = steady_clock::now();
    int ret = 0;
    while (!is.eof()) {
        auto pre_ts = steady_clock::now();
        ParserResult result;
        int code = parser.parse(is, result, doc_id_cur);
        if (code == 1) {
            cout << "End of the file" << endl;
            break;
        } else if (code < 0) {
            abort();
        } else if (result.url_.empty()) {
            break;
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

        // update doc table
        doc_table.emplace_back(doc_id_cur, result.url_, result.doc_length_);
        // update posting buffer and lexicon table
        for (const auto & term : result.terms) {
            if (auto ptr = lexicon_table.find(term); ptr == lexicon_table.end()) {
                LexiconEntry entry;
                entry.length_ = 1;
                lexicon_table[term] = entry;
            } else {
                lexicon_table[term].length_++;
            }
            //posting_builder.add_posting(doc_id_cur, term);
        }

        auto cur_ts = steady_clock::now();
//        cout << "Counter: " << doc_id_cur << ", parser elapsed: " << parser_elapse << " ms"
//             << " add posting elapsed: " << chrono::duration_cast<chrono::microseconds>(cur_ts - pre_ts).count() << " ms"
//             << endl;

        // generate a new doc id
        ++doc_id_cur;
    }
    is.close();

    // flush the remaining index postings to the disk

    //ret = posting_builder.dump();
    if (ret != 0) {
        cerr << "Dump postings failed" << endl;
        abort();
    }

    auto posting_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Create intermediate postings elapse: " << posting_elapse << " sec"
        << endl;

    begin_ts = steady_clock::now();

    // dump doc table
    doc_table_builder.dump(doc_table_file, doc_table);

    // dump intermediate lexicon table
    LexiconEncoder lexicon_builder;
    lexicon_builder.dump_inter(lexicon_table, lexicon_inter_file);
}

void CoreBuilder::merge_sort(const string & src_file,
                             const string & doc_table_file,
                             const string & output_dir,
                             const vector<string> & block_names,
                             int block_mode) {

    fs::path fs_input_file(src_file);
    string file_base = fs_input_file.filename();

    fs::path inverted_list_output = output_dir / fs::path( file_base + "_inverted_list");
    fs::path lexicon_inter_file = output_dir / fs::path(file_base + "_inter_lexicon_table");
    fs::path lexicon_dest_file = output_dir / fs::path(file_base + "_lexicon_table");

    // codec for intermediate postings
    shared_ptr<BlockEncoder> p_encoder;
    switch(block_mode) {
        case 0:
            p_encoder = make_shared<BlockBinaryEncoder>();
            break;
        default:
            p_encoder = make_shared<BlockPlainEncoder>();
            break;
    }

    // codec for final output
    shared_ptr<BlockEncoder> p_out_encoder =
            make_shared<BlockBinaryEncoder>();

    auto begin_ts = steady_clock::now();
    // load lexicon table
    unordered_map<string, LexiconEntry> lexicon_table;
    LexiconEncoder lexicon_builder;
    lexicon_builder.load_inter(lexicon_inter_file, lexicon_table);

    auto load_lexicon_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Load lexicon table elapse: " << load_lexicon_elapse << " sec" << endl;


    // load doc table
    DocTableBuilder doc_table_builder;
    vector<Document> doc_table;
    doc_table_builder.load(doc_table_file, doc_table);

    InvertedListBuilder inverted_builder(
            inverted_list_output, lexicon_dest_file,
            FLAGS_merge_cache_size, FLAGS_output_buffer_size, FLAGS_chunk_length,
            p_encoder, p_out_encoder);

    // disk-based merge sort
    for (auto & file: block_names) {
        cout << file << endl;
    }
    inverted_builder.set_block_files(block_names);

    begin_ts = steady_clock::now();

    inverted_builder.execute(doc_table, lexicon_table);

    auto merge_and_dump_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Merge and dump elapse: " << merge_and_dump_elapse << " sec" << endl;
}
