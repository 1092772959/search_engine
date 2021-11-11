//
// Created by xiuwenli on 11/7/21.
//

#include <iostream>
#include <gflags/gflags.h>
#include "query_process/QueryExecution.h"
#include <chrono>

using namespace std;
using namespace engine::builder;
using namespace engine::process;
using namespace std::chrono;

DEFINE_string(inverted_list_file, "./data/output/msmarco-docs.trec_inverted_list",
              "the path of the inverted list");
DEFINE_string(lexicon_file, "./data/output/msmarco-docs.trec_lexicon_table",
              "the path of the lexicon file");
DEFINE_string(doc_table_file, "./data/output/msmarco-docs.trec_doc_table",
              "the path of the doc table file");
DEFINE_int64(chunk_length, 128,
             "Max length for a chunk, which is the minimum unit to compress");
DEFINE_uint32(MAX_DOC_ID, UINT32_MAX, "To denote an unreachable doc id");
DEFINE_string(mongo_db, "test", "name of the database");
DEFINE_string(mongo_collection, "msmarco", "name of the collection");
DEFINE_uint32(list_caching_size, 1024, "The caching limit for each list");

int main(int argc, char * argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto begin_ts = steady_clock::now();
    QueryExecution qp(FLAGS_inverted_list_file,
                   FLAGS_lexicon_file,
                   FLAGS_doc_table_file);
    auto init_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Init elapse: " << init_elapse << " s" << endl;
    while (true) {
        string query;
        string mode;
        cout << "Enter query(Enter to exit)>";
        getline(cin, query);
        if (query.empty()) {
            break;
        }
        cout << "Conjunctive/Disconjunctive(0/others)>";
        getline(cin, mode);
        cout << "With snippet?(0 to drop)";
        string snippet;
        getline(cin, snippet);
        bool has_snippet = !(snippet == "0");
        if (mode == "0") {
            qp.conjunctive_query(query, has_snippet);
        } else {
            qp.disjunctive_query(query, has_snippet);
        }
    }
    return 0;
}