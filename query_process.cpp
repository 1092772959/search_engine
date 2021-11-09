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
              "inverted list");
DEFINE_string(lexicon_file, "./data/output/msmarco-docs.trec_lexicon_table",
              "lexicon file");
DEFINE_string(doc_table_file, "./data/output/msmarco-docs.trec_doc_table",
              "doc table file");
DEFINE_int64(chunk_length, 128,
             "Max length for a chunk, which is the minimum unit to compress");
DEFINE_uint32(MAX_DOC_ID, UINT32_MAX, "To denote an unreachable doc id");
DEFINE_string(mongo_db, "test", "name of the database");
DEFINE_string(mongo_collection, "msmarco", "name of the collection");

int main() {
    auto begin_ts = steady_clock::now();
    QueryExecution qp(FLAGS_inverted_list_file,
                   FLAGS_lexicon_file,
                   FLAGS_doc_table_file);
    auto init_elapse = duration_cast<seconds>(steady_clock::now() - begin_ts).count();
    cout << "Init elapse: " << init_elapse << " s" << endl;
    qp.conjunctive_query("apple NYC");
    qp.disjunctive_query("apple NYC");
    return 0;
}