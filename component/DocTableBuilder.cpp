//
// Created by xiuwenli on 10/2/21.
//

#include "DocTableBuilder.h"
#include <fstream>
#include <iostream>

using namespace engine::builder;
using namespace std;

Document::Document(const uint64_t &doc_id, const std::string &url, const uint64_t &doc_size):
    doc_id_(doc_id),
    url_(url),
    doc_size_(doc_size)
{ }

//int DocTableBuilder::addDoc(Document &&document) {
//    docTable.emplace_back(document);
//    return 0;
//}

int DocTableBuilder::dump(string file_name, const vector<Document> & doc_table) {
    std::ofstream fout(file_name + "_doc_tbl", ios::binary);
    for (auto & doc: doc_table) {
        fout << doc.doc_id_ << " ";
        fout << doc.url_ << " ";
        fout << doc.doc_size_ << " ";
        fout << endl;
    }
    fout.close();
    return 0;
}
