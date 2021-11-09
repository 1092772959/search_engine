//
// Created by xiuwenli on 10/2/21.
//

#include "DocTableBuilder.h"
#include <fstream>
#include <iostream>
#include <vector>

using namespace engine::builder;
using namespace std;


const string Document::DOC_ID_KEY = "doc_id";
const string Document::URL_KEY = "url";
const string Document::DOC_LENGTH_KEY = "doc_length";
const string Document::CONTENT_KEY = "content";

Document::Document(const uint64_t &doc_id, const std::string &url, const uint64_t &doc_size):
    doc_id_(doc_id),
    url_(url),
    doc_size_(doc_size)
{ }

Document::Document() {

}


int DocTableBuilder::dump(string file_name, const vector<Document> & doc_table) {
    ofstream fout(file_name, ios::binary);
    for (auto & doc: doc_table) {
        fout << doc.doc_id_ << " ";
        fout << doc.url_ << " ";
        fout << doc.doc_size_ << " ";
        fout << endl;
    }
    fout.close();
    return 0;
}

int DocTableBuilder::load(std::string file_name, std::vector<Document> & doc_table) {
    ifstream fin(file_name, ios::binary);
    while (!fin.eof()) {
        Document doc;
        fin >> doc.doc_id_;
        fin >> doc.url_;
        fin >> doc.doc_size_;
        doc_table.push_back(doc);
    }
    return 0;
}
