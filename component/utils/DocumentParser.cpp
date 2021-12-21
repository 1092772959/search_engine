//
// Created by xiuwenli on 12/8/21.
//

#include "DocumentParser.h"
#include <iostream>

using namespace engine::builder;
using namespace std;

DocumentParser::DocumentParser() {
    ascii_delimiters_ = {' ', ',', '.', ';', ':', '?', '[', ']', '{', '}',
                  '(', ')', '-', '/', '+', '*', '&', '^', '"', '\'',
                  '@', '!', '#', '%', '>', '<', '|', '~', '\n'};
    unicode_delimiters_ = {
            "，", "。", "（", "）", "·", "～", "？", "》", "《", "｜",
            "、", "；", "：", "$", "·", "「", "」", "【", "】",
            "“", "’"};
}

int DocumentParser::parse(const std::string &document,
                          std::vector<std::string> &terms) {
    string term;
    const char mask = 0x80;

    for (size_t pos = 0; pos < document.size(); ) {
        // check unicode
        const char c = document[pos];
        if ((c & mask) == 0) { // one byte utf8
            if (ascii_delimiters_.find(c) != ascii_delimiters_.end()) {
                if (!term.empty()) {
                    terms.push_back(term);
                    term.clear();
                }
            } else {
                term.push_back(c);
            }
            pos++;
        } else { // n-bytes utf8
            // get length of utf8
            size_t length = 0;
            if ((c & 0xF0) == 0xF0) { // 4
                length = 4;
            } else if ((c & 0xE0) == 0xE0) {
                length = 3;
            } else if ((c & 0xC0) == 0xC0) {
                length = 2;
            } else {
                cerr << "Invalid utf-8" << endl;
                abort();
            }
            string word;
            for (size_t j = 0; j < length; ++j) {
                word.push_back(document[pos + j]);
            }
            if (unicode_delimiters_.find(word) != unicode_delimiters_.end()) {
                if (!term.empty()) {
                    terms.push_back(term);
                    term.clear();
                }
            } else {
                term.insert(term.end(), word.begin(), word.end());
            }
            pos += length;
        }
    }
    if (!term.empty()) {
        terms.push_back(term);
    }
    return 0;
}

