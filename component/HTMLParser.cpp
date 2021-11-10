//
// Created by xiuwenli on 10/2/21.
//

#include "HTMLParser.h"

#include <sstream>
#include <utility>
#include <chrono>
#include <iostream>

using namespace engine::builder;
using namespace std;

HTMLParser::HTMLParser() {
    delimiters = {' ', ',', '.', ';', ':', '?', '[', ']', '{', '}',
                  '(', ')', '-', '/', '+', '*', '&', '^', '"', '\'',
                  '@', '!', '#', '%', '>', '<', '|', '~', '\n'};
}

int HTMLParser::parse(istream & in,
                      ParserResult & result,
                      uint32_t doc_id) {
    unordered_set<string> unicode_delimiters = {
        "，", "。", "（", "）", "·", "～", "？", "》", "《", "｜",
        "、", "；", "：", "$", "·", "「", "」", "【", "】",
        "“","’"
    };

    int status = 0;
    int cur = 0;
    bool find_url = false;
    for(string tmp ;getline(in, tmp); ) {
        ++cur;
        if (tmp.empty()) {
            continue;
        }
        cur += tmp.size();
        if (status == 0) {
            if (tmp.rfind("<DOC>", 0) != 0) {
                return -1;
            }
            status = 1;
        } else if (status == 1) {
            if (tmp.rfind("<DOCNO>", 0) != 0) {
                return -1;
            }
            status = 2;
        } else if (status == 2) {
            if (tmp.rfind("<TEXT>", 0) != 0) {
                return -1;
            }
            status = 3;
        } else if (status == 3) {
            if (tmp.rfind("</TEXT>", 0) == 0) {
                status = 4;
            } else {
                if (!find_url) {
                    result.url_ = tmp;
                    find_url = true;
                } else {
                    result.content.insert(result.content.end(),
                                          tmp.begin(), tmp.end());
                    result.content.push_back('\n');

                    string term;
                    bool alpha_num = false;
                    const char mask = 0x80;

                    for (size_t pos = 0; pos < tmp.size(); ) {
                        // check unicode
                        const char c = tmp[pos];
                        if ((c & mask) == 0) { // one byte utf8
                            if (delimiters.find(c) != delimiters.end()) {
                                if (!term.empty()) {
//                                    if (alpha_num) {
//                                    }
                                    result.terms.push_back(term);
                                    ++result.doc_length_;
                                    term.clear();
//                                    alpha_num = false;
                                }
                            } else {
//                                if (isalnum(c)) {
//                                    alpha_num = true;
//                                }
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
                                word.push_back(tmp[pos + j]);
                            }
                            if (unicode_delimiters.find(word) != unicode_delimiters.end()) {
                                if (!term.empty()) {
                                    result.terms.push_back(term);
                                    ++result.doc_length_;
                                    term.clear();
                                }
                            } else {
                                term.insert(term.end(), word.begin(), word.end());
                            }
                            pos += length;
                        }
                    }
                    if (!term.empty()) {
                        result.terms.push_back(term);
                        ++result.doc_length_;
                    }
                }
            }
        } else {
            if (tmp.rfind("</DOC>", 0) != 0) {
                return -1;
            }
            break;
        }
    }

    return 0;
}
