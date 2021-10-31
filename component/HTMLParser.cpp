//
// Created by xiuwenli on 10/2/21.
//

#include "HTMLParser.h"

#include <algorithm>
#include <sstream>
#include <utility>
#include <chrono>

using namespace engine::builder;
using namespace std;


HTMLParser::HTMLParser() {
    delimiters = {' ', ',', '.', ';', ':', '?', '[', ']', '{', '}',
                  '(', ')', '-', '/', '+', '*', '&', '^', '"', '\''};
}

int HTMLParser::parse(istream & in, ParserResult & result,
                      uint32_t doc_id,
                      const shared_ptr<PostingsBuilder>& p_builder) {

    //Result trecppResult = text::read_subsequent_record(in);

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
                    string term;
                    bool alpha_num = false;
                    for (char &c: tmp) {
                        if (delimiters.find(c) != delimiters.end()) {
                            if (!term.empty()) {
                                //++result.posting_tbl[term];
                                if (alpha_num) {
                                    //result.terms.emplace_back(move(term));
                                    p_builder->add_posting(doc_id, term);
                                }
                                term.clear();
                                alpha_num = false;
                            }
                        } else {
                            if (isalnum(c)) {
                                alpha_num = true;
                            }
                            term.push_back(c);
                        }
                    }
                    if (!term.empty()) {
                        //++result.posting_tbl[term];
                        //result.terms.emplace_back(move(term));
                        p_builder->add_posting(doc_id, term);
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

ParserResult::ParserResult(string doc_id, string url, size_t content_size):
    doc_id_(std::move(doc_id)),
    url_(std::move(url)),
    content_size_(content_size) { }
