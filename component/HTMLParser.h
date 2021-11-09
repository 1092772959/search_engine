//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_HTMLPARSER_H
#define INDEX_BUILDER_HTMLPARSER_H
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "PostingsBuilder.h"

namespace engine::builder {

    struct ParserResult {
        std::string doc_id_;
        std::string url_;
        size_t doc_length_;
        std::vector<std::string> terms;
        std::string content;
        ParserResult (): doc_length_(0) {}
    };

    class HTMLParser {
    public:
        HTMLParser();
        int parse(std::istream & is, ParserResult & result, uint32_t doc_id);
    private:
        std::unordered_set<char> delimiters;
    };

} // namespace engine

#endif //INDEX_BUILDER_HTMLPARSER_H
