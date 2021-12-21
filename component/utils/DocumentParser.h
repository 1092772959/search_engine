//
// Created by xiuwenli on 12/8/21.
//

#ifndef INDEX_BUILDER_DOCUMENTPARSER_H
#define INDEX_BUILDER_DOCUMENTPARSER_H

#include <string>
#include <vector>
#include <unordered_set>

namespace engine::builder {

    class DocumentParser {
    public:
        DocumentParser ();
        static DocumentParser & get_instance() {
            static DocumentParser parser;
            return parser;
        }
        int parse(const std::string & document,
                         std::vector<std::string> & terms);
    private:
        std::unordered_set<char> ascii_delimiters_;
        std::unordered_set<std::string> unicode_delimiters_;
    };
}




#endif //INDEX_BUILDER_DOCUMENTPARSER_H
