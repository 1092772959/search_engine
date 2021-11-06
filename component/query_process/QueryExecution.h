//
// Created by xiuwenli on 10/31/21.
//

#ifndef INDEX_BUILDER_QUERYEXECUTION_H
#define INDEX_BUILDER_QUERYEXECUTION_H
#include <string>
#include <unordered_map>
#include "LexiconBuilder.h"
#include <filesystem>

namespace engine::process {

    struct InvertedList {
        std::string term;

    };


    class QueryExecution {
    public:
        QueryExecution(std::string inverted_list_file,
                       std::string lexicon_file);
        void conjunctive_query(const std::string & query);
        void disjunctive_query(const std::string & query);

        ~QueryExecution();
    private:
        FILE * fd_inv_list_;
        const std::string inverted_list_file_;
        std::unordered_map<std::string, builder::LexiconEntry> lexicons_;
    };

}

#endif //INDEX_BUILDER_QUERYEXECUTION_H
