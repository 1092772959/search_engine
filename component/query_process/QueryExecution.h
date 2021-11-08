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
        builder::LexiconEntry entry_;
        FILE * fd_inv_list_;
        builder::Posting chunk_cache_;
        std::vector<uint64_t> chunk_byte_offsets;
        std::vector<uint64_t> chunk_byte_lengths;
        std::vector<uint32_t> last_doc_ids;
    };


    class QueryExecution {
    public:
        QueryExecution(std::string inverted_list_file,
                       std::string lexicon_file);
        void conjunctive_query(const std::string & query);
        void disjunctive_query(const std::string & query);

        ~QueryExecution();
    private:
        int openList(const std::string & term, InvertedList & lp);
        int closeList(InvertedList & lp);
        int nextGEQ(InvertedList & lp, uint32_t k);
        int loadAll(const std::string & term,
                    InvertedList & lp);
        int loadBlockHeader(uint64_t header_offset);
    private:
        FILE * fd_inv_list_;
        const std::string inverted_list_file_;
        std::unordered_map<std::string, builder::LexiconEntry> lexicons_;
        std::shared_ptr<builder::BlockEncoder> p_block_codec_;
        std::unordered_map<uint64_t, builder::BlockHeader> block_header_dict_;
    };

}

#endif //INDEX_BUILDER_QUERYEXECUTION_H
