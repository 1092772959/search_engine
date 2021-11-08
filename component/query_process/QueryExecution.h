//
// Created by xiuwenli on 10/31/21.
//

#ifndef INDEX_BUILDER_QUERYEXECUTION_H
#define INDEX_BUILDER_QUERYEXECUTION_H
#include <string>
#include <unordered_map>
#include "LexiconBuilder.h"
#include <filesystem>
#include <unordered_set>

namespace engine::process {

    struct InvertedList {
        std::string term;
        FILE * fd_inv_list_;
        builder::LexiconEntry entry_;
        builder::Posting chunk_cache_;
        std::vector<uint64_t> chunk_byte_offsets;
        std::vector<uint64_t> chunk_byte_lengths;
        std::vector<uint32_t> last_doc_ids;
    };

    struct QueryResult {
        uint32_t doc_id;
        float score_sum;
        std::vector<std::string> terms;
        std::vector<float> scores;
        std::vector<uint64_t> freqs;
        QueryResult () : score_sum(0.0) {}
        bool operator < (const QueryResult & rhs) const {
            return score_sum > rhs.score_sum;
        }
    };


    class QueryExecution {
    public:
        QueryExecution(std::string inverted_list_file,
                       std::string lexicon_file,
                       std::string doc_table_file);
        int conjunctive_query(const std::string & query);
        int disjunctive_query(const std::string & query);

        ~QueryExecution();
    private:
        void word_segmentation(const std::string & query, std::vector<std::string> & terms);
        int openList(const std::string & term, InvertedList & lp);
        void clearListCache(InvertedList & lp);
        int closeList(InvertedList & lp);

        // get doc_id after k
        int nextGEQ(InvertedList & lp, uint32_t k, uint32_t & doc_id);
        int getFreq(InvertedList & lp, uint32_t doc_id, uint64_t & freq);

        int loadChunk(InvertedList & lp, int index);
        int loadAll(const std::string & term, InvertedList & lp);
        int loadBlockHeader(uint64_t header_offset);
        float compute_score(const std::string & term, uint32_t doc_id, uint64_t freq);
    private:
        FILE * fd_inv_list_;
        const std::string inverted_list_file_;
        std::unordered_map<std::string, builder::LexiconEntry> lexicons_;
        std::vector<builder::Document> doc_table_;
        std::shared_ptr<builder::BlockEncoder> p_block_codec_;
        std::unordered_map<uint64_t, builder::BlockHeader> block_header_dict_;
        std::unordered_set<char> delimiters_;
        float average_doc_length_;
    };

}

#endif //INDEX_BUILDER_QUERYEXECUTION_H
