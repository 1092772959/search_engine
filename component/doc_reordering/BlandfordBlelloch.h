//
// Created by xiuwenli on 12/6/21.
//

#ifndef INDEX_BUILDER_BLANDFORDBLELLOCH_H
#define INDEX_BUILDER_BLANDFORDBLELLOCH_H

#include <string>
#include <vector>
#include <unordered_map>

#include "mongo_client/MongoService.h"
#include "BaseReordering.h"

namespace engine::builder {
    using TermVector = std::unordered_map<std::string, float>;
    struct DocumentVertex {
        uint32_t doc_id_;
        std::string url_;
        TermVector terms;
    };

    struct DocumentEdge {
        uint32_t to_index;
        float weight;
        bool operator == (const DocumentEdge & rhs) const {
            return to_index == rhs.to_index;
        }
    };

    class EdgeHashFunction {
    public:
        // id is returned as hash function
        size_t operator()(const DocumentEdge& t) const {
            return t.to_index;
        }
    };

    struct DocumentGraph {
        std::vector<std::vector<DocumentEdge>> adjacency_list;
        std::vector<DocumentVertex> vertices;
    };

    /**
     * Index builder using document reordering algorithms
     * to get a better compression performance.
     */
    class BlandfordBlelloch : public BaseReordering {
    public:
        BlandfordBlelloch(int threshold, int sample_threshold, float sample_ratio)
            : threshold_(threshold)
            , sample_ratio_(sample_ratio)
            , sample_threshold_(sample_threshold)
            , mongoClient(database::MongoService::get_instance()) {
        }

        /**
         * Before calling this function, you should ensure that
         * each document has been inserted into the DynamoDB colleciton
         * you provided in the gflags.
         * @param I
         * @return
         */
        int reorder_index(std::vector<uint32_t> & I);
    private:
        int split_index(std::vector<uint32_t> & I,
                        std::vector<uint32_t> & indexes1,
                        std::vector<uint32_t> & indexes2);

        int get_sample_docs(std::vector<uint32_t> &I,
                std::vector<DocumentVertex> & out);
        int build_graph(DocumentGraph & g);
        int partition(DocumentGraph & g, DocumentGraph & g1, DocumentGraph &g2);

        // recursive function
        int order_index(std::vector<uint32_t> &I,
                        uint32_t L, uint32_t R,
                        TermVector &IL,
                        TermVector &IR,
                        std::vector<uint32_t> &reordered_indexes);
        int order_cluster(TermVector & mL, TermVector & m1,
                          TermVector & m2, TermVector & mR,
                          int & ret
        );
        int get_term_vector(std::vector<std::string> & terms, TermVector & output);
    private:
        float cosine(const std::unordered_map<std::string, float>& doc1,
                     const std::unordered_map<std::string, float>& doc2);
        int get_center_of_mass(std::vector<uint32_t> & indexes,
                               TermVector & output);

        const int threshold_;
        const float sample_ratio_;
        const int sample_threshold_;
        std::unordered_map<uint32_t, std::unordered_map<uint32_t, float>> dp;
        database::MongoService & mongoClient;
        std::vector<uint32_t> new_doc_ids_;
    };
}

#endif //INDEX_BUILDER_BLANDFORDBLELLOCH_H
