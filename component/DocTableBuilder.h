//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_DOCTABLEBUILDER_H
#define INDEX_BUILDER_DOCTABLEBUILDER_H

#include <cstdint>
#include <string>

namespace engine::builder {

        struct Document {
            uint64_t doc_id_;
            std::string url_;
            uint64_t doc_size_;
            Document ();
            Document(const uint64_t & doc_id, const std::string & url,
                     const uint64_t & doc_size);
        };

        class DocTableBuilder {
        public:
            int dump(std::string file_name, const std::vector<Document> & doc_table);
            int load(std::string file_name, std::vector<Document> & doc_table);
        };

    } // namespace engin


#endif //INDEX_BUILDER_DOCTABLEBUILDER_H
