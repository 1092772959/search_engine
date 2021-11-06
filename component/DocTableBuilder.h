//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_DOCTABLEBUILDER_H
#define INDEX_BUILDER_DOCTABLEBUILDER_H

#include <cstdint>
#include <string>
#include "HTMLParser.h"

namespace engine {

    namespace builder {

        struct Document {
            uint64_t doc_id_;
            std::string url_;
            uint64_t doc_size_;
            Document(const uint64_t & doc_id, const std::string & url,
                     const uint64_t & doc_size);
        };

        class DocTableBuilder {
        public:
            //int addDoc(Document && document);
            int dump(std::string file_name, const std::vector<Document> & doc_table);
//        private:
            //std::vector<Document> docTable;
        };

    } // namespace builder
} // namespace engin


#endif //INDEX_BUILDER_DOCTABLEBUILDER_H
