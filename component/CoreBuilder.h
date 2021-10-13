//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_COREBUILDER_H
#define INDEX_BUILDER_COREBUILDER_H
#include <string>
#include <utility>
#include "HTMLParser.h"
#include "DocTableBuilder.h"
#include "PostingsBuilder.h"
#include "InvertedListBuilder.h"

#define MAX_BLOCK_SIZE (1 << 30)
#define MAX_CHUNK_SIZE (1 << 7)

#define MAX_CACHE_SIZE (1 << 24)
#define OUT_BLOCK_SIZE (1 << 30)
#define OUT_CHUNK_SIZE 128

namespace engine::builder {

        class CoreBuilder{
        public:
            CoreBuilder(std::string filename,
                        std::string inter_dir,
                        std::string output_dir,
                        int block_mode);

            void run();

        private:
            std::string filename;
            HTMLParser parser;
            std::string doc_table_file_;
            DocTableBuilder doc_table_builder_;
            std::shared_ptr<PostingsBuilder> p_posting_builder_;
            std::shared_ptr<InvertedListBuilder> p_inverted_builder;
        };

    } // namespace engin

#endif //INDEX_BUILDER_COREBUILDER_H
