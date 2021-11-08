//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_COREBUILDER_H
#define INDEX_BUILDER_COREBUILDER_H
#include <string>
#include <utility>
#include <gflags/gflags.h>
#include "HTMLParser.h"
#include "DocTableBuilder.h"
#include "PostingsBuilder.h"
#include "InvertedListBuilder.h"

DECLARE_int32(encode_mode);
DECLARE_int64(inter_buffer_size);
DECLARE_int64(merge_cache_size);
DECLARE_int64(output_buffer_size);
DECLARE_int64(chunk_length);

namespace engine::builder {

    class CoreBuilder{
    public:
        void run(const std::string & filename,
                 const std::string & inter_dir,
                 const std::string & output_dir,
                 int block_mode);

        void merge_sort(const std::string & src_file,
                        const std::string & doc_table_file,
                        const std::string & output_dir,
                        const std::vector<std::string> & block_names,
                        int block_mode);
        };

    } // namespace engin

#endif //INDEX_BUILDER_COREBUILDER_H
