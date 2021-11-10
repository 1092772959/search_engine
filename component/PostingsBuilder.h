//
// Created by xiuwenli on 10/2/21.
//

#ifndef INDEX_BUILDER_POSTINGSBUILDER_H
#define INDEX_BUILDER_POSTINGSBUILDER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <filesystem>
#include "utils/BitStream.h"
#include "BlockEncoder.h"

namespace engine::builder {

        struct Posting {
            std::string term;
            std::vector<uint32_t> doc_ids;
            std::vector<uint64_t> frequencies;
            Posting() { }
            Posting(std::string t, std::vector<uint32_t> && ids,
                    std::vector<uint64_t> && freqs)
                    : term(std::move(t))
                    , doc_ids(ids)
                    , frequencies(freqs)
            { }
        };

        struct BlockHeader {
            uint32_t header_size;
            uint32_t block_size; // block body size in bytes
            uint32_t chunk_count;
            uint32_t last_chunk_length; // for the list across blocks
            std::vector<uint32_t> chunk_offsets;  // in bytes
            std::vector<uint32_t> last_doc_ids; // last doc id in each chunk
            BlockHeader (): header_size(0), block_size(0), chunk_count(0) { }
        };

        struct Block {
            BlockHeader header_;
            std::vector<Posting> postings;
        };

        struct BlockEncoder;

        class PostingsBuilder {
        public:
            PostingsBuilder(const std::string & block_dir, const std::string & src_file,
                                 const uint64_t max_block_size, std::shared_ptr<BlockEncoder> block_encoder)
                : block_counter(0)
                , byte_counter(0)
                , max_block_size_(max_block_size)
                , block_dir_(block_dir)
                , p_block_encoder_(block_encoder)
            {
                file_base_ = std::filesystem::path(src_file).filename();
            }

            int add_posting(uint32_t doc_id, const std::string& term);

            int add_postings(uint32_t doc_id,
                             std::vector<std::string> & terms);

            inline int get_block_counter() {
                return block_counter;
            }

            std::vector<std::pair<std::string, uint32_t>> & get_terms_buf() {
                return terms_buf_;
            }

            int get_block_names(std::vector<std::string> & out) {
                out.insert(out.end(), block_names.begin(), block_names.end());
                return 0;
            }

            int dump();
        private:

            uint32_t block_counter;
            uint64_t byte_counter;
            const uint64_t max_block_size_;
            std::string block_dir_;
            std::string file_base_;
            std::vector<std::pair<std::string, uint32_t> > terms_buf_;
            std::vector<std::string> block_names;
            std::shared_ptr<BlockEncoder> p_block_encoder_;
        };

        class BlockLoader {
        public:
            BlockLoader(std::string block_filename, std::shared_ptr<BlockEncoder> p_block_encoder);
            ~BlockLoader();
            uint32_t postings_size();
            bool empty();
            int load_posting(Posting & posting);
        private:
            const std::string block_filename_;
            FILE * fd;
            BitStream buf_;
            uint32_t cur_posting;
            std::shared_ptr<BlockEncoder> p_block_encoder_;
            BlockHeader header_;
        };

    } // namespace engin

#endif //INDEX_BUILDER_POSTINGSBUILDER_H
