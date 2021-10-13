//
// Created by xiuwenli on 10/7/21.
//

#ifndef INDEX_BUILDER_INVERTEDLISTBUILDER_H
#define INDEX_BUILDER_INVERTEDLISTBUILDER_H


#include <string>
#include <vector>
#include <list>
#include <queue>
#include <memory.h>

#include "LexiconBuilder.h"
#include "PostingsBuilder.h"

namespace engine::builder {
    class LexiconEncoder;

    class BufferNode {
    public:
        explicit BufferNode(std::string block_file,
                            std::shared_ptr<BlockEncoder> p_block_loader,
                            uint32_t buf_size);
        bool block_empty();
        bool cache_empty();
        bool operator < (const BufferNode & rhs) const;
        int load_from_disk();
        int pop_posting(Posting & dest);
    private:
        const std::string block_file_;
        const uint32_t buf_size_limit_;
        uint32_t buf_size_;
        std::queue<Posting> posting_buf;
        BlockLoader loader_;
    };

    struct LexiconHeader {
        uint64_t size_in_byte_;
        uint64_t entry_size_;
    };

    struct LexiconEntry {
        uint64_t block_cursor_;
        uint32_t chunk_offset_;
        uint32_t length_;
        LexiconEntry()
                : block_cursor_(0)
                , chunk_offset_(0)
                , length_(0)
        { }

        LexiconEntry(uint64_t cursor, uint32_t chunk_offset, uint32_t len)
                : block_cursor_(cursor)
                , chunk_offset_(chunk_offset)
                , length_(len)
        { }
    };

    class InvertedListBuilder {
    public:
        InvertedListBuilder(std::string output_file,
                            std::string lexicon_file,
                            uint32_t buf_size_limit,
                            uint32_t out_block_size,
                            uint32_t out_chunk_size,
                            std::shared_ptr<BlockEncoder> p_block_encoder,
                            std::shared_ptr<BlockEncoder> p_out_encoder);
        ~InvertedListBuilder();
        int set_block_files(std::vector<std::string> & block_files);
        int execute();

        int dump_output_block(std::vector<Posting> & buf, std::unordered_map<std::string, LexiconEntry> & lexicon);
        int dump_lexicon_entries(std::unordered_map<std::string, LexiconEntry> & lexicons);

    private:
        std::vector<std::string> block_files_;
        FILE * out_fd;
        const std::string output_file_;
        const std::string lexicon_file_;
        const uint32_t in_buf_limit_; // in bytes

        const uint32_t out_chunk_limit_; // count
        const uint32_t out_buf_limit_; // in bytes
        uint32_t out_buf_size_;
        uint64_t out_cursor_;
        std::shared_ptr<BlockEncoder> p_inter_block_encoder_;
        std::shared_ptr<BlockEncoder> p_out_encoder_;
        std::shared_ptr<LexiconEncoder> p_lexicon_encoder_;
    };
}

#endif //INDEX_BUILDER_INVERTEDLISTBUILDER_H
