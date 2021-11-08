//
// Created by xiuwenli on 10/7/21.
//

#ifndef INDEX_BUILDER_BLOCKENCODER_H
#define INDEX_BUILDER_BLOCKENCODER_H

#include "utils/BitStream.h"
#include "PostingsBuilder.h"

namespace engine::builder {
    struct Posting;
    struct BlockHeader;
    struct Block;

    /*
     * Abstract class
     */
    class BlockEncoder {
    public:
        BlockEncoder() { }

        virtual int encode_inter_block(const std::vector<Posting> & posting_table,
                                       BitStream & header_s,
                                       BitStream & out_s,
                                       uint32_t & header_size) = 0;
        virtual int encode_block_header(BlockHeader & header,
                                              BitStream & out) = 0;
        virtual int encode_inter_header(BlockHeader & header,
                                        BitStream & out) = 0;
        virtual int encode_inter_posting(const std::string & term,
                                         const Posting & posting,
                                         BitStream & out) = 0;
        virtual int encode_chunk(const Posting & posting, BitStream & out) = 0;

        virtual int decode_inter_block(BitStream & src, Block & block) = 0;
        virtual int decode_block_header(BitStream & src, BlockHeader & header) = 0;
        virtual int decode_inter_header(BitStream & src, BlockHeader & header) = 0;
        virtual int decode_inter_posting(BitStream & src, Posting & posting) = 0;
        virtual int decode_chunk(BitStream & src, Posting & posting) = 0;
    };

    /*
     * Generate compressed binary output
     */
    class BlockBinaryEncoder : public BlockEncoder {
    public:

        BlockBinaryEncoder(){ }

        int encode_inter_block(const std::vector<Posting> & posting_table,
                               BitStream & header_s,
                               BitStream & out_s,
                               uint32_t & header_size);
        int encode_block_header(BlockHeader & header,
                                        BitStream & out);
        int encode_inter_header(BlockHeader & header,
                                BitStream & out);
        int encode_inter_posting(const std::string & term,
                                         const Posting & posting,
                                         BitStream & out);
        int encode_chunk(const Posting & posting, BitStream & out);

        int decode_inter_block(BitStream & src, Block & block);
        int decode_block_header(BitStream & src, BlockHeader & header);
        int decode_inter_header(BitStream & src, BlockHeader & header);
        int decode_inter_posting(BitStream & src, Posting & posting);
        int decode_chunk(BitStream & src, Posting & posting);
    };

    /*
     * Generate plain text for debugging
     */
    class BlockPlainEncoder : public BlockEncoder {
    public:
        BlockPlainEncoder() {}
        int encode_inter_block(const std::vector<Posting> & posting_table,
                               BitStream & header_s,
                               BitStream & out_s,
                               uint32_t & header_size);
        int encode_block_header(BlockHeader & header,
                                BitStream & out);
        int encode_inter_header(BlockHeader & header,
                                BitStream & out);
        int encode_inter_posting(const std::string & term,
                                 const Posting & posting,
                                 BitStream & out);
        int encode_chunk(const Posting & posting, BitStream & out);

        int decode_inter_block(BitStream & src, Block & block);
        int decode_block_header(BitStream & src, BlockHeader & header);
        int decode_inter_header(BitStream & src, BlockHeader & header);
        int decode_inter_posting(BitStream & src, Posting & posting);
        int decode_chunk(BitStream & src, Posting & posting);
    };
}


#endif //INDEX_BUILDER_BLOCKENCODER_H
