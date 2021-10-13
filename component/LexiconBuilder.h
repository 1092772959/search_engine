//
// Created by xiuwenli on 10/12/21.
//

#ifndef INDEX_BUILDER_LEXICONBUILDER_H
#define INDEX_BUILDER_LEXICONBUILDER_H

#include <string>
#include <unordered_map>
#include "utils/BitStream.h"
#include "InvertedListBuilder.h"

namespace engine::builder {

    struct LexiconHeader;
    struct LexiconEntry;

    class LexiconEncoder {
    public:
        virtual int encode(const std::unordered_map<std::string, LexiconEntry> & lexicon_tbl,
                           BitStream & header_s,
                           BitStream & body_s);
        virtual int decode(const LexiconHeader & header, BitStream & body_s,
                           std::unordered_map<std::string, LexiconEntry> & lexicon_tbl);

        virtual int dump(BitStream & header_s, BitStream & body_s, const std::string & dest_file);
        virtual int load(const std::string & src_file, LexiconHeader & header, BitStream & body_s);
    };

}



#endif //INDEX_BUILDER_LEXICONBUILDER_H
