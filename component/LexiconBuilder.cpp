//
// Created by xiuwenli on 10/12/21.
//

#include "LexiconBuilder.h"
#include "utils/encoding.h"
#include <filesystem>
#include <fstream>

using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;

int LexiconEncoder::encode(const unordered_map<string, LexiconEntry> &lexicon_tbl,
                                            BitStream &header_s,
                                            BitStream &body_s) {

    LexiconHeader header = {0};
    for (auto const & [term, entry] : lexicon_tbl) {
        body_s.put_string(term);
        var_byte_encode(entry.block_cursor_, body_s);
        var_byte_encode(entry.chunk_offset_, body_s);
        var_byte_encode(entry.length_, body_s);
    }
    header.size_in_byte_ = body_s.get_length();
    header.entry_size_ = lexicon_tbl.size();

    header_s.put_int64(header.size_in_byte_);
    header_s.put_int64(header.entry_size_);
    return 0;
}

int LexiconEncoder::decode(const LexiconHeader & header, BitStream &body_s,
                           unordered_map<string, LexiconEntry> & lexicon_tbl) {
    for (size_t i = 0; i < header.entry_size_; ++i) {
        LexiconEntry entry;
        string term;
        body_s.get_string(term);
        var_byte_decode(body_s, entry.block_cursor_);
        var_byte_decode(body_s, entry.chunk_offset_);
        var_byte_decode(body_s, entry.length_);
        lexicon_tbl[term] = entry;
    }
    return 0;
}

int LexiconEncoder::dump(BitStream &header_s, BitStream &body_s, const string & dest_file) {
    FILE * fd = fopen(dest_file.c_str(), "w");
    if (fd == nullptr) {
        return -1;
    }
    fwrite(header_s.get_stream(), sizeof(uint8_t), header_s.get_length(), fd);
    fwrite(body_s.get_stream(), sizeof(uint8_t), body_s.get_length(), fd);
    fclose(fd);
    return 0;
}

int LexiconEncoder::load(const string & src_file, LexiconHeader & header, BitStream &body_s) {
    if (!fs::exists(src_file)) {
        return -1;
    }
    FILE * fd = fopen(src_file.c_str(), "r");
    if (fd == nullptr) {
        return -1;
    }
    fread(&header.size_in_byte_, sizeof(uint64_t), 1, fd);
    fread(&header.entry_size_, sizeof(uint64_t), 1, fd);

    char * body_buf = new char[header.size_in_byte_];
    fread(body_buf, sizeof(char), header.size_in_byte_, fd);
    body_s.append_byte_array(body_buf, header.size_in_byte_);
    delete []body_buf;

    fclose(fd);
    return 0;
}

int LexiconEncoder::dump_inter(const unordered_map<string, LexiconEntry> & lexicon_tbl,
                               const string & dest_file) {
    ofstream fout(dest_file, ios::binary);
    for (const auto & [term, entry] : lexicon_tbl) {
        fout << term << " " << entry.length_ << endl;
    }
    return 0;
}


int LexiconEncoder::load_inter(const string & src_file,
                               unordered_map<string, LexiconEntry> & lexicon_tbl) {
    ifstream fin(src_file, ifstream::binary);
    while (!fin.eof()) {
        LexiconEntry entry;
        string term;
        fin >> term;
        fin >> entry.length_;
        lexicon_tbl[term] = entry;
    }
}
