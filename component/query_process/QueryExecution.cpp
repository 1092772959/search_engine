//
// Created by xiuwenli on 10/31/21.
//

#include "QueryExecution.h"
#include "LexiconBuilder.h"
#include <utility>

using namespace engine::process;
using namespace engine::builder;
using namespace std;
namespace fs = std::filesystem;

QueryExecution::QueryExecution(string inverted_list_file, string lexicon_file)
    : inverted_list_file_(move(inverted_list_file))
{
    assert(fs::exists(inverted_list_file_));
    assert(fs::exists(lexicon_file));
    //

    LexiconEncoder encoder;
    LexiconHeader header;
    BitStream body;
    encoder.load(lexicon_file, header, body);
    encoder.decode(header, body, lexicons_);

    fd_inv_list_ = fopen(inverted_list_file_.c_str(), "r");
}

QueryExecution::~QueryExecution() {
    fclose(fd_inv_list_);
}
