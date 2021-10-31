#include <iostream>

#include <string>
#include <fstream>
#include "component/CoreBuilder.h"

#include <gflags/gflags.h>

DEFINE_int32(encode_mode, 0,
             "Default encoding format for intermediate files, 0 for binary, 1 for plain text");
DEFINE_int64(inter_buffer_size, 1L << 30,
                "Buffer size to hold intermediate postings");
DEFINE_int64(merge_cache_size, 1L << 24,
             "Memory cache size for each intermediate file");
DEFINE_int64(output_buffer_size, 1L << 30,
             "Output buffer size for merged inverted lists");
DEFINE_int64(chunk_length, 128,
             "Max length for a chunk, which is the minimum unit to compress");
DEFINE_string(input_file, "./data/test.trec",
              "The input document file");
DEFINE_string(inter_dir, "./data/tmp",
              "Directory for intermediate files");
DEFINE_string(output_dir, "./data/output",
              "Directory for final output");

using namespace engine::builder;
using namespace std;

int main(int argc, char * argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    CoreBuilder core_builder(FLAGS_input_file,
                             FLAGS_inter_dir,
                             FLAGS_output_dir,
                             FLAGS_encode_mode);
    core_builder.run();
    return 0;
}
