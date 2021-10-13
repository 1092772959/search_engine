#include <iostream>

#include <string>
#include <fstream>
#include "component/CoreBuilder.h"

using namespace engine::builder;
using namespace std;

int main(int argc, char * argv[]) {
    if (argc == 5) {
        const std::string input(argv[1]);
        const std::string inter_dir(argv[2]);
        const std::string output_dir(argv[3]);
        const int mode = atoi(argv[4]);
        CoreBuilder core_builder(input, inter_dir, output_dir, mode);
        core_builder.run();
    } else {
        cerr << "Expected two arguments" << endl;
    }
    return 0;
}
