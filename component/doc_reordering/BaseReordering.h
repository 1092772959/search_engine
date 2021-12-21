//
// Created by xiuwenli on 12/18/21.
//

#ifndef INDEX_BUILDER_BASEREORDERING_H
#define INDEX_BUILDER_BASEREORDERING_H

#include <vector>

namespace engine::builder {

    class BaseReordering {
    public:
        virtual int reorder_index(std::vector<uint32_t> & I);
    };

}

#endif //INDEX_BUILDER_BASEREORDERING_H
