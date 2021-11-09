//
// Created by xiuwenli on 11/8/21.
//

#ifndef INDEX_BUILDER_MONGOSERVICE_H
#define INDEX_BUILDER_MONGOSERVICE_H

#include <gflags/gflags.h>

#include <cstdint>
#include <iostream>
#include <vector>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>

#include "DocTableBuilder.h"

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

DEFINE_string(mongo_db, "test", "name of the database");
DEFINE_string(mongo_collection, "search_engine", "name of the collection");

namespace engine::database {

    /**
     *  A Singleton class for the mongodb connection
     */
    class MongoService {
    public:
        static MongoService & get_instance() {
            static MongoService service_;
            return service_;
        }
        void operator=(const MongoService &) = delete;
        MongoService(const MongoService &) = delete;
        int addDocument(const builder::Document & doc);
        int selectDocument(const uint32_t & doc_id, builder::Document & output);
    private:
        MongoService() {
            std::cout << "Init mongodb service" << std::endl;
            mongocxx::instance instance{}; // This should be done only once.
            mongocxx::client client {mongocxx::uri{}};
            db_ = client[FLAGS_mongo_db];
            coll_ = db_[FLAGS_mongo_collection];
        }
    private:
        mongocxx::database db_;
        mongocxx::collection coll_;
    };
}




#endif //INDEX_BUILDER_MONGOSERVICE_H
