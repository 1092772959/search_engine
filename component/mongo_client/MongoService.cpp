//
// Created by xiuwenli on 11/8/21.
//

#include "MongoService.h"
#include <string>
#include "constant/status_code.h"

using namespace std;
using namespace engine::database;
using namespace engine::builder;

int MongoService::addDocument(const Document &doc) {
    auto builder = bsoncxx::builder::stream::document{};
    bsoncxx::document::value doc_value = builder
            << Document::DOC_ID_KEY << (int64_t)doc.doc_id_
            << Document::URL_KEY << doc.url_
            << Document::DOC_LENGTH_KEY << (int64_t)doc.doc_size_
            << Document::CONTENT_KEY << doc.content_
            << bsoncxx::builder::stream::finalize;

    bsoncxx::stdx::optional<mongocxx::result::insert_one> result
            = coll_.insert_one(doc_value.view());
    return 0;
}

int MongoService::selectDocument(const uint32_t &doc_id, Document & output) {
    bsoncxx::stdx::optional<bsoncxx::document::value> maybe_result =
            coll_.find_one({
                document{} << output.DOC_ID_KEY << (int64_t)doc_id << finalize
            });
    if(maybe_result) {
        bsoncxx::document::view view = maybe_result->view();

        bsoncxx::document::element element = view[Document::DOC_ID_KEY];
        output.doc_id_ = element.get_int64();
        element = view[Document::URL_KEY];
        output.url_ = element.get_utf8().value.to_string();
        element = view[Document::DOC_LENGTH_KEY];
        output.doc_size_ = element.get_int64();
        element = view[Document::CONTENT_KEY];
        output.content_ = element.get_utf8().value.to_string();
    } else {
        cerr << "Cannot find document with id: " << doc_id << endl;
        return -1;
    }
    return 0;
}

int MongoService::addDocuments(const vector<Document> & docs) {
    vector<bsoncxx::document::value> documents;
    for (const auto & doc : docs) {
        auto builder = bsoncxx::builder::stream::document{};
        bsoncxx::document::value doc_value = builder
                << Document::DOC_ID_KEY << (int64_t)doc.doc_id_
                << Document::URL_KEY << doc.url_
                << Document::DOC_LENGTH_KEY << (int64_t)doc.doc_size_
                << Document::CONTENT_KEY << doc.content_
                << bsoncxx::builder::stream::finalize;

        documents.push_back(doc_value);
    }
    cout << "Start to insert" << endl;
    coll_.insert_many(documents);
    cout << "End of insert" << endl;
    return 0;
}

int64_t MongoService::countDocuments() {
    int64_t count = coll_.estimated_document_count();
    return count;
}

int MongoService::selectDocumentRange(uint32_t L, uint32_t R, vector<Document> & output) {
    mongocxx::cursor cursor =
            coll_.find({
                document{} << Document::DOC_ID_KEY << bsoncxx::builder::stream::open_document
                << "$gte" << (int64_t)L
                << "$lt" << (int64_t)R
                << bsoncxx::builder::stream::close_document
                << finalize
            });
    for (auto doc : cursor) {

        //bsoncxx::document::view view = maybe_result->view();
        Document cur_doc;
        bsoncxx::document::element element = doc[Document::DOC_ID_KEY];
        cur_doc.doc_id_ = element.get_int64();
        element = doc[Document::URL_KEY];
        cur_doc.url_ = element.get_utf8().value.to_string();
        element = doc[Document::DOC_LENGTH_KEY];
        cur_doc.doc_size_ = element.get_int64();
        element = doc[Document::CONTENT_KEY];
        cur_doc.content_ = element.get_utf8().value.to_string();

        output.emplace_back(move(cur_doc));
    }
    return STATUS_SUCCESS;
}

