//
// Created by xiuwenli on 12/6/21.
//
#include <filesystem>
#include <unordered_set>
#include <queue>

#include "BlandfordBlelloch.h"
#include "HTMLParser.h"
#include "constant/status_code.h"
#include "utils/DocumentParser.h"

using namespace std;
using namespace engine;
using namespace engine::builder;
using namespace engine::database;
namespace fs = std::filesystem;


int BlandfordBlelloch::reorder_index(vector<uint32_t> & reordered_indexes) {
    assert (!reordered_indexes.empty());
    //vector<uint32_t> IL, IR;
    vector<uint32_t> I(reordered_indexes.size());
    for (uint32_t i = 0; i < I.size(); ++i) {
        I[i] = i;
    }
    // initialize intermediate left and right clusters
    // Initially, they should set an equal value to each term
    TermVector IL, IR;
    const int docs_to_init = min(100, (int)I.size());
    unordered_set<uint32_t> doc_set;
    for (int i = 0; i < docs_to_init; ++i) {
        uint32_t doc_id;
        do {
            doc_id = rand() % I.size();
        } while (doc_set.find(doc_id) != doc_set.end());
        doc_set.insert(doc_id);
    }
    vector<uint32_t> doc_ids(doc_set.begin(), doc_set.end());
    get_center_of_mass(doc_ids, IL);
    IR = IL;
    order_index(I, 0, I.size() - 1, IL, IR, reordered_indexes);

    return STATUS_SUCCESS;
}

int BlandfordBlelloch::split_index (
            vector<uint32_t> & I,
            vector<uint32_t> &indexes1,
            vector<uint32_t> &indexes2) {
    DocumentParser & parser = DocumentParser::get_instance();

    // subsample
    if (I.size() > 500) { // too large
        DocumentGraph g;
        get_sample_docs(I, g.vertices);
        // build graph
        build_graph(g);

        // partition
        DocumentGraph g1, g2;
        partition(g, g1, g2);

        // get center of mass for two subgraph
        TermVector d1, d2;
        // g1
        for (auto & v : g1.vertices) {
            for (auto &[term, value] : v.terms) {
                if (d1.find(term) == d1.end()) {
                    d1[term] = value;
                } else {
                    d1[term] += value;
                }
            }
        }
        for (auto & ptr : d1) {
            ptr.second /= g1.vertices.size();
        }
        // g2
        for (auto & v : g2.vertices) {
            for (auto &[term, value] : v.terms) {
                if (d2.find(term) == d2.end()) {
                    d2[term] = value;
                } else {
                    d2[term] += value;
                }
            }
        }
        for (auto & ptr : d2) {
            ptr.second /= g2.vertices.size();
        }

        // split-index
        for (auto & doc_id : I) {
            Document doc;
            mongoClient.selectDocument(doc_id, doc);

            // parse the document
            vector<string> terms;
            parser.parse(doc.content_, terms);

            // convert it into a multi-dimensional vector
            TermVector doc_vec;
            get_term_vector(terms, doc_vec);

            // check cosine
            float score1 = cosine(doc_vec, d1);
            float score2 = cosine(doc_vec, d2);
            if (score1 > score2) { // add it into I1
                indexes1.push_back(doc.doc_id_);
            } else { // add into I2
                indexes2.push_back(doc.doc_id_);
            }
        }
    } else { // use the whole data directly to partition
        DocumentGraph g;
        for (const auto & doc_id : I) {
            Document doc;
            if (mongoClient.selectDocument(doc_id, doc) != STATUS_SUCCESS) {
                cerr << "Get data from mongodb error" << endl;
                abort();
            }
            DocumentVertex v;
            v.url_ = doc.url_;
            v.doc_id_ = doc.doc_id_;
            vector<string> terms;
            // parse terms
            parser.parse(doc.content_, terms);

            // convert it to a vector
            get_term_vector(terms, v.terms);

            // store
            g.vertices.emplace_back(move(v));
        }
        // build graph
        build_graph(g);

        DocumentGraph g1, g2;

        // partition
        partition(g, g1, g2);
        for (auto & v : g1.vertices) {
            indexes1.push_back(v.doc_id_);
        }
        for (auto & v : g2.vertices) {
            indexes2.push_back(v.doc_id_);
        }
    }

    return STATUS_SUCCESS;
}

int BlandfordBlelloch::get_sample_docs(vector<uint32_t> &I,
                                       vector<DocumentVertex> &out) {
    int ret = STATUS_SUCCESS;
    //auto & mongoClient = MongoService::get_instance();
    // get total number
    const uint32_t total_size = I.size();
    // random select some document ids
    // set a limit for sample size
    const uint32_t sub_sample_size = min(1000, (int)ceil(total_size * sample_ratio_));
    unordered_set<int64_t> samples;
    for (uint32_t i = 0; i < sub_sample_size; ++i) {
        int64_t n_id;
        do {
            n_id = rand() % sub_sample_size;
        } while (samples.find(n_id) != samples.end());
        samples.insert(n_id);
    }
    // get them from the documents from mongodb
    vector<Document> documents;
    for (const auto id : samples) {
        Document n_doc;
        // get real doc_id form I
        uint32_t doc_id = I[id];
        mongoClient.selectDocument(doc_id, n_doc);
        documents.push_back(n_doc);
    }

    // parse them into terms
    DocumentParser & parser = DocumentParser::get_instance();

    for (auto & doc : documents) {
        DocumentVertex v;
        v.url_ = doc.url_;
        v.doc_id_ = doc.doc_id_;
        vector<string> terms;
        // parse terms
        parser.parse(doc.content_, terms);

        // convert it to a vector
        get_term_vector(terms, v.terms);

        // store
        out.emplace_back(move(v));
    }

    return ret;
}

int BlandfordBlelloch::build_graph(DocumentGraph &g) {
    int ret = STATUS_SUCCESS;
    unordered_map<string, vector<uint32_t> > doc_list_table;
    const int N = g.vertices.size();
    g.adjacency_list.resize(N);
    vector<vector<float>> memo(N, vector<float>(N, -1.0));

    //cout << "Overall graph size: " << g.vertices.size() << endl;
    for (uint32_t i = 0; i < g.vertices.size(); ++i) {
        DocumentVertex & doc = g.vertices[i];
        for (const auto& t : doc.terms) {
            if (t.second > threshold_) { // ignore meaningless terms
                continue;
            }
            const string & term = t.first;
            if (auto ptr = doc_list_table.find(term); ptr == doc_list_table.end()) {
                doc_list_table[term] = {i};
            } else {
                for (auto n_idx : ptr->second) {
                    float weight;
                    if (memo[i][n_idx] == -1.0) {
                        weight = cosine(g.vertices[i].terms, g.vertices[n_idx].terms);
                        memo[n_idx][i] = weight;
                        memo[i][n_idx] = weight;
                    } else {
                        continue;
                    }
                    g.adjacency_list[i].push_back((DocumentEdge){n_idx, weight});
                    g.adjacency_list[n_idx].push_back((DocumentEdge){i, weight});
                }
                ptr->second.push_back(i);
            }
        }
    }
    return ret;
}

int BlandfordBlelloch::partition(DocumentGraph &g, DocumentGraph &g1, DocumentGraph &g2) {
    int ret = STATUS_SUCCESS;
    // use bfs to label vertex with a level id
    // vector<int> lev_ids;
    // lev_ids.resize(g.vertices.size(), -1);
    using QueueNode = pair<float, uint32_t>;
    const int N = g.vertices.size();

    priority_queue<QueueNode, vector<QueueNode>, greater<>> Q;
    Q.push({0.0, 0});
    //float max_dist = -1.0;
    vector<float> dist;
    dist.resize(g.vertices.size(), -1.0);

    // try label each node with some value to better partition
//    while (!Q.empty()) {
//        auto [d, v_id] = Q.top();
//        Q.pop();
//        if (dist[v_id] > -1.0) {
//            continue;
//        }
//        dist[v_id] = d;
//        max_dist = max(d, max_dist);
//
//        for (auto & e : g.adjacency_list[v_id]) {
//            if (dist[e.to_index] > -1.0) {
//                continue;
//            }
//            Q.push({d + e.weight, e.to_index});
//        }
//    }

    // start of Prim
    const float MAXF = 1e30;
    vector<float> D(N, MAXF);
    vector<bool> vis(N, false);
    vis[0] = true;
    D[0] = 0.0;
    for (const auto & e : g.adjacency_list[0]) {
        D[e.to_index] = min(D[e.to_index], e.weight);
    }
    int count = 1;
    int half = N / 2;
    while (count < half) {
        int u = -1;
        for (const auto & list : g.adjacency_list) {
            for (const auto & e : list) {
                if (vis[e.to_index]) {
                    continue;
                }
                if (u == -1 || D[e.to_index] < D[u]) {
                    u = e.to_index;
                }
            }
        }
        if (u == -1) {
            break;
        }
        vis[u] = true;
        for (const auto & e : g.adjacency_list[u]) {
            D[e.to_index] = min(D[e.to_index], e.weight);
        }
        count++;
    }


    // partition with level id into 2 parts
//    vector<float> vec(dist);
//    sort(vec.begin(), vec.end());
//
//    //const float half_dist = max_dist / 2.0;
//    const float half_dist = vec[vec.size() / 2];

    unordered_map<uint32_t, uint32_t> vid_mapping_1, vid_mapping_2;
    vector<uint32_t> n_vid;
    n_vid.resize(g.vertices.size(), -1);

    // move vertices
    for (uint32_t v = 0; v < g.vertices.size(); ++v) {
        if (vis[v]) {
            vid_mapping_1[g1.vertices.size()] = v;
            n_vid[v] = g1.vertices.size();
            g1.vertices.push_back(g.vertices[v]);
        } else {
            vid_mapping_2[g2.vertices.size()] = v;
            n_vid[v] = g2.vertices.size();
            g2.vertices.push_back(g.vertices[v]);
        }
    }

    g1.adjacency_list.resize(g1.vertices.size());
    g2.adjacency_list.resize(g2.vertices.size());

    // construct v1
    for (uint32_t v1 = 0; v1 < g1.vertices.size(); ++v1) {
        uint32_t ori_v = vid_mapping_1[v1];
        for (const DocumentEdge & e : g.adjacency_list[ori_v]) {
            // check if is 1st part
            if (vis[e.to_index]) {
                float weight = cosine(g.vertices[ori_v].terms, g.vertices[e.to_index].terms);

                uint32_t n_to_index = n_vid[e.to_index];
                g1.adjacency_list[v1].push_back((DocumentEdge){n_to_index, weight});
            }
        }
    }

    // construct g2
    for (uint32_t v2 = 0; v2 < g2.vertices.size(); ++v2) {
        uint32_t ori_v = vid_mapping_2[v2];
        for (const DocumentEdge & e : g.adjacency_list[ori_v]) {
            // check if is 2nd part
            if (!vis[e.to_index]) {
                float weight = cosine(g.vertices[ori_v].terms, g.vertices[e.to_index].terms);

                uint32_t n_to_index = n_vid[e.to_index];
                g2.adjacency_list[v2].push_back((DocumentEdge){n_to_index, weight});
            }
        }
    }
    return ret;
}

int BlandfordBlelloch::order_index(vector<uint32_t> &I,
                uint32_t L, uint32_t R,
                TermVector &mL,
                TermVector &mR,
                vector<uint32_t> &reordered_indexes) {
    //if (L == R) { // end of recursion
    if (R - L <= 20) {
        // set new id
        for (uint32_t i = L; i <= R; ++i) {
            reordered_indexes[i] = I[i - L];
        }
        return STATUS_SUCCESS;
    }
    vector<uint32_t> I1, I2;

    split_index(I, I1, I2);
    // convert I1 and I2 to term vector
    cout << L << " " << R << " " << I1.size() << " " << I2.size() << endl;

    int ret = 0;
    TermVector m1, m2;
    get_center_of_mass(I1, m1);
    get_center_of_mass(I2, m2);
    order_cluster(mL, m1, m2, mR, ret);
    if (ret == -1) { // swap I1 and I2
        swap(I1, I2);
    }

    // recurse on two sides
    order_index(I1, L, L + I1.size() - 1, mL, m2, reordered_indexes);
    order_index(I2, L + I1.size(), R, m1, mR, reordered_indexes);

    return STATUS_SUCCESS;
}

int BlandfordBlelloch::order_cluster(TermVector &mL, TermVector &m1, TermVector &m2, TermVector &mR, int &ret) {
    // compare and order
    float s1 = cosine(mL, m1) * cosine(mR, m2);
    float s2 = cosine(mL, m2) * cosine(mR, m1);
    if (s1 > s2) {
        ret = 1;
    } else {
        ret = -1;
    }
    return 0;
}

int BlandfordBlelloch::get_term_vector(vector<std::string> &terms, TermVector & out) {
    //out.clear();
    for (auto & term : terms) {
        if (out.find(term) == out.end()) {
            out[term] = 1.0;
        } else {
            out[term] += 1.0;
        }
    }
    return STATUS_SUCCESS;
}

int BlandfordBlelloch::get_center_of_mass(vector<uint32_t> & indexes,
                                          TermVector & output) {
    DocumentParser & parser = DocumentParser::get_instance();

    for (uint32_t & doc_id : indexes) {
        Document doc;
        mongoClient.selectDocument(doc_id, doc);

        vector<string> terms;
        parser.parse(doc.content_, terms);

        for (auto & term : terms) {
            if (output.find(term) == output.end()) {
                output[term] = 1.0;
            } else {
                output[term] += 1.0;
            }
        }
    }
    for (auto & pr : output) {
        pr.second /= indexes.size();
    }
    return 0;
}

float BlandfordBlelloch::cosine(const unordered_map<string, float>& doc1,
                                const unordered_map<string, float>& doc2) {
    float up = 0.0;
    unordered_set<string> S;
    for (const auto& pr_a : doc1) {
        if (auto pr_b = doc2.find(pr_a.first); pr_b != doc2.end()) {
            up += pr_a.second * pr_b->second;
            //up = 1.0;
        }
    }
    float qa = 0.0;
    for (const auto & pr_a : doc1) {
        qa += pr_a.second * pr_a.second;
    }
    float qb = 0.0;
    for (const auto & pr_b : doc2) {
        qb += pr_b.second * pr_b.second;
    }
    return up / (sqrt(qa) * sqrt(qb));
}
