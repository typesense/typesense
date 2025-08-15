#include <gtest/gtest.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <memory>
#include "raft_http.h"
#include "http_data.h"

TEST(RaftHttpTest, HandleGzipDecompression) {
    auto req = std::make_shared<http_req>();
    std::ifstream infile(std::string(ROOT_DIR)+"test/resources/hnstories.jsonl.gz");
    std::stringstream outbuffer;

    infile.seekg (0, infile.end);
    int length = infile.tellg();
    infile.seekg (0, infile.beg);

    req->body.resize(length);
    infile.read(&req->body[0], length);

    auto res = raft::http::handle_gzip(req);
    if (!res.error().empty()) {
        LOG(ERROR) << res.error();
        FAIL();
    } else {
        outbuffer << req->body;
    }

    std::vector<std::string> doc_lines;
    std::string line;
    while(std::getline(outbuffer, line)) {
        doc_lines.push_back(line);
    }

    ASSERT_EQ(14, doc_lines.size());
    ASSERT_EQ("{\"points\":1,\"title\":\"DuckDuckGo Settings\"}", doc_lines[0]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Making Twitter Easier to Use\"}", doc_lines[1]);
    ASSERT_EQ("{\"points\":2,\"title\":\"London refers Uber app row to High Court\"}", doc_lines[2]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Young Global Leaders, who should be nominated? (World Economic Forum)\"}", doc_lines[3]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Blooki.st goes BETA in a few hours\"}", doc_lines[4]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Unicode Security Data: Beta Review\"}", doc_lines[5]);
    ASSERT_EQ("{\"points\":2,\"title\":\"FileMap: MapReduce on the CLI\"}", doc_lines[6]);
    ASSERT_EQ("{\"points\":1,\"title\":\"[Full Video] NBC News Interview with Edward Snowden\"}", doc_lines[7]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Hybrid App Monetization Example with Mobile Ads and In-App Purchases\"}", doc_lines[8]);
    ASSERT_EQ("{\"points\":1,\"title\":\"We need oppinion from Android Developers\"}", doc_lines[9]);
    ASSERT_EQ("{\"points\":1,\"title\":\"\\\\t Why Mobile Developers Should Care About Deep Linking\"}", doc_lines[10]);
    ASSERT_EQ("{\"points\":2,\"title\":\"Are we getting too Sassy? Weighing up micro-optimisation vs. maintainability\"}", doc_lines[11]);
    ASSERT_EQ("{\"points\":2,\"title\":\"Google's XSS game\"}", doc_lines[12]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Telemba Turns Your Old Roomba and Tablet Into a Telepresence Robot\"}", doc_lines[13]);

    infile.close();
}
