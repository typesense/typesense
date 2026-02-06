#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include "string_utils.h"

#define private public
#include "api_acl.h"
#undef private

class APIAclTest : public ::testing::Test {
protected:
    void SetUp() override { reset_acl(); }
    void TearDown() override { reset_acl(); }

    static void reset_acl() {
        auto& acl = APIAcl::instance();
        acl.set_rate_limit_10s(0);
        acl.set_disallowed_dest_cidrs("");
        {
            std::lock_guard<std::mutex> lk(acl.rate_mu_);
            acl.hits_.clear();
        }
    }
};

TEST_F(APIAclTest, AllowsWhenAllowedSourceIpsEmpty) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips;
    EXPECT_TRUE(acl.is_allowed("192.168.1.10", "https://example.com", allowed_src_ips));
}

TEST_F(APIAclTest, DisallowsWhenDisallowedCidrsEmptyWhenSrcNotAllowed) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("");

    const std::vector<std::string> allowed_src_ips = {"1.1.1.1"};
    EXPECT_FALSE(acl.is_allowed("2.2.2.2", "http://127.0.0.1:80/path", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesWhenSrcIpNotParseable) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("not_an_ip", "http://8.8.8.8:80", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesWhenAllowedSrcIpsHasInvalidIp) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4", "999.999.999.999"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:80", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesWhenSrcIpNotInAllowedList) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.5", "http://8.8.8.8:80", allowed_src_ips));
}

TEST_F(APIAclTest, AllowsWhenSrcAllowedAndDestNotDisallowed) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_TRUE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:8080/foo?bar#baz", allowed_src_ips));
}

TEST_F(APIAclTest, AllowsUrlWithoutScheme) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_TRUE(acl.is_allowed("1.2.3.4", "8.8.8.8:8080/some/path", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesWhenDestinationHostMissing) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://:80/path", allowed_src_ips));
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesUnbracketedIpv6Hosts) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://::1:8080/", allowed_src_ips));
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "::1", allowed_src_ips));
}

TEST_F(APIAclTest, DeniesBracketedIpv6HostsBecauseOnlyIpv4IsSupported) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://[::1]:8080/", allowed_src_ips));
}

TEST_F(APIAclTest, BlocksWhenResolvedDestinationIpFallsInDisallowedCidr) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://127.0.0.1:80", allowed_src_ips));
    EXPECT_TRUE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:80", allowed_src_ips));
}

TEST_F(APIAclTest, BlocksPrivate10OutgoingRange) {
  auto& acl = APIAcl::instance();
  acl.set_rate_limit_10s(0);
  acl.set_disallowed_dest_cidrs("10.0.0.0/8");

  const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
  EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://10.1.1.1:80", allowed_src_ips));
  EXPECT_TRUE(acl.is_allowed("1.2.3.4", "http://11.0.0.1:80", allowed_src_ips));
}

TEST_F(APIAclTest, BlocksExactIpWithCidr32) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("8.8.8.8/32");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:80", allowed_src_ips));
    EXPECT_TRUE(acl.is_allowed("1.2.3.4", "http://8.8.8.9:80", allowed_src_ips));
}

TEST_F(APIAclTest, BlocksAllDestinationsWithCidr0) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("0.0.0.0/0");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:80", allowed_src_ips));
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://127.0.0.1:80", allowed_src_ips));
}

TEST_F(APIAclTest, TrimsAndParsesMultipleDisallowedCidrs) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs(" 10.0.0.0/8 , 192.168.0.0/16 ");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://192.168.1.2:80", allowed_src_ips));
    EXPECT_TRUE(acl.is_allowed("1.2.3.4", "http://8.8.8.8:80", allowed_src_ips));
}

TEST_F(APIAclTest, IgnoresInvalidCidrEntries) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("not_a_cidr,127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://127.0.0.1:80", allowed_src_ips));
}

TEST_F(APIAclTest, ResolvesHostnamesAndAppliesDisallowedCidrs) {
    auto& acl = APIAcl::instance();
    acl.set_rate_limit_10s(0);
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");

    const std::vector<std::string> allowed_src_ips = {"1.2.3.4"};
    EXPECT_FALSE(acl.is_allowed("1.2.3.4", "http://localhost:80", allowed_src_ips));
}

TEST_F(APIAclTest, EnforcesRateLimitAcrossCalls) {
    auto& acl = APIAcl::instance();
    acl.set_disallowed_dest_cidrs(
        "127.0.0.0/8"); // non-empty; real ACL checks are bypassed via empty allowed_src_ips
    acl.set_rate_limit_10s(2);

    const std::vector<std::string> allowed_src_ips;
    EXPECT_TRUE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));
    EXPECT_TRUE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));
    EXPECT_FALSE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));
}

TEST_F(APIAclTest, DisablingRateLimitAllowsRequestsAgain) {
    auto& acl = APIAcl::instance();
    acl.set_disallowed_dest_cidrs("127.0.0.0/8");
    acl.set_rate_limit_10s(1);

    const std::vector<std::string> allowed_src_ips;
    EXPECT_TRUE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));
    EXPECT_FALSE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));

    acl.set_rate_limit_10s(0);
    EXPECT_TRUE(acl.is_allowed("8.8.1.1", "http://192.169.1.200", allowed_src_ips));
}
