#pragma once

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

class APIAcl {
public:
  // Singleton
  static APIAcl& instance() {
    static APIAcl inst;
    return inst;
  }

  // Class-level configuration (thread-safe)
  void set_disallowed_dest_cidrs(const std::string&dest_cidr_list) {
    std::vector<std::string> cidrs;
    StringUtils::split(dest_cidr_list, cidrs, ",");

    std::vector<Cidr> parsed;
    parsed.reserve(cidrs.size());
    for (const auto& c : cidrs) {
      Cidr out;
      if (parse_cidr_v4(c, out)) parsed.push_back(out);
    }
    std::lock_guard<std::mutex> lk(cfg_mu_);
    disallowed_dest_cidrs_ = std::move(parsed);
  }

  void set_rate_limit_10s(std::size_t limit) {
    rate_limit_10s_.store(limit, std::memory_order_relaxed);
  }

  // Returns true if:
  //  - src_ip is in allowed_ips AND
  //  - dest_url host does NOT resolve into any disallowed CIDR
  bool is_allowed(const std::string& src_ip,
                  const std::string& dest_url,
                  const std::vector<std::string>& allowed_src_ips) {
    // 1) Throttle (class-level)
    if (!throttle_ok_()) return false;

    // 2) If either list is empty => allow
    std::vector<Cidr> disallowed_dest_cidrs_copy;
    {
      std::lock_guard<std::mutex> lk(cfg_mu_);
      disallowed_dest_cidrs_copy = disallowed_dest_cidrs_;
    }

    // 3) src_ip must be in allowed_ips
    uint32_t src = 0;
    if (!parse_ipv4(src_ip, src)) {
      return false;
    }

    std::unordered_set<uint32_t> allowed_src_ips_set;
    allowed_src_ips_set.reserve(allowed_src_ips.size());

    for (const auto& ip : allowed_src_ips) {
      uint32_t v = 0;
      if (parse_ipv4(ip, v)) {
        allowed_src_ips_set.insert(v);
      } else {
        return false;
      }
    }

    if (!allowed_src_ips_set.empty() && allowed_src_ips_set.find(src) == allowed_src_ips_set.end()) {
      return false;
    }

    // 4) URL host must NOT be in disallowed CIDRs
    const std::string host = extract_host_(dest_url);
    if (host.empty()) {
      return false;
    }

    std::vector<uint32_t> host_ips;
    if (!host_to_ipv4s_(host, host_ips)) {
      return false; // fail-closed if can't evaluate
    }

    for (uint32_t hip : host_ips) {
      if (ip_in_any_cidr_(hip, disallowed_dest_cidrs_copy)) {
        return false;
      }
    }

    return true;
  }

private:
  APIAcl() : rate_limit_10s_(0) {}
  APIAcl(const APIAcl&) = delete;
  APIAcl& operator=(const APIAcl&) = delete;

  struct Cidr {
    uint32_t network; // host order
    uint32_t mask;    // host order
  };

  // ---- Throttling (fixed 10s rolling window) ----
  bool throttle_ok_() {
    const std::size_t limit = rate_limit_10s_.load(std::memory_order_relaxed);
    if (limit == 0) return true; // 0 == unlimited

    using clock = std::chrono::steady_clock;
    const auto now = clock::now();
    const auto window = std::chrono::seconds(10);

    std::lock_guard<std::mutex> lk(rate_mu_);
    while (!hits_.empty() && (now - hits_.front()) >= window) {
      hits_.pop_front();
    }

    if (hits_.size() >= limit) {
      return false;
    }
    hits_.push_back(now);
    return true;
  }

  // ---- IPv4 + CIDR helpers ----
  static bool parse_ipv4(const std::string& ip, uint32_t& out_host_order) {
    in_addr a;
    if (::inet_pton(AF_INET, ip.c_str(), &a) != 1) return false;
    out_host_order = ntohl(a.s_addr);
    return true;
  }

  static bool parse_cidr_v4(const std::string& cidr, Cidr& out) {
    // "a.b.c.d/prefix"
    const auto slash = cidr.find('/');
    if (slash == std::string::npos) return false;

    const std::string ip_part = cidr.substr(0, slash);
    const std::string pre_part = cidr.substr(slash + 1);

    char* end = nullptr;
    long prefix = std::strtol(pre_part.c_str(), &end, 10);
    if (!end || *end != '\0') return false;
    if (prefix < 0 || prefix > 32) return false;

    uint32_t ip = 0;
    if (!parse_ipv4(ip_part, ip)) return false;

    uint32_t mask = 0;
    if (prefix == 0) mask = 0u;
    else mask = 0xFFFFFFFFu << (32 - static_cast<uint32_t>(prefix));

    out.mask = mask;
    out.network = ip & mask;
    return true;
  }

  static bool ip_in_cidr_(uint32_t ip, const Cidr& c) {
    return (ip & c.mask) == c.network;
  }

  static bool ip_in_any_cidr_(uint32_t ip, const std::vector<Cidr>& cidrs) {
    for (const auto& c : cidrs) {
      if (ip_in_cidr_(ip, c)) return true;
    }
    return false;
  }

  // ---- URL parsing + DNS ----
  static std::string extract_host_(const std::string& url) {
    // Very small URL parser:
    //   [scheme://]host[:port][/...]
    //   Supports IPv6 in brackets: http://[::1]:8080/ (host returned without brackets).
    std::string s = url;

    // strip scheme
    const auto scheme = s.find("://");
    std::size_t start = (scheme == std::string::npos) ? 0 : scheme + 3;

    // host ends at first of "/?#"
    const auto end = s.find_first_of("/?#", start);
    std::string hostport = s.substr(start, (end == std::string::npos) ? std::string::npos : (end - start));
    if (hostport.empty()) return "";

    // IPv6 bracket form
    if (!hostport.empty() && hostport[0] == '[') {
      const auto rb = hostport.find(']');
      if (rb == std::string::npos) return "";
      return hostport.substr(1, rb - 1);
    }

    // split host:port
    const auto colon = hostport.rfind(':');
    if (colon != std::string::npos) {
      // If there are multiple colons, it's probably an unbracketed IPv6 (unsupported here)
      if (hostport.find(':') != colon) return "";
      return hostport.substr(0, colon);
    }
    return hostport;
  }

  static bool host_to_ipv4s_(const std::string& host, std::vector<uint32_t>& out) {
    out.clear();

    // If host is already an IPv4 literal
    uint32_t ip = 0;
    if (parse_ipv4(host, ip)) {
      out.push_back(ip);
      return true;
    }

    // DNS resolve hostname -> IPv4s
    addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;       // IPv4 only
    hints.ai_socktype = SOCK_STREAM; // doesn't matter; helps filtering

    addrinfo* res = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), nullptr, &hints, &res);
    if (rc != 0 || !res) return false;

    for (addrinfo* p = res; p; p = p->ai_next) {
      if (p->ai_family == AF_INET && p->ai_addr && p->ai_addrlen >= sizeof(sockaddr_in)) {
        const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(p->ai_addr);
        uint32_t hip = ntohl(sin->sin_addr.s_addr);
        out.push_back(hip);
      }
    }
    ::freeaddrinfo(res);

    // de-dupe
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return !out.empty();
  }

private:
  // Class-level config
  std::mutex cfg_mu_;
  std::vector<Cidr> disallowed_dest_cidrs_;
  std::atomic<std::size_t> rate_limit_10s_;

  // Rate-limiter state
  std::mutex rate_mu_;
  std::deque<std::chrono::steady_clock::time_point> hits_;
};

