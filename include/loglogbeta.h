#pragma once

#include <iostream>
#include <vector>
#include <array>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <functional>
#include <algorithm>

// ---------------------------
// Adapted from: https://github.com/seiflotfy/loglogbeta
// The LogLogBeta Sketch Class
// ---------------------------
class LogLogBeta {
    static constexpr int PRECISION = 14;
    static constexpr uint32_t M = 1 << PRECISION;  // 16384
    static constexpr int MAX_SHIFT = 64 - PRECISION;
// For example, 64 - 14 = 50
    static constexpr uint64_t MAX_X = (std::numeric_limits<uint64_t>::max() >> MAX_SHIFT);

// alpha = 0.7213 / (1 + 1.079 / M)
    static constexpr double ALPHA = 0.7213 / (1.0 + 1.079 / static_cast<double>(M));

    std::array<uint8_t, M> registers_;

/**
 * Beta polynomial approximation (from the paper).
 * Argument ez = number of zero-registers.
 */
    double betaApprox(double ez) const {
        double zl = std::log(ez + 1.0);
        // The polynomial used in the paper
        return -0.370393911 * ez
               + 0.070471823 * zl
               + 0.17393686  * std::pow(zl, 2)
               + 0.16339839  * std::pow(zl, 3)
               - 0.09237745  * std::pow(zl, 4)
               + 0.03738027  * std::pow(zl, 5)
               - 0.005384159 * std::pow(zl, 6)
               + 0.00042419  * std::pow(zl, 7);
    }

/**
 * Helper function to count leading zeros in a 64-bit integer.
 * Uses GCC/Clang built-in. Replace with your platform-specific function if needed.
 */
    inline uint32_t leadingZeros64(uint64_t x) const {
        if (x == 0) {
            // By definition, leading zeros of 0 can be considered 64,
            // but the usage in the LogLogBeta code means we rarely pass 0 in practice.
            return 64;
        }
        return __builtin_clzll(x);
    }

/**
 * Summation of 1 / 2^register[i] and counting of zero-valued registers.
 */
    std::pair<double, double> regSumAndZeros(const std::array<uint8_t, M>& registers) const {
        double sum = 0.0;
        double ez = 0.0;
        for (auto val : registers) {
            if (val == 0) {
                ez += 1.0;
            }
            // 1 / 2^val
            sum += std::ldexp(1.0, -static_cast<int>(val));
        }
        return {sum, ez};
    }

public:
    LogLogBeta() {
        registers_.fill(0);
    }

    /**
     * AddHash takes a 64-bit hashed value and updates the sketch.
     */
    void addHash(uint64_t x) {
        // k = upper bits as bucket index
        uint32_t k = static_cast<uint32_t>(x >> MAX_SHIFT);

        // val = number of leading zeros of ( (x << PRECISION) ^ MAX_X ) plus 1
        uint64_t shiftedX = (x << PRECISION) ^ MAX_X;
        uint8_t val = static_cast<uint8_t>(leadingZeros64(shiftedX)) + 1;

        // Only update if we found a larger number of leading zeros
        if (registers_[k] < val) {
            registers_[k] = val;
        }
    }

    /**
     * Add a byte sequence (e.g., string) by hashing it first, then calling addHash().
     */
    void add(const std::string& value) {
        uint64_t h = StringUtils::hash_wy(value.c_str(), value.size());
        addHash(h);
    }

    /**
     * Estimate cardinality using the formula from the paper.
     */
    uint64_t cardinality() const {
        auto [sum, ez] = regSumAndZeros(registers_);
        double m = static_cast<double>(M);
        double estimate = ALPHA * m * (m - ez) / (betaApprox(ez) + sum);

        // The original paper and code simply returns the truncated value of that expression.
        if (estimate < 0.0) {
            estimate = 0.0;
        }
        return static_cast<uint64_t>(estimate);
    }

    /**
     * Merge "other" into this sketch. This is the union operation:
     * each register is the max of the two corresponding registers.
     */
    void merge(const LogLogBeta& other) {
        for (size_t i = 0; i < M; i++) {
            if (other.registers_[i] > registers_[i]) {
                registers_[i] = other.registers_[i];
            }
        }
    }
};