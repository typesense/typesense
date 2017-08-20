#pragma once

struct person {
    bool is_float;
    union {
        int64_t intval;
        float floatval;
    };

    person(): intval(0), is_float(false) {

    }

    person(bool is_float, float floatval): floatval(floatval), is_float(is_float) {

    }

    person(bool is_float, int64_t intval): intval(intval), is_float(is_float) {

    }


    person(float val): floatval(val), is_float(true) {

    }

    person(int64_t val): intval(val), is_float(false) {

    }

    inline void operator = (const float & val) {
        floatval = val;
        is_float = true;
    }

    inline void operator = (const int64_t & val) {
        intval = val;
        is_float = false;
    }

    inline bool operator == (const person & rhs) const  {
        if(is_float) {
            return floatval == rhs.floatval;
        }
        return intval == rhs.intval;
    }

    inline bool operator < (const person & rhs) const  {
        if(is_float) {
            return floatval < rhs.floatval;
        }
        return intval < rhs.intval;
    }

    inline bool operator > (const person & rhs) const {
        if(is_float) {
            return floatval > rhs.floatval;
        }
        return intval > rhs.intval;
    }

    inline person operator * (const person & rhs) const {
        if(is_float) {
            return person(floatval * rhs.floatval);
        }
        return person(intval * rhs.intval);
    }
};

namespace std
{
// inject specialization of std::hash for Person into namespace std
// ----------------------------------------------------------------
    template<>
    struct hash<person>
    {
        std::size_t operator()(person const &p) const
        {
            std::size_t seed = 0;
            spp::hash_combine(seed, p.is_float);
            spp::hash_combine(seed, p.intval);
            return seed;
        }
    };
}