#pragma once

#include <sparsepp.h>

/*
struct number_t {
    bool is_float;
    int64_t intval;

    number_t(): is_float(false), intval(0) {

    }

    explicit number_t(float val): is_float(true), intval(*reinterpret_cast<int64_t*>(&val)) {

    }

    explicit number_t(int64_t val): is_float(false), intval(val) {

    }

    inline number_t& operator = (const float val) {
        intval = *reinterpret_cast<const int64_t*>(&val);
        is_float = true;
        return *this;
    }

    inline number_t& operator = (const int64_t & val) {
        intval = val;
        is_float = false;
        return *this;
    }

    inline bool operator == (const number_t & rhs) const  {
        if(is_float) {
            return (*reinterpret_cast<const float*>(&intval)) == (*reinterpret_cast<const float*>(&rhs.intval));
        }
        return intval == rhs.intval;
    }

    inline bool operator < (const number_t & rhs) const  {
        if(is_float) {
            return (*reinterpret_cast<const float*>(&intval)) < (*reinterpret_cast<const float*>(&rhs.intval));
        }
        return intval < rhs.intval;
    }

    inline bool operator > (const number_t & rhs) const {
        if(is_float) {
            return (*reinterpret_cast<const float*>(&intval)) > (*reinterpret_cast<const float*>(&rhs.intval));
        }
        return intval > rhs.intval;
    }

    inline number_t operator * (const number_t & rhs) const {
        if(is_float) {
            return number_t((*reinterpret_cast<const float*>(&intval)) * (*reinterpret_cast<const float*>(&rhs.intval)));
        }
        return number_t(intval * rhs.intval);
    }

    inline number_t operator-()  {
        if(is_float) {
            float floatval = *reinterpret_cast<float*>(&intval);
            floatval = -floatval;
            intval = *reinterpret_cast<int64_t *>(&floatval);
        } else {
            intval = -intval;
        }

        return *this;
    }
};*/
