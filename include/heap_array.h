#pragma once

template <typename T> inline void swapMe(T& a, T& b){
    T c = a;
    a = b;
    b = c;
}

struct HeapArray {
    enum{maxSize = 5000};

    int size;
    uint32_t data[maxSize];

    void add(const uint32_t& val){
        if (size >= maxSize){
            if(data[0] <= val)
                return;

            data[0] = val;

            for (int i = 0; (2*i+1) < maxSize; ){
                int next = 2*i + 1;
                if (data[next] < data[next+1])
                    next++;
                if (data[i] < data[next])
                    swapMe(data[i], data[next]);
                else
                    break;
                i = next;
            }
        }
        else {
            data[size++] = val;
            for (int i = size - 1; i > 0;){
                int parent = (i-1)/2;
                if (data[parent] < data[i]){
                    swapMe(data[parent], data[i]);
                    i = parent;
                }
                else
                    break;
            }
        }
    }

    void clear(){
        size = 0;
    }

    HeapArray() :size(0){
    }
};