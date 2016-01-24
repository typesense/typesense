#pragma once

template <size_t MAX_SIZE>
struct Topster {

    uint32_t data[MAX_SIZE];
    int size = 0;

    Topster(){ }

    template <typename T> inline void swapMe(T& a, T& b){
        T c = a;
        a = b;
        b = c;
    }

    void add(const uint32_t& val){
        if (size >= MAX_SIZE) {
            if(data[0] <= val) return;

            data[0] = val;
            int i = 0;

            while ((2*i+1) < MAX_SIZE){
                int next = 2*i + 1;
                if (data[next] < data[next+1])
                    next++;

                if (data[i] < data[next]) swapMe(data[i], data[next]);
                else break;

                i = next;
            }
        } else{
            data[size++] = val;
            for (int i = size - 1; i > 0;){
                int parent = (i-1)/2;
                if (data[parent] < data[i]){
                    swapMe(data[parent], data[i]);
                    i = parent;
                }
                else break;
            }
        }
    }

    void clear(){
        size = 0;
    }

};