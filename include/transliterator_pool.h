#include <unicode/brkiter.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>

class TransliteratorPool {
    struct Entry {
        std::string id;
        std::unique_ptr<icu::Transliterator> ptr;
    };

    std::mutex  mutex;
    std::condition_variable cv;
    std::vector<Entry> pool;
    std::size_t in_use = 0;
    const std::size_t capacity = std::thread::hardware_concurrency();

public:
    static TransliteratorPool& get_instance() {
        static TransliteratorPool instance;
        return instance;
    }

    std::unique_ptr<icu::Transliterator, std::function<void(icu::Transliterator*)>> acquire(const std::string& id) {
        std::unique_lock lk(mutex);
        for (;;) {
            // Look for an idle object with matching id
            auto it = std::find_if(pool.begin(), pool.end(),
                                   [&](auto& e){ return e.id == id; });
            if (it != pool.end()) {
                auto up = std::move(it->ptr);
                *it = std::move(pool.back());
                pool.pop_back();
                ++in_use;
                lk.unlock();
                return { up.release(), [this](icu::Transliterator* p){ release(p); } };
            }
            if (in_use < capacity) break;          // can create a new one
            cv.wait(lk);                       // wait for somebody to release
        }
        lk.unlock();

        // Heavy work outside the lock
        UErrorCode ec = U_ZERO_ERROR;
        std::unique_ptr<icu::Transliterator> fresh{
            icu::Transliterator::createInstance(id.c_str(), UTRANS_FORWARD, ec) };

        if (U_FAILURE(ec) || !fresh) {
            return { nullptr, [](auto*){}};
        }

        lk.lock();
        ++in_use;
        lk.unlock();
        return { fresh.release(), [this](icu::Transliterator* p){ release(p); } };
    }

private:
    void release(icu::Transliterator* ptr) {
        if (!ptr) {
            return;
        }

        std::string id;
        ptr->getID().toUTF8String(id);

        std::unique_lock lk(mutex);
        pool.push_back( {std::move(id), std::unique_ptr<icu::Transliterator>(ptr)} );
        --in_use;
        lk.unlock();
        cv.notify_one();
    }
};
