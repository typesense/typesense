// https://github.com/jhasse/ThreadPool

#pragma once

#include <functional>
#include <future>
#include <queue>

class ThreadPool {
public:
    explicit ThreadPool(size_t);
    template<class F, class... Args>
    decltype(auto) enqueue(F&& f, Args&&... args);
    void shutdown();
private:
    // need to keep track of threads so we can join them
    std::vector< std::thread > workers;
    // the task queue
    std::queue< std::packaged_task<void()> > tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::condition_variable condition_producers;
    bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
        :   stop(false)
{
    for(size_t i = 0;i<threads;++i)
        workers.emplace_back(
                [this]
                {
                    for(;;)
                    {
                        std::packaged_task<void()> task;
                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock,
                                                 [this]{ return this->stop || !this->tasks.empty(); });
                            if(this->stop) {
                                return;
                            }

                            if(this->tasks.empty()) {
                                continue;
                            }
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                            if (tasks.empty()) {
                                condition_producers.notify_one(); // notify the destructor that the queue is empty
                            }
                        }

                        task();
                    }
                }
        );
}

// add new work item to the pool
template<class F, class... Args>
decltype(auto) ThreadPool::enqueue(F&& f, Args&&... args)
{
    using return_type = std::invoke_result_t<F, Args...>;

    std::packaged_task<return_type()> task(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task.get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(!stop) {
            tasks.emplace(std::move(task));
        }
    }
    condition.notify_one();
    return res;
}

inline void ThreadPool::shutdown() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        condition_producers.wait(lock, [this] { return tasks.empty(); });
        stop = true;
    }
    condition.notify_all();
    for (std::thread& worker : workers) {
        worker.join();
    }
}
