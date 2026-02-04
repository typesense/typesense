#pragma once

#include <memory>
#include <string>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <field.h>


// Singleton class to manage Prometheus metrics
class PrometheusMetrics {
public:
    // Get the singleton instance
    static PrometheusMetrics& getInstance() {
        static PrometheusMetrics instance;
        return instance;
    }

    // Delete copy constructor and assignment operator
    PrometheusMetrics(const PrometheusMetrics&) = delete;
    PrometheusMetrics& operator=(const PrometheusMetrics&) = delete;

   static PrometheusMetrics& get_instance() {
        return getInstance();
    }

    // Initialize the Prometheus exposer
    void initialize();

    // Shutdown the Prometheus exposer
    void shutdown();

    void recordSearchRequest(const std::string& model_name, bool success, double latency_ms);

    void recordDocumentOperation(const std::string& collection, const index_operation_t operation, bool success, double latency_ms);

    void recordImportOperation(const std::string& collection, size_t batch_size, bool success, double latency_ms);

    std::string get_metrics_text();  // Standard Prometheus exposition format
private:
    PrometheusMetrics();
    ~PrometheusMetrics();

    std::shared_ptr<prometheus::Registry> registry_;
    std::unordered_map<std::string, prometheus::Family<prometheus::Counter>*> counter_map;
    std::unordered_map<std::string, prometheus::Family<prometheus::Gauge>*> gauge_map;
    std::unordered_map<std::string, prometheus::Family<prometheus::Histogram>*> histogram_map;

    prometheus::Family<prometheus::Counter>& getCounterFamily(const std::string& name, const std::vector<std::string>& label_names);
    prometheus::Family<prometheus::Gauge>& getGaugeFamily(const std::string& name, const std::vector<std::string>& label_names);
    prometheus::Family<prometheus::Histogram>& getHistogramFamily(const std::string& name, const std::vector<std::string>& label_names);
};

