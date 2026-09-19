#include <prometheus_metrics.h>
#include <sstream>
#include <cmath>

PrometheusMetrics::PrometheusMetrics() {

}

void PrometheusMetrics::initialize() {
    registry_ = std::make_shared<prometheus::Registry>();

    // Pre-create commonly used metric families
    getCounterFamily("search_requests_total", {"model_name", "success"},
        "Total number of search requests processed, partitioned by model name and success status");
    getHistogramFamily("search_request_latency_ms", {"model_name"},
        "Distribution of search request latency in milliseconds, partitioned by model name");
    getCounterFamily("document_operations_total", {"collection", "operation", "success"},
        "Total number of document operations (create, upsert, update, delete), partitioned by collection, operation type, and success status");
    getHistogramFamily("document_operation_latency_ms", {"collection", "operation"},
        "Distribution of document operation latency in milliseconds, partitioned by collection and operation type");
    getCounterFamily("import_operations_total", {"collection", "success"},
        "Total number of bulk import operations, partitioned by collection and success status");
    getHistogramFamily("import_operation_latency_ms", {"collection"},
        "Distribution of bulk import operation latency in milliseconds, partitioned by collection");

    LOG(INFO) << "Prometheus metrics initialized.";
}

void PrometheusMetrics::recordSearchRequest(const std::string& model_name, bool success, double latency_ms) {
    if(!registry_) return;  // Not initialized yet
    
    auto& counter_family = getCounterFamily("search_requests_total", {"model_name", "success"});
    auto& counter = counter_family.Add({{"model_name", model_name}, {"success", success ? "true" : "false"}});
    counter.Increment();

    auto& histogram_family = getHistogramFamily("search_request_latency_ms", {"model_name"});
    auto& histogram = histogram_family.Add({{"model_name", model_name}}, prometheus::Histogram::BucketBoundaries{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000});
    histogram.Observe(latency_ms);
}

void PrometheusMetrics::recordDocumentOperation(const std::string& collection, const index_operation_t operation, bool success, double latency_ms) {
    if(!registry_) return;  // Not initialized yet
    
    auto& counter_family = getCounterFamily("document_operations_total", {"collection", "operation", "success"});
    auto& counter = counter_family.Add({{"collection", collection},
                                       {"operation", std::to_string(static_cast<int>(operation))},
                                       {"success", success ? "true" : "false"}});
    counter.Increment();

    auto& histogram_family = getHistogramFamily("document_operation_latency_ms", {"collection", "operation"});
    auto& histogram = histogram_family.Add({{"collection", collection},
                                           {"operation", std::to_string(static_cast<int>(operation))}}, prometheus::Histogram::BucketBoundaries{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000});
    histogram.Observe(latency_ms);
}

void PrometheusMetrics::recordImportOperation(const std::string& collection, size_t batch_size, bool success, double latency_ms) {
    if(!registry_) return;  // Not initialized yet
    
    auto& counter_family = getCounterFamily("import_operations_total", {"collection", "success"});
    auto& counter = counter_family.Add({{"collection", collection},
                                       {"success", success ? "true" : "false"}});
    counter.Increment();

    auto& histogram_family = getHistogramFamily("import_operation_latency_ms", {"collection"});
    auto& histogram = histogram_family.Add({{"collection", collection}}, prometheus::Histogram::BucketBoundaries{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000});
    histogram.Observe(latency_ms);
}

prometheus::Family<prometheus::Counter>& PrometheusMetrics::getCounterFamily(const std::string& name, const std::vector<std::string>& label_names, const std::string& help_text) {
    auto it = counter_map.find(name);
    if (it != counter_map.end()) {
        return *(it->second);
    }

    auto& family = prometheus::BuildCounter()
                       .Name(name)
                       .Help(help_text.empty() ? name : help_text)
                       .Labels({})
                       .Register(*registry_);
    counter_map[name] = &family;
    return family;
}

prometheus::Family<prometheus::Gauge>& PrometheusMetrics::getGaugeFamily(const std::string& name, const std::vector<std::string>& label_names, const std::string& help_text) {
    auto it = gauge_map.find(name);
    if (it != gauge_map.end()) {
        return *(it->second);
    }

    auto& family = prometheus::BuildGauge()
                       .Name(name)
                       .Help(help_text.empty() ? name : help_text)
                       .Labels({})
                       .Register(*registry_);
    gauge_map[name] = &family;
    return family;
}

prometheus::Family<prometheus::Histogram>& PrometheusMetrics::getHistogramFamily(const std::string& name, const std::vector<std::string>& label_names, const std::string& help_text) {
    auto it = histogram_map.find(name);
    if (it != histogram_map.end()) {
        return *(it->second);
    }

    auto& family = prometheus::BuildHistogram()
                       .Name(name)
                       .Help(help_text.empty() ? name : help_text)
                       .Labels({})
                       .Register(*registry_);
    histogram_map[name] = &family;
    return family;
}

std::string PrometheusMetrics::get_metrics_text() {
    std::ostringstream output;

    if(!registry_) return "";  // Not initialized yet

    auto collected_metrics = registry_->Collect();
    for (const auto& metric_family : collected_metrics) {
        // Write HELP line
        output << "# HELP " << metric_family.name << " " << metric_family.help << "\n";
        
        // Write TYPE line
        std::string type_str;
        switch (metric_family.type) {
            case prometheus::MetricType::Counter:
                type_str = "counter";
                break;
            case prometheus::MetricType::Gauge:
                type_str = "gauge";
                break;
            case prometheus::MetricType::Histogram:
                type_str = "histogram";
                break;
            case prometheus::MetricType::Summary:
                type_str = "summary";
                break;
            default:
                type_str = "untyped";
                break;
        }
        output << "# TYPE " << metric_family.name << " " << type_str << "\n";

        for (const auto& metric : metric_family.metric) {
            // Build labels string
            std::string labels_str;
            if (!metric.label.empty()) {
                labels_str = "{";
                bool first = true;
                for (const auto& label_pair : metric.label) {
                    if (!first) labels_str += ",";
                    labels_str += label_pair.name + "=\"" + label_pair.value + "\"";
                    first = false;
                }
                labels_str += "}";
            }

            switch (metric_family.type) {
                case prometheus::MetricType::Counter:
                    output << metric_family.name << labels_str << " " << metric.counter.value << "\n";
                    break;
                case prometheus::MetricType::Gauge:
                    output << metric_family.name << labels_str << " " << metric.gauge.value << "\n";
                    break;
                case prometheus::MetricType::Histogram: {
                    // Build base labels (without le)
                    std::string base_labels;
                    for (const auto& label_pair : metric.label) {
                        if (!base_labels.empty()) base_labels += ",";
                        base_labels += label_pair.name + "=\"" + label_pair.value + "\"";
                    }
                    
                    // Output bucket lines
                    for (const auto& bucket : metric.histogram.bucket) {
                        std::string bucket_labels = base_labels;
                        if (!bucket_labels.empty()) bucket_labels += ",";
                        
                        if (std::isinf(bucket.upper_bound)) {
                            bucket_labels += "le=\"+Inf\"";
                        } else {
                            std::ostringstream le_stream;
                            le_stream << bucket.upper_bound;
                            bucket_labels += "le=\"" + le_stream.str() + "\"";
                        }
                        
                        output << metric_family.name << "_bucket{" << bucket_labels << "} " 
                               << bucket.cumulative_count << "\n";
                    }
                    
                    // Output sum and count
                    std::string sum_count_labels = base_labels.empty() ? "" : "{" + base_labels + "}";
                    output << metric_family.name << "_sum" << sum_count_labels << " " 
                           << metric.histogram.sample_sum << "\n";
                    output << metric_family.name << "_count" << sum_count_labels << " " 
                           << metric.histogram.sample_count << "\n";
                    break;
                }
                default:
                    break;
            }
        }
        output << "\n";
    }

    return output.str();
}