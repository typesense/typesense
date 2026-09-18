#include "jev_client.h"
#include "http_client.h"
#include "logger.h"

#include <chrono>

static std::string model_name_on_the_wire(const nlohmann::json& model_config) {
    const std::string& model_name = model_config.value("model_name", std::string("jev/jev-latest"));
    const size_t slash = model_name.find('/');
    return slash == std::string::npos ? model_name : model_name.substr(slash + 1);
}

static std::unordered_map<std::string, std::string> auth_headers(const nlohmann::json& model_config) {
    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"}
    };

    const std::string& api_key = model_config.value("api_key", std::string(""));
    if(!api_key.empty()) {
        headers["Authorization"] = "Bearer " + api_key;
    }

    return headers;
}

// typesafe reports failures as {"error": {"message": ...}} or {"detail": ...}, fall back to the raw body
static std::string extract_error(const std::string& response, long status_code) {
    try {
        nlohmann::json error_json = nlohmann::json::parse(response);
        if(error_json.contains("error") && error_json["error"].is_object() &&
           error_json["error"].contains("message") && error_json["error"]["message"].is_string()) {
            return error_json["error"]["message"].get<std::string>();
        }
        if(error_json.contains("detail")) {
            return error_json["detail"].dump();
        }
    } catch(...) {
    }

    if(response.empty()) {
        return "HTTP " + std::to_string(status_code);
    }
    return response.substr(0, 512);
}

Option<nlohmann::json> JevClient::ask(const nlohmann::json& state,
                                      const nlohmann::json& questions,
                                      const nlohmann::json& model_config,
                                      long timeout_ms) {
    if(!questions.is_object() || questions.empty()) {
        return Option<nlohmann::json>(400, "Jev request must carry at least one question.");
    }

    nlohmann::json request_body;
    request_body["model"] = model_name_on_the_wire(model_config);
    request_body["state"] = state;
    request_body["questions"] = questions;

    // a wrong typed url must not throw, validate_model gates it but stored configs are typed by no one
    const std::string api_url = model_config.contains("api_url") && model_config["api_url"].is_string() ?
                                model_config["api_url"].get<std::string>() : DEFAULT_API_URL;

    std::string response;
    std::map<std::string, std::string> response_headers;

    const std::string body = request_body.dump();
    long status_code = post_response(api_url, body, response, response_headers,
                                     auth_headers(model_config), timeout_ms);

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "Jev API timeout.");
    }

    if(status_code != 200) {
        return Option<nlohmann::json>(status_code == 429 ? 429 : 400,
                                      "Jev API error: " + extract_error(response, status_code));
    }

    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response);
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Got malformed response from the Jev API.");
    }

    if(!response_json.contains("answers") || !response_json["answers"].is_object()) {
        return Option<nlohmann::json>(500, "Jev response is missing the `answers` object.");
    }

    return Option<nlohmann::json>(response_json);
}

Option<bool> JevClient::verify_api_key(const nlohmann::json& model_config) {
    const std::string models_url = model_config.contains("models_url") && model_config["models_url"].is_string() ?
                                   model_config["models_url"].get<std::string>() : DEFAULT_MODELS_URL;

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = get_response(models_url, response, response_headers,
                                    auth_headers(model_config), VALIDATION_TIMEOUT_MS);

    if(status_code == 401 || status_code == 403) {
        return Option<bool>(400, "Jev API key is invalid.");
    }

    if(status_code != 200) {
        return Option<bool>(400, "Jev API error: " + extract_error(response, status_code));
    }

    return Option<bool>(true);
}

nlohmann::json JevClient::noul_question(const std::string& instructions) {
    nlohmann::json question;
    question["type"] = "noul";
    question["instructions"] = instructions;
    return question;
}

nlohmann::json JevClient::noul_question(const std::string& instructions,
                                        const std::string& true_desc,
                                        const std::string& false_desc) {
    nlohmann::json question = noul_question(instructions);
    question["criteria"] = {{"true", true_desc}, {"false", false_desc}};
    return question;
}

nlohmann::json JevClient::choice_question(const std::string& instructions,
                                          const std::vector<std::pair<std::string, std::string>>& options) {
    nlohmann::json question;
    question["type"] = "choice";
    question["instructions"] = instructions;

    nlohmann::json criteria = nlohmann::json::object();
    size_t num_options = 0;
    for(const auto& [option, description] : options) {
        if(num_options >= MAX_CHOICE_OPTIONS) {
            break;
        }
        // null is the api's self-explanatory-name form, boilerplate descriptions only pad the payload
        criteria[option] = description.empty() ? nlohmann::json(nullptr) : nlohmann::json(description);
        num_options++;
    }

    question["criteria"] = criteria;
    return question;
}

Option<double> JevClient::get_noul(const nlohmann::json& answers, const std::string& id) {
    if(!answers.is_object() || !answers.contains(id) || !answers[id].is_object()) {
        return Option<double>(404, "No answer for `" + id + "`.");
    }

    const auto& answer = answers[id];
    if(!answer.contains("noul") || !answer["noul"].is_number()) {
        return Option<double>(400, "Answer for `" + id + "` is not a noul.");
    }

    return Option<double>(answer["noul"].get<double>());
}

Option<std::string> JevClient::get_choice(const nlohmann::json& answers, const std::string& id) {
    if(!answers.is_object() || !answers.contains(id) || !answers[id].is_object()) {
        return Option<std::string>(404, "No answer for `" + id + "`.");
    }

    const auto& answer = answers[id];
    if(!answer.contains("choice") || !answer["choice"].is_string()) {
        return Option<std::string>(400, "Answer for `" + id + "` is not a choice.");
    }

    return Option<std::string>(answer["choice"].get<std::string>());
}

Option<double> JevClient::get_confidence(const nlohmann::json& answers, const std::string& id) {
    if(!answers.is_object() || !answers.contains(id) || !answers[id].is_object()) {
        return Option<double>(404, "No answer for `" + id + "`.");
    }

    const auto& answer = answers[id];
    if(!answer.contains("confidence") || !answer["confidence"].is_number()) {
        return Option<double>(400, "Answer for `" + id + "` has no confidence.");
    }

    return Option<double>(answer["confidence"].get<double>());
}

long JevClient::post_response(const std::string& url, const std::string& body, std::string& response,
                              std::map<std::string, std::string>& res_headers,
                              const std::unordered_map<std::string, std::string>& headers,
                              long timeout_ms) {
    if(capture_request) {
        captured_requests.push_back({url, body, headers});
    }

    if(use_mock_response && !mock_responses.empty() && mock_response_index < mock_responses.size()) {
        auto& [mock_body, status, mock_headers] = mock_responses[mock_response_index++];
        response = mock_body;
        res_headers = mock_headers;
        return status;
    }

    return HttpClient::post_response_verified(url, body, response, res_headers, headers, timeout_ms, false);
}

long JevClient::get_response(const std::string& url, std::string& response,
                             std::map<std::string, std::string>& res_headers,
                             const std::unordered_map<std::string, std::string>& headers,
                             long timeout_ms) {
    if(capture_request) {
        captured_requests.push_back({url, "", headers});
    }

    if(use_mock_response && !mock_responses.empty() && mock_response_index < mock_responses.size()) {
        auto& [mock_body, status, mock_headers] = mock_responses[mock_response_index++];
        response = mock_body;
        res_headers = mock_headers;
        return status;
    }

    return HttpClient::get_response_verified(url, response, res_headers, headers, timeout_ms, false);
}

void JevClient::add_mock_response(const std::string& response_body, long status_code,
                                  const std::map<std::string, std::string>& response_headers) {
    use_mock_response = true;
    mock_responses.push_back(std::make_tuple(response_body, status_code, response_headers));
}

void JevClient::clear_mock_responses() {
    use_mock_response = false;
    mock_responses.clear();
    mock_response_index = 0;
    captured_requests.clear();
}
