#include <fstream>
#include <sstream>
#include <algorithm>
#include "text_embedder_tokenizer.h"
#include "logger.h"
#include "json.hpp"
#include <unicode/normalizer2.h>


BertTokenizerWrapper::BertTokenizerWrapper(const std::string& vocab_path) {
    std::ifstream stream(vocab_path);
    std::stringstream ss;
    ss << stream.rdbuf();
    auto vocab_ = ss.str();
    bert_tokenizer_ = std::make_unique<BertTokenizer>(vocab_, true, true, ustring("[UNK]"), ustring("[SEP]"), ustring("[PAD]"),
                    ustring("[CLS]"), ustring("[MASK]"), true, true, ustring("##"),512, std::string("longest_first"));
}

encoded_input_t BertTokenizerWrapper::Encode(const std::string& text) {
    auto encoded = bert_tokenizer_->Encode(bert_tokenizer_->Tokenize(ustring(text)));
    auto input_ids = bert_tokenizer_->AddSpecialToken(encoded);
    auto token_type_ids = bert_tokenizer_->GenerateTypeId(encoded);
    auto attention_mask = std::vector<int64_t>(input_ids.size(), 1);
    // BERT supports max sequence length of 512
    if (input_ids.size() > 512) {
        input_ids.resize(512);
        token_type_ids.resize(512);
        attention_mask.resize(512);
    }
    return {input_ids, token_type_ids, attention_mask};
}



DistilbertTokenizer::DistilbertTokenizer(const std::string& vocab_path) : BertTokenizerWrapper(vocab_path) {}


encoded_input_t DistilbertTokenizer::Encode(const std::string& text) {
    auto encoded = bert_tokenizer_->Encode(bert_tokenizer_->Tokenize(ustring(text)));
    auto input_ids = bert_tokenizer_->AddSpecialToken(encoded);
    auto attention_mask = std::vector<int64_t>(input_ids.size(), 1);
    // DistilBERT supports max sequence length of 512
    if (input_ids.size() > 512) {
        input_ids.resize(512);
        attention_mask.resize(512);
    }
    return {input_ids, {}, attention_mask};
}


XLMRobertaTokenizer::XLMRobertaTokenizer(const std::string& model_path) {
    sentencepiece_tokenizer_ = std::make_unique<sentencepiece::SentencePieceProcessor>();
    sentencepiece_tokenizer_->Load(model_path);
    fairseq_tokens_to_ids_["<mask>"] = sentencepiece_tokenizer_->GetPieceSize() + fairseq_offset;
    sentencepiece_tokenizer_->SetEncodeExtraOptions("bos:eos");
}

const int XLMRobertaTokenizer::token_to_id(const std::string& token) {
    auto it = fairseq_tokens_to_ids_.find(token);
    if (it != fairseq_tokens_to_ids_.end()) {
        return it->second;
    }
    auto spm_id = sentencepiece_tokenizer_->PieceToId(token);
    if (spm_id == 0) {
        return fairseq_tokens_to_ids_["<unk>"];
    }
    return spm_id + fairseq_offset;
}

const std::vector<std::string> XLMRobertaTokenizer::tokenize(const std::string& text) {
    std::vector<std::string> tokens;
    sentencepiece_tokenizer_->Encode(text, &tokens);
    return tokens;
}


encoded_input_t XLMRobertaTokenizer::Encode(const std::string& text) {
    auto tokens = tokenize(text);
    auto input_ids = std::vector<int64_t>(tokens.size());
    auto attention_mask = std::vector<int64_t>(tokens.size(), 1);
    for (size_t i = 0; i < tokens.size(); i++) {
        input_ids[i] = token_to_id(tokens[i]);
    }
    // XLM-RoBERTa supports max sequence length of 128
    if (input_ids.size() > 128) {
        input_ids.resize(128);
        attention_mask.resize(128);
        input_ids[input_ids.size() - 1] = fairseq_tokens_to_ids_["<eos>"];
    }

    return {input_ids, {}, attention_mask};
}


SigLIPTokenizer::SigLIPTokenizer(const std::string& model_path) {
    sentencepiece_tokenizer_ = std::make_unique<sentencepiece::SentencePieceProcessor>();
    sentencepiece_tokenizer_->Load(model_path);
}

encoded_input_t SigLIPTokenizer::Encode(const std::string& text) {
    // Lowercase the input text
    std::string lower_text = text;
    std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Tokenize with SentencePiece (raw IDs, no fairseq offset)
    std::vector<int> piece_ids;
    sentencepiece_tokenizer_->Encode(lower_text, &piece_ids);

    std::vector<int64_t> input_ids(piece_ids.begin(), piece_ids.end());

    // Append EOS token
    input_ids.push_back(eos_token_id_);

    // Truncate to max_length, ensuring last token is EOS
    if (input_ids.size() > max_length_) {
        input_ids.resize(max_length_);
        input_ids[max_length_ - 1] = eos_token_id_;
    }

    // Build attention mask: 1 for real tokens, 0 for padding
    std::vector<int64_t> attention_mask(input_ids.size(), 1);

    // Pad with EOS token to max_length
    input_ids.resize(max_length_, eos_token_id_);
    attention_mask.resize(max_length_, 0);

    return {input_ids, {}, attention_mask};
}


// The GPT-2 byte to unicode table: maps every byte to a printable code point so
// that BPE never has to deal with raw bytes or an unknown token.
static std::array<std::string, 256> build_byte_encoder() {
    std::vector<int> bytes;
    for(int i = 33; i < 127; i++) bytes.push_back(i);
    for(int i = 161; i < 173; i++) bytes.push_back(i);
    for(int i = 174; i < 256; i++) bytes.push_back(i);

    std::vector<int> code_points = bytes;
    int n = 0;
    for(int b = 0; b < 256; b++) {
        if(std::find(bytes.begin(), bytes.end(), b) == bytes.end()) {
            bytes.push_back(b);
            code_points.push_back(256 + n);
            n++;
        }
    }

    std::array<std::string, 256> byte_encoder;
    for(size_t i = 0; i < bytes.size(); i++) {
        icu::UnicodeString(static_cast<UChar32>(code_points[i])).toUTF8String(byte_encoder[bytes[i]]);
    }
    return byte_encoder;
}

// A merge is keyed on both halves. Concatenating them would be ambiguous, since
// "ab" + "c" and "a" + "bc" would collide, so separate them with a NUL, which
// the byte encoder above can never produce.
static inline std::string merge_key(const std::string& first, const std::string& second) {
    std::string key;
    key.reserve(first.size() + second.size() + 1);
    key.append(first);
    key.push_back('\0');
    key.append(second);
    return key;
}

QwenTokenizer::QwenTokenizer(const std::string& vocab_path) {
    std::ifstream vocab_file(vocab_path);
    if(!vocab_file.is_open()) {
        throw std::runtime_error("Could not open tokenizer file: " + vocab_path);
    }

    nlohmann::json tokenizer_json;
    vocab_file >> tokenizer_json;

    if(tokenizer_json.count("model") == 0 || tokenizer_json["model"].count("vocab") == 0 ||
       tokenizer_json["model"].count("merges") == 0) {
        throw std::runtime_error("Tokenizer file is missing model.vocab or model.merges: " + vocab_path);
    }

    byte_encoder_ = build_byte_encoder();

    for(const auto& [token, id] : tokenizer_json["model"]["vocab"].items()) {
        encoder_[token] = id.get<int64_t>();
    }

    // Merges are stored either as ["a", "b"] pairs or as a single "a b" string,
    // depending on which version of tokenizers wrote the file.
    int32_t rank = 0;
    for(const auto& merge : tokenizer_json["model"]["merges"]) {
        if(merge.is_array() && merge.size() == 2) {
            bpe_ranks_[merge_key(merge[0].get<std::string>(), merge[1].get<std::string>())] = rank;
        } else if(merge.is_string()) {
            const std::string& pair = merge.get<std::string>();
            auto space = pair.find(' ');
            if(space == std::string::npos) {
                continue;
            }
            bpe_ranks_[merge_key(pair.substr(0, space), pair.substr(space + 1))] = rank;
        }
        rank++;
    }

    // Added tokens are not part of model.vocab, and the post processor appends
    // <|endoftext|> to every sequence.
    const std::string eos_token = "<|endoftext|>";
    if(tokenizer_json.count("added_tokens") != 0) {
        for(const auto& added : tokenizer_json["added_tokens"]) {
            if(added.count("content") != 0 && added.count("id") != 0) {
                encoder_[added["content"].get<std::string>()] = added["id"].get<int64_t>();
            }
        }
    }

    auto eos_it = encoder_.find(eos_token);
    if(eos_it == encoder_.end()) {
        throw std::runtime_error("Tokenizer file has no " + eos_token + " token: " + vocab_path);
    }
    eos_token_id_ = eos_it->second;

    UErrorCode status = U_ZERO_ERROR;
    nfc_ = icu::Normalizer2::getNFCInstance(status);
    if(U_FAILURE(status)) {
        throw std::runtime_error("Could not load the NFC normalizer");
    }

    // The pre tokenization pattern used by the Qwen2 and Qwen3 tokenizers.
    const icu::UnicodeString pattern(
        "(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}| ?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+");
    matcher_ = std::make_unique<icu::RegexMatcher>(pattern, 0, status);
    if(U_FAILURE(status)) {
        throw std::runtime_error("Could not compile the Qwen pre tokenization pattern");
    }
}

const std::vector<std::string> QwenTokenizer::bpe(const std::string& token) {
    auto cached = cache_.find(token);
    if(cached != cache_.end()) {
        return cached->second;
    }

    // Every byte maps to its own symbol, so the byte encoded token is already
    // split into the single character symbols that BPE starts from.
    std::vector<std::string> word;
    word.reserve(token.size());
    for(unsigned char byte : token) {
        word.push_back(byte_encoder_[byte]);
    }

    while(word.size() > 1) {
        int32_t best_rank = INT32_MAX;
        size_t best_index = 0;
        for(size_t i = 0; i + 1 < word.size(); i++) {
            auto rank_it = bpe_ranks_.find(merge_key(word[i], word[i + 1]));
            if(rank_it != bpe_ranks_.end() && rank_it->second < best_rank) {
                best_rank = rank_it->second;
                best_index = i;
            }
        }

        if(best_rank == INT32_MAX) {
            break;
        }

        // Every occurrence of the best ranked pair is merged in one pass.
        const std::string first = word[best_index];
        const std::string second = word[best_index + 1];
        std::vector<std::string> merged;
        merged.reserve(word.size());
        for(size_t i = 0; i < word.size(); ) {
            if(i + 1 < word.size() && word[i] == first && word[i + 1] == second) {
                merged.push_back(first + second);
                i += 2;
            } else {
                merged.push_back(word[i]);
                i++;
            }
        }
        word = std::move(merged);
    }

    if(cache_.size() >= max_cache_size_) {
        cache_.clear();
    }
    cache_[token] = word;

    return word;
}

encoded_input_t QwenTokenizer::Encode(const std::string& text) {
    // The matcher and the BPE cache are shared state, and batch_encode does not
    // hold the embedder lock.
    std::unique_lock<std::mutex> lock(mutex_);

    UErrorCode status = U_ZERO_ERROR;
    const icu::UnicodeString normalized = nfc_->normalize(icu::UnicodeString::fromUTF8(text), status);
    if(U_FAILURE(status)) {
        return {{eos_token_id_}, {}, {1}};
    }

    std::vector<int64_t> input_ids;
    matcher_->reset(normalized);

    // One slot is kept free for the EOS token appended below.
    while(input_ids.size() < max_length_ - 1 && matcher_->find(status) && U_SUCCESS(status)) {
        std::string piece;
        matcher_->group(status).toUTF8String(piece);
        if(U_FAILURE(status)) {
            break;
        }

        for(const auto& symbol : bpe(piece)) {
            auto id_it = encoder_.find(symbol);
            if(id_it != encoder_.end()) {
                input_ids.push_back(id_it->second);
                if(input_ids.size() == max_length_ - 1) {
                    break;
                }
            }
        }
    }

    input_ids.push_back(eos_token_id_);

    std::vector<int64_t> attention_mask(input_ids.size(), 1);

    return {input_ids, {}, attention_mask};
}


CLIPTokenizerWrapper::CLIPTokenizerWrapper(const std::string& vocab_path) {
    try {
        clip_tokenizer_ = std::make_unique<CLIPTokenizer>(vocab_path);
    } catch (const std::exception& e) {
        LOG(INFO) << "Failed to load CLIP tokenizer: " << e.what();
        throw;
    }
}

encoded_input_t CLIPTokenizerWrapper::Encode(const std::string& text) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto res = clip_tokenizer_->tokenize({text});
    lock.unlock();

    // convert vector int to vector int64_t
    std::vector<int64_t> input_ids(res.tokens[0].begin(), res.tokens[0].end());
    std::vector<int64_t> attention_mask(res.attention_mask[0].begin(), res.attention_mask[0].end());

    return {input_ids, {}, attention_mask};
}