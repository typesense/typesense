#include <fstream>
#include <sstream>
#include "text_embedder_tokenizer.h"
#include "logger.h"
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


CLIPTokenizerWrapper::CLIPTokenizerWrapper(const std::string& vocab_path) {
    try {
        clip_tokenizer_ = std::make_unique<CLIPTokenizer>(vocab_path);
    } catch (const std::exception& e) {
        LOG(INFO) << "Failed to load CLIP tokenizer: " << e.what();
        throw;
    }
}

encoded_input_t CLIPTokenizerWrapper::Encode(const std::string& text) {
    auto res = clip_tokenizer_->tokenize({text});

    // convert vector int to vector int64_t
    std::vector<int64_t> input_ids(res.tokens[0].begin(), res.tokens[0].end());
    std::vector<int64_t> attention_mask(res.attention_mask[0].begin(), res.attention_mask[0].end());

    return {input_ids, {}, attention_mask};
}