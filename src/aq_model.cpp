#include "vq_model.h"
#include <sstream>
#define DR_WAV_IMPLEMENTATION
#include "dr_wav.h"


whisper_context* WhisperModel::validate_and_load_model(const std::string& model_path) {
    return whisper_init_from_file(model_path.c_str());                                                              
}

WhisperModel::WhisperModel(whisper_context* ctx, const std::string& model_name) : ctx(ctx), VQModel(model_name) {
    // surpress whisper logs
    whisper_log_set([](enum ggml_log_level level, const char * text, void * user_data) {
    }, nullptr);

    if(whisper_is_multilingual(ctx)) {
        params.language = "auto";
        params.detect_language = true;
    }

    params.suppress_non_speech_tokens = true;
}

WhisperModel::~WhisperModel() {
    whisper_free(ctx);
}

bool WhisperModel::read_wav(const void* data, size_t size, std::vector<float>& pcmf32) {
    drwav wav;
    if(!drwav_init_memory(&wav, data, size, nullptr)) {
        return false;
    }

    if(wav.channels != 1 && wav.channels != 2) {
        drwav_uninit(&wav);
        return false;
    }

    if(wav.bitsPerSample != 16) {
        drwav_uninit(&wav);
        return false;
    }

    if(wav.sampleRate != 16000) {
        drwav_uninit(&wav);
        return false;
    }

    const uint64_t samples = wav.totalPCMFrameCount * wav.channels;
    std::vector<int16_t> pcmi16(samples);
    drwav_read_pcm_frames_s16(&wav, wav.totalPCMFrameCount, pcmi16.data());
    drwav_uninit(&wav);

    pcmf32.resize(samples);
    if(wav.channels == 1) {
        for (uint64_t i = 0; i < wav.totalPCMFrameCount; i++) {
            pcmf32[i] = float(pcmi16[i]) / 32768.0f;
        }
    } else {
        for (uint64_t i = 0; i < wav.totalPCMFrameCount; i++) {
            pcmf32[i] = float(pcmi16[2 * i] + pcmi16[2 * i + 1]) / 65536.0f;
        }
    }
    
    return true;
}

Option<std::string> WhisperModel::transcribe(const std::string& audio_base64) {
    std::vector<float> pcmf32;
    // Decode audio
    auto raw_audio = StringUtils::base64_decode(audio_base64);

    // Read wav
    auto res = read_wav(raw_audio.data(), raw_audio.size(), pcmf32);
    if(!res) {
        return Option<std::string>(400, "Invalid audio format. Please provide a 16-bit 16kHz wav file.");
    }
 
    {
        std::unique_lock<std::mutex> lock(mutex);
        if(whisper_full_parallel(ctx, params, pcmf32.data(), pcmf32.size(), 1) != 0) {
            return Option<std::string>(400, "Error while transcribing.");
        }
    }

    std::stringstream ss;
    for(int i = 0; i < whisper_full_n_segments(ctx); i++) {
        ss << whisper_full_get_segment_text(ctx, i);
    }

    std::string result = ss.str();
    return Option<std::string>(StringUtils::trim(result));
}

