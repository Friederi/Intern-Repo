import subprocess
from transformers import pipeline

def extract_audio(video_in: str, audio_out: str):
    subprocess.run([
        "ffmpeg", "-i", video_in, "-vn", "-c:a", "libmp3lame", "-b:a", "192k", audio_out
    ], check=True)

def transcribe_audio(audio_path: str):
    transcriber = pipeline(
        "automatic-speech-recognition",
        model="vinai/PhoWhisper-large", 
        return_timestamps="word"
    )
    return transcriber(audio_path)

def group_by_sentences(chunks):
    import re
    sentences = []
    sentence = []
    for word in chunks:
        sentence.append(word)
        if re.search(r"[.!?]", word["text"]):
            text = " ".join(w["text"] for w in sentence)
            start = sentence[0]["timestamp"][0]
            end = sentence[-1]["timestamp"][1]
            sentences.append({"text": text, "start": start, "end": end})
            sentence = []
    return sentences

def main():
    video_file = "input_vn.mp4"
    audio_file = "output_vn.mp3"

    extract_audio(video_file, audio_file)

    result = transcribe_audio(audio_file)
    if isinstance(result, dict) and "chunks" in result:
        sentences = group_by_sentences(result["chunks"])
        print("Timestamps:")
        for s in sentences:
            print(f"[{s['start']:.2f}s - {s['end']:.2f}s]: {s['text']}")
    else:
        print("Transcription result:\n", result)

if __name__ == "__main__":
    main()
