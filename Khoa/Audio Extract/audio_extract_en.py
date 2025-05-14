import subprocess
import whisper

model = whisper.load_model("base.en")

video_in = 'input.mp4'
audio_out = 'output.mp3'

ffmpeg_cmd = f"ffmpeg -i {video_in} -vn -c:a libmp3lame -b:a 192k {audio_out}"

subprocess.run(["ffmpeg", "-i", video_in, "-vn", "-c:a", "libmp3lame", "-b:a", "192k", audio_out])

result = model.transcribe(audio_out)
print(result["text"])
