# Open Issues

### 1. Lip Sync Not Working
**Description:** The lip movements of the avatar do not synchronize with the audio output.

**Possible Causes:**
- **Audio-Viseme Mismatch:** The `TalkingHead` library may not be correctly mapping the audio phonemes to the corresponding visemes (lip movements).
- **Audio Format:** The audio format being sent to the `TalkingHead` library might not be optimal for its processing, leading to incorrect lip-sync generation.
- **Timing Issues:** There could be a delay between when the audio is played and when the visemes are rendered, causing them to be out of sync.

### 2. Delay in Audio and Text Transcription
**Description:** There is a noticeable delay between the user speaking and the audio being played back, as well as the text transcription appearing on the screen.

**Possible Causes:**
- **Network Latency:** High latency between the client, the server, and the AWS Bedrock service can introduce delays.
- **AWS Bedrock Processing Time:** The time it takes for AWS Bedrock to process the audio and return the transcription can contribute to the delay.
- **Client-Side Buffering:** The client-side audio buffering might be introducing a delay before the audio is sent to the server.
- **Server-Side Processing:** The server-side processing of the audio and the communication with AWS Bedrock might be adding to the overall delay.

### 3. Duplicates in Text Transcription
**Description:** The text transcription sometimes shows duplicate words or phrases.

**Possible Causes:**
- **Audio Chunking:** The way the audio is being chunked and sent to the server might be causing some chunks to be sent multiple times.
- **WebSocket Reconnections:** If the WebSocket connection is unstable and reconnects, it might be re-sending the same audio chunks.
- **Server-Side Logic:** The server-side logic for handling audio chunks might have a bug that causes it to process the same chunk multiple times.

## Resolved Issues
