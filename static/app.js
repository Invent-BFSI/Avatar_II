import * as THREE from 'three';
import { TalkingHead } from 'talkinghead';

let avatar, websocket, audioCtx, micStream, workletNode;
let isStreaming = false;
let keepAliveInterval = null;

// Buffer incoming audio chunks until the sentence is over for perfect lip sync
let incomingAudioChunks = [];

// FIX 2: Accumulate text chunks; only display when the turn ends
let pendingText = '';

const avatarView = document.getElementById('avatar-view');
const startStopBtn = document.getElementById('start-stop-btn');
const clearLogBtn = document.getElementById('clear-log-btn');
const statusDiv = document.getElementById('status');
const chatLog = document.getElementById('chat-log');

// Web Audio fallback queue (used only when avatar is not loaded)
let audioQueue = [];
let isPlayingAudio = false;

function playNextInQueue() {
    if (isPlayingAudio || audioQueue.length === 0) return;
    isPlayingAudio = true;
    const audioCtxPlayback = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
    const wavBlob = audioQueue.shift();
    const reader = new FileReader();
    reader.onload = async (e) => {
        try {
            const arrayBuffer = e.target.result;
            const audioBuffer = await audioCtxPlayback.decodeAudioData(arrayBuffer);
            const source = audioCtxPlayback.createBufferSource();
            source.buffer = audioBuffer;
            source.connect(audioCtxPlayback.destination);
            source.onended = () => {
                isPlayingAudio = false;
                audioCtxPlayback.close();
                playNextInQueue();
            };
            source.start(0);
        } catch (err) {
            console.error('Audio decode error:', err);
            isPlayingAudio = false;
            audioCtxPlayback.close();
            playNextInQueue();
        }
    };
    reader.readAsArrayBuffer(wavBlob);
}

// Play audio through TalkingHead (handles both playback + lip sync viseme generation)
function speakWithLipSync(pcmInt16Array) {
    if (!avatar) return false;
    try {
        // TalkingHead.speakAudio expects: { audio: ArrayBuffer|TypedArray, lipsyncLang: 'en', ... }
        // It plays the PCM audio AND auto-generates visemes for lip sync
        avatar.speakAudio({
            audio: pcmInt16Array.buffer,
            lipsyncLang: 'en'
        });
        return true;
    } catch (err) {
        console.error('speakAudio error:', err);
        return false;
    }
}

// 1. Initialize 3D Scene & TalkingHead
async function initAvatar() {
    avatar = new TalkingHead(avatarView, {
        cameraView: 'head',
        ttsEndpoint: null,
        pcmSampleRate: 24000,   // Bedrock Nova-2-Sonic outputs 24kHz PCM
        lipsyncLang: 'en'        // Enable built-in English lip-sync viseme generation
    });

    try {
        await avatar.showAvatar({ url: '/static/aria.glb' }, (ev) => {
            if (ev.lengthComputable) {
                statusDiv.innerText = `Loading Avatar: ${Math.round((ev.loaded / ev.total) * 100)}%`;
            }
        });
        statusDiv.innerText = 'Avatar Ready. Click Start.';
        startStopBtn.disabled = false;
    } catch (error) {
        console.error('Error loading avatar:', error);
        statusDiv.innerText = 'Avatar failed to load. Voice-only mode available.';
        startStopBtn.disabled = false;
    }
}

// Helper: Append text to chat
function logChat(text, role = 'Aria') {
    if (!text || !text.trim()) return; // FIX 2: never log empty strings
    const div = document.createElement('div');
    div.className = `chat-message ${role}`;
    div.innerHTML = `<strong>${role}:</strong> ${text}`;
    chatLog.appendChild(div);
    chatLog.scrollTop = chatLog.scrollHeight;
}

// 2. Start/Stop Session (Mic + WebSocket)
async function toggleSession() {
    if (isStreaming) {
        stopSession();
        return;
    }
    startStopBtn.disabled = true;
    startStopBtn.innerText = 'Connecting...';

    try {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        await audioCtx.resume();

        micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        const source = audioCtx.createMediaStreamSource(micStream);

        await audioCtx.audioWorklet.addModule('/static/audio-worklet-processor.js');
        workletNode = new AudioWorkletNode(audioCtx, 'audio-sender-processor');

        source.connect(workletNode);
        workletNode.connect(audioCtx.destination);

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        websocket = new WebSocket(`${protocol}//${window.location.host}/ws`);
        websocket.binaryType = 'arraybuffer';

        websocket.onopen = () => {
            isStreaming = true;
            statusDiv.innerText = 'Status: Listening...';
            startStopBtn.innerText = 'Stop Session';
            startStopBtn.disabled = false;

            keepAliveInterval = setInterval(() => {
                if (websocket && websocket.readyState === WebSocket.OPEN) {
                    websocket.send(JSON.stringify({ type: 'ping' }));
                }
            }, 10000);
        };

        workletNode.port.onmessage = (event) => {
            if (isStreaming && websocket.readyState === WebSocket.OPEN) {
                websocket.send(event.data);
            }
        };

        websocket.onmessage = handleServerMessage;
        websocket.onclose = () => { stopSession(); };

    } catch (err) {
        console.error('Microphone/WS Error:', err);
        statusDiv.innerText = 'Error: Check Mic Permissions.';
        startStopBtn.disabled = false;
        startStopBtn.innerText = 'Start Session';
    }
}

// 3. Handle messages from server
async function handleServerMessage(event) {
    if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);

        if (msg.type === 'text') {
            // FIX 2: accumulate chunks instead of logging each one immediately
            pendingText += msg.text;

        } else if (msg.type === 'interrupted') {
            incomingAudioChunks = [];
            pendingText = '';
            audioQueue = [];
            isPlayingAudio = false;
            if (avatar) avatar.stop();

        } else if (msg.type === 'audio_end') {
            // Flush accumulated text once the full turn is done
            if (pendingText.trim()) {
                logChat(pendingText.trim());
                pendingText = '';
            }
            // Play buffered PCM audio
            if (incomingAudioChunks.length > 0) {
                // Flatten all chunks into a single Int16Array
                let totalLength = 0;
                for (const c of incomingAudioChunks) totalLength += c.length;
                const flattened = new Int16Array(totalLength);
                let offset = 0;
                for (const c of incomingAudioChunks) { flattened.set(c, offset); offset += c.length; }
                incomingAudioChunks = [];

                // Prefer TalkingHead (handles playback + lip sync visemes automatically)
                const usedAvatar = speakWithLipSync(flattened);

                // Fallback: plain Web Audio playback if avatar not loaded
                if (!usedAvatar) {
                    const wavBlob = buildWavBlob([flattened]);
                    audioQueue.push(wavBlob);
                    playNextInQueue();
                }
            }

        } else if (msg.type === 'system') {
            logChat(`[System: ${msg.text}]`, 'System');
        }
        // 'ping' / 'pong' silently ignored

    } else if (event.data instanceof ArrayBuffer) {
        // Binary PCM audio chunk (24 kHz Int16)
        incomingAudioChunks.push(new Int16Array(event.data));
    }
}

// 4. Build a WAV Blob from PCM chunks (fallback for voice-only mode)
function buildWavBlob(chunks) {
    let totalLength = 0;
    for (const c of chunks) totalLength += c.length;
    const flattened = new Int16Array(totalLength);
    let offset = 0;
    for (const c of chunks) { flattened.set(c, offset); offset += c.length; }
    return encodeWAV(flattened, 24000);
}

// Utility: wrap raw PCM in a WAV container
function encodeWAV(samples, sampleRate) {
    const buffer = new ArrayBuffer(44 + samples.length * 2);
    const view = new DataView(buffer);
    const ws = (o, s) => { for (let i = 0; i < s.length; i++) view.setUint8(o + i, s.charCodeAt(i)); };
    ws(0, 'RIFF');
    view.setUint32(4, 36 + samples.length * 2, true);
    ws(8, 'WAVE'); ws(12, 'fmt ');
    view.setUint32(16, 16, true);
    view.setUint16(20, 1, true);   // PCM
    view.setUint16(22, 1, true);   // Mono
    view.setUint32(24, sampleRate, true);
    view.setUint32(28, sampleRate * 2, true);
    view.setUint16(32, 2, true);
    view.setUint16(34, 16, true);
    ws(36, 'data');
    view.setUint32(40, samples.length * 2, true);
    let off = 44;
    for (let i = 0; i < samples.length; i++, off += 2) view.setInt16(off, samples[i], true);
    return new Blob([view], { type: 'audio/wav' });
}

function stopSession() {
    isStreaming = false;
    clearInterval(keepAliveInterval);
    keepAliveInterval = null;
    pendingText = '';
    incomingAudioChunks = [];
    audioQueue = [];
    isPlayingAudio = false;
    if (workletNode) workletNode.disconnect();
    if (micStream) micStream.getTracks().forEach(t => t.stop());
    if (websocket) websocket.close();
    if (audioCtx) audioCtx.close();
    statusDiv.innerText = 'Status: Disconnected';
    startStopBtn.disabled = false;
    startStopBtn.innerText = 'Start Session';
}

function clearLog() { chatLog.innerHTML = ''; }

startStopBtn.addEventListener('click', toggleSession);
clearLogBtn.addEventListener('click', clearLog);
startStopBtn.disabled = true;

window.onload = () => { initAvatar(); };