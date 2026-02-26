import * as THREE from 'three';
import { TalkingHead } from 'talkinghead';

let avatar, websocket, audioCtx, micStream, workletNode;
let isStreaming = false;
let keepAliveInterval = null;

// Buffer incoming audio chunks until the turn ends for lip sync
let incomingAudioChunks = [];

// Accumulate text chunks; only display when the turn ends
let pendingText = '';

// Deduplication: track last logged Aria message to prevent Bedrock echo repeats
let lastLoggedText = '';

const avatarView = document.getElementById('avatar-view');
const startStopBtn = document.getElementById('start-stop-btn');
const clearLogBtn = document.getElementById('clear-log-btn');
const statusDiv = document.getElementById('status');
const chatLog = document.getElementById('chat-log');

// Shared AudioContext for playback (created on first use after user gesture)
let playbackCtx = null;

function getPlaybackCtx() {
    if (!playbackCtx || playbackCtx.state === 'closed') {
        playbackCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
    }
    return playbackCtx;
}

// ─── AUDIO PLAYBACK ──────────────────────────────────────────────────────────

// Convert Int16 PCM → Float32 AudioBuffer (required by Web Audio API and TalkingHead)
async function pcmToAudioBuffer(pcmInt16Array, sampleRate) {
    const ctx = getPlaybackCtx();
    await ctx.resume();
    const float32 = new Float32Array(pcmInt16Array.length);
    for (let i = 0; i < pcmInt16Array.length; i++) {
        float32[i] = pcmInt16Array[i] / 32768.0;
    }
    const audioBuffer = ctx.createBuffer(1, float32.length, sampleRate);
    audioBuffer.copyToChannel(float32, 0);
    return audioBuffer;
}

// Play audio through TalkingHead — drives both audio output AND lip-sync visemes
async function speakWithLipSync(pcmInt16Array) {
    if (!avatar) return false;
    try {
        // Resume TalkingHead's AudioContext (Chrome requires user gesture first)
        if (avatar.audioCtx) await avatar.audioCtx.resume();

        const audioBuffer = await pcmToAudioBuffer(pcmInt16Array, 24000);

        // TalkingHead.speakAudio({ audio: AudioBuffer }) plays audio + generates
        // English phoneme visemes automatically for lip sync
        await avatar.speakAudio({ audio: audioBuffer });
        return true;
    } catch (err) {
        console.error('speakAudio error:', err);
        return false;
    }
}

// Fallback: plain Web Audio playback when avatar is not loaded
let audioQueue = [];
let isPlayingAudio = false;

async function playNextInQueue() {
    if (isPlayingAudio || audioQueue.length === 0) return;
    isPlayingAudio = true;
    const pcmChunk = audioQueue.shift();
    try {
        const audioBuffer = await pcmToAudioBuffer(pcmChunk, 24000);
        const ctx = getPlaybackCtx();
        const source = ctx.createBufferSource();
        source.buffer = audioBuffer;
        source.connect(ctx.destination);
        source.onended = () => {
            isPlayingAudio = false;
            playNextInQueue();
        };
        source.start(0);
    } catch (err) {
        console.error('Fallback audio error:', err);
        isPlayingAudio = false;
        playNextInQueue();
    }
}

// ─── AVATAR INIT ─────────────────────────────────────────────────────────────

async function initAvatar() {
    avatar = new TalkingHead(avatarView, {
        cameraView: 'head',
        ttsEndpoint: null,
        pcmSampleRate: 24000,  // Bedrock Nova-2-Sonic outputs 24kHz PCM
        lipsyncLang: 'en'      // Built-in English phoneme → viseme lip sync
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

// ─── CHAT LOG ────────────────────────────────────────────────────────────────

function logChat(text, role = 'Aria') {
    const trimmed = text && text.trim();
    if (!trimmed) return;

    // Deduplicate: Bedrock re-sends the full response after an interruption/silence
    if (role === 'Aria' && trimmed === lastLoggedText) {
        console.warn('Duplicate Aria message suppressed:', trimmed.substring(0, 60));
        return;
    }
    if (role === 'Aria') lastLoggedText = trimmed;

    const div = document.createElement('div');
    div.className = `chat-message ${role}`;
    div.innerHTML = `<strong>${role}:</strong> ${trimmed}`;
    chatLog.appendChild(div);
    chatLog.scrollTop = chatLog.scrollHeight;
}

// ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

async function toggleSession() {
    if (isStreaming) {
        stopSession();
        return;
    }
    startStopBtn.disabled = true;
    startStopBtn.innerText = 'Connecting...';

    try {
        // Create AudioContext AFTER user gesture (Chrome autoplay policy)
        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        await audioCtx.resume();

        // Resume TalkingHead's internal AudioContext now that we have a user gesture
        if (avatar && avatar.audioCtx) {
            await avatar.audioCtx.resume();
        }

        // Pre-warm the shared playback context too
        await getPlaybackCtx().resume();

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

// ─── MESSAGE HANDLER ─────────────────────────────────────────────────────────

async function handleServerMessage(event) {
    if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);

        if (msg.type === 'text') {
            pendingText += msg.text;

        } else if (msg.type === 'interrupted') {
            incomingAudioChunks = [];
            pendingText = '';
            audioQueue = [];
            isPlayingAudio = false;
            if (avatar) avatar.stop();

        } else if (msg.type === 'audio_end') {
            // Display accumulated text (deduplicated)
            if (pendingText.trim()) {
                logChat(pendingText.trim());
                pendingText = '';
            }

            // Play buffered PCM with lip sync
            if (incomingAudioChunks.length > 0) {
                let totalLength = 0;
                for (const c of incomingAudioChunks) totalLength += c.length;
                const flattened = new Int16Array(totalLength);
                let offset = 0;
                for (const c of incomingAudioChunks) { flattened.set(c, offset); offset += c.length; }
                incomingAudioChunks = [];

                const usedAvatar = await speakWithLipSync(flattened);
                if (!usedAvatar) {
                    audioQueue.push(flattened);
                    playNextInQueue();
                }
            }

        } else if (msg.type === 'system') {
            logChat(`[System: ${msg.text}]`, 'System');
        }
        // 'ping' / 'pong' silently ignored

    } else if (event.data instanceof ArrayBuffer) {
        incomingAudioChunks.push(new Int16Array(event.data));
    }
}

// ─── STOP SESSION ────────────────────────────────────────────────────────────

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

function clearLog() {
    chatLog.innerHTML = '';
    lastLoggedText = '';
}

// ─── INIT ────────────────────────────────────────────────────────────────────

startStopBtn.addEventListener('click', toggleSession);
clearLogBtn.addEventListener('click', clearLog);
startStopBtn.disabled = true;

window.onload = () => { initAvatar(); };