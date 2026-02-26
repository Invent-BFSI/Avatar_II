import * as THREE from 'three';
import { TalkingHead } from 'talkinghead';

let avatar, websocket, audioCtx, micStream, workletNode;
let isStreaming = false;
let keepAliveInterval = null;

let incomingAudioChunks = [];
let pendingText = '';

// Deduplication: normalize and store recent Aria messages to suppress Bedrock echoes
const recentAriaMessages = [];
const DEDUP_WINDOW = 3; // remember last 3 messages

const avatarView = document.getElementById('avatar-view');
const startStopBtn = document.getElementById('start-stop-btn');
const clearLogBtn = document.getElementById('clear-log-btn');
const statusDiv = document.getElementById('status');
const chatLog = document.getElementById('chat-log');

// Shared playback AudioContext (created after user gesture)
let playbackCtx = null;
function getPlaybackCtx() {
    if (!playbackCtx || playbackCtx.state === 'closed') {
        playbackCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
    }
    return playbackCtx;
}

// ─── LIP ANIMATION ───────────────────────────────────────────────────────────

let lipAnimationId = null;

function startLipAnimation() {
    stopLipAnimation();
    if (!avatar) return;

    const startTime = performance.now();
    function animate() {
        const elapsed = (performance.now() - startTime) / 1000;
        // Sine wave jaw open/close at ~4Hz (natural speech rhythm)
        const openAmount = Math.max(0, Math.sin(elapsed * 4 * Math.PI) * 0.5 + 0.15);
        try {
            // TalkingHead exposes morphTargetInfluences on the head mesh
            // Try both common morph target names for jaw open
            if (avatar.head && avatar.head.morphTargetDictionary) {
                const dict = avatar.head.morphTargetDictionary;
                const influences = avatar.head.morphTargetInfluences;
                const jawIdx = dict['jawOpen'] ?? dict['mouthOpen'] ?? dict['jaw_open'] ?? -1;
                if (jawIdx >= 0) influences[jawIdx] = openAmount;
            }
        } catch (e) { /* ignore */ }
        lipAnimationId = requestAnimationFrame(animate);
    }
    lipAnimationId = requestAnimationFrame(animate);
}

function stopLipAnimation() {
    if (lipAnimationId !== null) {
        cancelAnimationFrame(lipAnimationId);
        lipAnimationId = null;
    }
    // Close jaw when done
    if (avatar) {
        try {
            if (avatar.head && avatar.head.morphTargetDictionary) {
                const dict = avatar.head.morphTargetDictionary;
                const influences = avatar.head.morphTargetInfluences;
                const jawIdx = dict['jawOpen'] ?? dict['mouthOpen'] ?? dict['jaw_open'] ?? -1;
                if (jawIdx >= 0) influences[jawIdx] = 0;
            }
        } catch (e) { /* ignore */ }
    }
}

// ─── AUDIO PLAYBACK ──────────────────────────────────────────────────────────

// Int16 PCM → Float32 AudioBuffer
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

// Play audio + animate lips for the duration
async function playWithLipSync(pcmInt16Array) {
    try {
        const ctx = getPlaybackCtx();
        await ctx.resume();
        const audioBuffer = await pcmToAudioBuffer(pcmInt16Array, 24000);
        const source = ctx.createBufferSource();
        source.buffer = audioBuffer;
        source.connect(ctx.destination);

        startLipAnimation();

        source.onended = () => {
            stopLipAnimation();
        };
        source.start(0);
    } catch (err) {
        console.error('Audio playback error:', err);
        stopLipAnimation();
    }
}

// ─── AVATAR INIT ─────────────────────────────────────────────────────────────

async function initAvatar() {
    avatar = new TalkingHead(avatarView, {
        cameraView: 'head',
        ttsEndpoint: null,
        lipsyncLang: 'en'
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

// Normalize text for dedup comparison: lowercase, collapse whitespace, strip punctuation
function normalizeText(text) {
    return text.toLowerCase().replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').trim();
}

function logChat(text, role = 'Aria') {
    const trimmed = text && text.trim();
    if (!trimmed) return;

    // Deduplicate Bedrock echoes: check if normalized version was recently logged
    if (role === 'Aria') {
        const normalized = normalizeText(trimmed);
        if (recentAriaMessages.includes(normalized)) {
            console.warn('Duplicate suppressed:', trimmed.substring(0, 60));
            return;
        }
        recentAriaMessages.push(normalized);
        if (recentAriaMessages.length > DEDUP_WINDOW) recentAriaMessages.shift();
    }

    const div = document.createElement('div');
    div.className = `chat-message ${role}`;
    div.innerHTML = `<strong>${role}:</strong> ${trimmed}`;
    chatLog.appendChild(div);
    chatLog.scrollTop = chatLog.scrollHeight;
}

// ─── SESSION MANAGEMENT ──────────────────────────────────────────────────────

async function toggleSession() {
    if (isStreaming) { stopSession(); return; }

    startStopBtn.disabled = true;
    startStopBtn.innerText = 'Connecting...';

    try {
        // AudioContext must be created/resumed after a user gesture (Chrome policy)
        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        await audioCtx.resume();

        // Resume TalkingHead's internal AudioContext with the same user gesture
        if (avatar && avatar.audioCtx) await avatar.audioCtx.resume();
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
                if (websocket?.readyState === WebSocket.OPEN) {
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
        websocket.onclose = () => stopSession();

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
            stopLipAnimation();

        } else if (msg.type === 'audio_end') {
            if (pendingText.trim()) {
                logChat(pendingText.trim());
                pendingText = '';
            }

            if (incomingAudioChunks.length > 0) {
                let totalLength = 0;
                for (const c of incomingAudioChunks) totalLength += c.length;
                const flattened = new Int16Array(totalLength);
                let offset = 0;
                for (const c of incomingAudioChunks) { flattened.set(c, offset); offset += c.length; }
                incomingAudioChunks = [];

                await playWithLipSync(flattened);
            }

        } else if (msg.type === 'system') {
            logChat(`[System: ${msg.text}]`, 'System');
        }

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
    stopLipAnimation();
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
    recentAriaMessages.length = 0;
}

startStopBtn.addEventListener('click', toggleSession);
clearLogBtn.addEventListener('click', clearLog);
startStopBtn.disabled = true;

window.onload = () => { initAvatar(); };