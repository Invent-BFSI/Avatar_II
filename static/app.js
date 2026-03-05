import * as THREE from 'three';
import { TalkingHead } from 'talkinghead';
import { LipsyncEn } from './lipsync-en.mjs';

let avatar, websocket, audioCtx, micStream, workletNode;
let isStreaming = false;
let keepAliveInterval = null;

let incomingAudioChunks = [];
let pendingText = '';
let lastLoggedText = '';

const lipsync = new LipsyncEn();

const avatarView   = document.getElementById('avatar-view');
const startStopBtn = document.getElementById('start-stop-btn');
const clearLogBtn  = document.getElementById('clear-log-btn');
const statusDiv    = document.getElementById('status');
const chatLog      = document.getElementById('chat-log');

// ─── AUDIO CONVERSION ────────────────────────────────────────────────────────

/**
 * Convert Int16 PCM → Float32 AudioBuffer using TalkingHead's OWN audioCtx.
 * CRITICAL: AudioBuffers are context-bound — using any other context causes
 * speakAudio to silently fail or throw "unable to decode audio data".
 */
function pcmToAudioBuffer(pcmInt16Array, sampleRate) {
    const ctx = avatar.audioCtx;                        // must be avatar's context
    const float32 = new Float32Array(pcmInt16Array.length);
    for (let i = 0; i < pcmInt16Array.length; i++) {
        float32[i] = pcmInt16Array[i] / 32768.0;
    }
    const audioBuffer = ctx.createBuffer(1, float32.length, sampleRate);
    audioBuffer.copyToChannel(float32, 0);
    return audioBuffer;
}

// ─── LIPSYNC BUILDER ─────────────────────────────────────────────────────────

function buildSpeakAudioObj(audioBuffer, text) {
    const durationMs   = audioBuffer.duration * 1000;

    console.log('[buildSpeakAudioObj] raw text:', JSON.stringify(text));
    console.log('[buildSpeakAudioObj] audio duration:', durationMs.toFixed(1), 'ms');

    const preprocessed = lipsync.preProcessText(text);
    console.log('[buildSpeakAudioObj] preprocessed:', JSON.stringify(preprocessed));

    const wordList = preprocessed.split(/\s+/).filter(Boolean);
    console.log('[buildSpeakAudioObj] wordList:', wordList);

    if (wordList.length === 0) {
        console.warn('[buildSpeakAudioObj] ⚠️ no words — returning audio-only object');
        return { audio: audioBuffer };
    }

    // Log first word's raw viseme data to check LipsyncEn output shape
    const sample = lipsync.wordsToVisemes(wordList[0]);
    console.log('[buildSpeakAudioObj] sample wordsToVisemes("' + wordList[0] + '"):', JSON.stringify(sample));

    const wordData    = wordList.map(w => lipsync.wordsToVisemes(w));
    const wordRelDurs = wordData.map(r =>
        r.durations.length > 0 ? r.durations.reduce((a, b) => a + b, 0) : 1
    );

    const spacingUnits  = Math.max(0, wordList.length - 1);
    const totalRelUnits = wordRelDurs.reduce((a, b) => a + b, 0) + spacingUnits;
    const msPerUnit     = durationMs / (totalRelUnits || 1);
    const spacingMs     = msPerUnit;

    const words = [], wtimes = [], wdurations = [];
    const visemes = [], vtimes = [], vdurations = [];

    let wordStartMs = 0;

    wordData.forEach((r, wi) => {
        const wordMs = wordRelDurs[wi] * msPerUnit;

        words.push(wordList[wi]);
        wtimes.push(Math.round(wordStartMs));
        wdurations.push(Math.round(wordMs));

        r.visemes.forEach((v, vi) => {
            visemes.push(v);
            vtimes.push(Math.round(wordStartMs + r.times[vi] * msPerUnit));
            vdurations.push(Math.max(20, Math.round(r.durations[vi] * msPerUnit)));
        });

        wordStartMs += wordMs + (wi < wordData.length - 1 ? spacingMs : 0);
    });

    console.log(
        `[LipSync] words:${words.length} visemes:${visemes.length} | ` +
        `first: ${visemes[0]} @${vtimes[0]}ms | ` +
        `last: ${visemes[visemes.length-1]} @${vtimes[vtimes.length-1]}ms | ` +
        `audio: ${Math.round(durationMs)}ms`
    );

    return { audio: audioBuffer, words, wtimes, wdurations, visemes, vtimes, vdurations };
}

// ─── SPEAK WITH LIP SYNC ─────────────────────────────────────────────────────

async function speakWithLipSync(pcmInt16Array, text) {
    if (!avatar) return false;

    try {
        // Always resume avatar's AudioContext first — Chrome blocks until user gesture
        await avatar.audioCtx.resume();

        // Build AudioBuffer inside avatar's OWN context — this is the critical fix
        const audioBuffer = pcmToAudioBuffer(pcmInt16Array, 24000);

        const audioObj = (text && text.trim())
            ? buildSpeakAudioObj(audioBuffer, text)
            : { audio: audioBuffer };

        console.log('[speakWithLipSync] calling speakAudio, keys:', Object.keys(audioObj));

        await avatar.speakAudio(audioObj);
        return true;
    } catch (err) {
        console.error('[speakWithLipSync] error:', err);
        return false;
    }
}

// ─── FALLBACK AUDIO (no avatar) ──────────────────────────────────────────────

let fallbackCtx    = null;
let audioQueue     = [];
let isPlayingAudio = false;

function getFallbackCtx() {
    if (!fallbackCtx || fallbackCtx.state === 'closed') {
        fallbackCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
    }
    return fallbackCtx;
}

async function playNextInQueue() {
    if (isPlayingAudio || audioQueue.length === 0) return;
    isPlayingAudio = true;
    const pcmChunk = audioQueue.shift();
    try {
        const ctx     = getFallbackCtx();
        await ctx.resume();
        const float32 = new Float32Array(pcmChunk.length);
        for (let i = 0; i < pcmChunk.length; i++) float32[i] = pcmChunk[i] / 32768.0;
        const buf    = ctx.createBuffer(1, float32.length, 24000);
        buf.copyToChannel(float32, 0);
        const source = ctx.createBufferSource();
        source.buffer = buf;
        source.connect(ctx.destination);
        source.onended = () => { isPlayingAudio = false; playNextInQueue(); };
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
        cameraView:    'upper',
        ttsEndpoint:   null,
        pcmSampleRate: 24000,
        lipsyncLang:   'en'
    });

    try {
        await avatar.showAvatar({ url: '/static/aria.glb' }, (ev) => {
            if (ev.lengthComputable) {
                statusDiv.innerText = `Loading Avatar: ${Math.round((ev.loaded / ev.total) * 100)}%`;
            }
        });
        statusDiv.innerText   = 'Avatar Ready. Click Start.';
        startStopBtn.disabled = false;
    } catch (error) {
        console.error('Error loading avatar:', error);
        statusDiv.innerText   = 'Avatar failed to load. Voice-only mode available.';
        startStopBtn.disabled = false;
    }
}

// ─── CHAT LOG ────────────────────────────────────────────────────────────────

function logChat(text, role = 'Aria') {
    const trimmed = text && text.trim();
    if (!trimmed) return;
    if (role === 'Aria' && trimmed === lastLoggedText) {
        console.warn('Duplicate suppressed:', trimmed.substring(0, 60));
        return;
    }
    if (role === 'Aria') lastLoggedText = trimmed;
    const div = document.createElement('div');
    div.className = `chat-message ${role}`;
    div.innerHTML = `<strong>${role}:</strong> ${trimmed}`;
    chatLog.appendChild(div);
    chatLog.scrollTop = chatLog.scrollHeight;
}

// ─── SESSION ─────────────────────────────────────────────────────────────────

async function toggleSession() {
    if (isStreaming) { stopSession(); return; }

    startStopBtn.disabled  = true;
    startStopBtn.innerText = 'Connecting...';

    try {
        // Mic capture context (16 kHz for Bedrock input)
        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        await audioCtx.resume();

        // Unblock avatar's AudioContext now that we have a user gesture
        if (avatar && avatar.audioCtx) await avatar.audioCtx.resume();

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
            isStreaming            = true;
            statusDiv.innerText    = 'Status: Listening...';
            startStopBtn.innerText = 'Stop Session';
            startStopBtn.disabled  = false;

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
        websocket.onclose   = () => { stopSession(); };

    } catch (err) {
        console.error('Microphone/WS Error:', err);
        statusDiv.innerText    = 'Error: Check Mic Permissions.';
        startStopBtn.disabled  = false;
        startStopBtn.innerText = 'Start Session';
    }
}

// ─── MESSAGE HANDLER ─────────────────────────────────────────────────────────

// The server sends text and audio in SEPARATE turns:
//   turn A:  text  → audio_end  (no pcm)
//   turn B:  [binary pcm chunks] → audio_end  (no text)
//
// Strategy: buffer the latest text and consume it on the next audio turn.
let turnPcmChunks   = [];   // pcm for the current in-progress audio turn
let pendingLipText  = '';   // last received text, waiting for its paired audio turn

async function handleServerMessage(event) {
    if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);
        console.log('[server msg]', JSON.stringify(msg).substring(0, 200));

        if (msg.type === 'text') {
            // Accumulate — may span multiple text events before audio_end
            pendingLipText += msg.text;

        } else if (msg.type === 'interrupted') {
            pendingLipText = '';
            turnPcmChunks  = [];
            audioQueue     = [];
            isPlayingAudio = false;
            if (avatar) avatar.stop();

        } else if (msg.type === 'audio_end') {
            const chunks = turnPcmChunks;
            turnPcmChunks = [];

            if (chunks.length === 0) {
                // Text-only turn — log the text but nothing to speak yet
                const textToLog = pendingLipText.trim();
                if (textToLog) {
                    logChat(textToLog);
                    console.log('[audio_end] text-only turn, buffered for next audio:', JSON.stringify(textToLog));
                }
                // Do NOT clear pendingLipText — keep it for the upcoming audio turn
                return;
            }

            // Audio turn — consume the buffered text for lip sync
            const spokenText = pendingLipText.trim();
            pendingLipText = '';

            console.log('[audio_end] audio turn | spokenText:', JSON.stringify(spokenText),
                        '| pcm chunks:', chunks.length);

            if (chunks.length > 0) {
                let totalLen = 0;
                for (const c of chunks) totalLen += c.length;
                const flattened = new Int16Array(totalLen);
                let offset = 0;
                for (const c of chunks) { flattened.set(c, offset); offset += c.length; }

                const usedAvatar = await speakWithLipSync(flattened, spokenText);
                if (!usedAvatar) {
                    audioQueue.push(flattened);
                    playNextInQueue();
                }
            }

        } else if (msg.type === 'system') {
            logChat(`[System: ${msg.text}]`, 'System');
        }

    } else if (event.data instanceof ArrayBuffer) {
        turnPcmChunks.push(new Int16Array(event.data));
    }
}

// ─── STOP SESSION ────────────────────────────────────────────────────────────

function stopSession() {
    isStreaming         = false;
    pendingLipText      = '';
    turnPcmChunks       = [];
    audioQueue          = [];
    isPlayingAudio      = false;

    clearInterval(keepAliveInterval);
    keepAliveInterval = null;

    if (workletNode) workletNode.disconnect();
    if (micStream)   micStream.getTracks().forEach(t => t.stop());
    if (websocket)   websocket.close();
    if (audioCtx)    audioCtx.close();

    statusDiv.innerText    = 'Status: Disconnected';
    startStopBtn.disabled  = false;
    startStopBtn.innerText = 'Start Session';
}

// ─── CLEAR LOG ───────────────────────────────────────────────────────────────

function clearLog() {
    chatLog.innerHTML = '';
    lastLoggedText    = '';
}

// ─── INIT ────────────────────────────────────────────────────────────────────

startStopBtn.addEventListener('click', toggleSession);
clearLogBtn.addEventListener('click', clearLog);
startStopBtn.disabled = true;

window.onload = () => { initAvatar(); };