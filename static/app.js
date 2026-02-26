import * as THREE from 'three';
import { TalkingHead } from 'talkinghead';

let avatar, websocket, audioCtx, micStream, processor;
let isStreaming = false;

// We buffer incoming audio chunks until the sentence is over to ensure perfect lip sync
let incomingAudioChunks = [];

const avatarView = document.getElementById('avatar-view');
const startStopBtn = document.getElementById('start-stop-btn');
const clearLogBtn = document.getElementById('clear-log-btn');
const statusDiv = document.getElementById('status');
const chatLog = document.getElementById('chat-log');

// 1. Initialize 3D Scene & TalkingHead
async function initAvatar() {
    avatar = new TalkingHead(avatarView, {
        cameraView: 'head',
        ttsEndpoint: null // We stream our own audio
    });

    try {
        // Load a default Ready Player Me avatar url. (You can place your own .glb in /static/)
        await avatar.showAvatar('https://models.readyplayer.me/64bfa15f0e72c63d7c3934a6.glb', (ev) => {
            if (ev.lengthComputable) {
                statusDiv.innerText = `Loading Avatar: ${Math.round((ev.loaded/ev.total)*100)}%`;
            }
        });
        statusDiv.innerText = "Avatar Ready. Click Start.";
        startStopBtn.disabled = false;
    } catch (error) {
        console.error("Error loading avatar:", error);
        statusDiv.innerText = "Failed to load Avatar.";
    }
}

// Helper: Append text to chat
function logChat(text, role="Aria") {
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
    startStopBtn.innerText = "Connecting...";

    try {
        // Init AudioContext at exactly 16000Hz (Bedrock Requirement)
        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        
        const source = audioCtx.createMediaStreamSource(micStream);
        processor = audioCtx.createScriptProcessor(2048, 1, 1);
        
        source.connect(processor);
        processor.connect(audioCtx.destination);

        // Connect WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        websocket = new WebSocket(`${protocol}//${window.location.host}/ws`);
        websocket.binaryType = "arraybuffer";

        websocket.onopen = () => {
            isStreaming = true;
            statusDiv.innerText = "Status: Listening...";
            startStopBtn.innerText = "Stop Session";
            startStopBtn.disabled = false;
        };

        // Stream Mic to Backend
        processor.onaudioprocess = (e) => {
            if (!isStreaming || websocket.readyState !== WebSocket.OPEN) return;
            const floatData = e.inputBuffer.getChannelData(0);
            const intData = new Int16Array(floatData.length);
            for (let i = 0; i < floatData.length; i++) {
                // Convert Float32 to Int16
                intData[i] = Math.max(-1, Math.min(1, floatData[i])) * 32767;
            }
            websocket.send(intData.buffer);
        };

        websocket.onmessage = handleServerMessage;

        websocket.onclose = () => {
            stopSession();
        };

    } catch (err) {
        console.error("Microphone/WS Error:", err);
        statusDiv.innerText = "Error: Check Mic Permissions.";
        startStopBtn.disabled = false;
    }
}

// 3. Handle messages from server
async function handleServerMessage(event) {
    if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);
        if (msg.type === "text") {
            logChat(msg.text);
        } else if (msg.type === "interrupted") {
            incomingAudioChunks = [];
            avatar.stop(); 
        } else if (msg.type === "audio_end") {
            if (incomingAudioChunks.length > 0) {
                playBufferedAudio(incomingAudioChunks);
                incomingAudioChunks = []; 
            }
        } else if (msg.type === "system") {
            logChat(`[System: ${msg.text}]`, "System");
        }
    } else if (event.data instanceof ArrayBuffer) {
        // Binary audio chunk (24kHz Int16 PCM)
        const int16View = new Int16Array(event.data);
        incomingAudioChunks.push(int16View);
    }
}

// 4. Convert PCM chunks to WAV, and give to TalkingHead
function playBufferedAudio(chunks) {
    // Calculate total length
    let totalLength = 0;
    for (const chunk of chunks) totalLength += chunk.length;
    
    // Flatten chunks
    const flattened = new Int16Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
        flattened.set(chunk, offset);
        offset += chunk.length;
    }

    // Wrap in WAV Blob (24000Hz, 1 channel, 16bit)
    const wavBlob = encodeWAV(flattened, 24000);
    const audioUrl = URL.createObjectURL(wavBlob);
    
    // Create HTMLAudioElement and pass to Avatar
    const audio = new Audio(audioUrl);
    
    // Speak using TalkingHead (automatically generates lip-sync)
    avatar.speakAudio(audio);
}

// Utility: Build WAV header to make the raw PCM playable by the browser
function encodeWAV(samples, sampleRate) {
    const buffer = new ArrayBuffer(44 + samples.length * 2);
    const view = new DataView(buffer);
    const writeString = (view, offset, string) => {
        for (let i = 0; i < string.length; i++) {
            view.setUint8(offset + i, string.charCodeAt(i));
        }
    };
    writeString(view, 0, 'RIFF');
    view.setUint32(4, 36 + samples.length * 2, true);
    writeString(view, 8, 'WAVE');
    writeString(view, 12, 'fmt ');
    view.setUint32(16, 16, true);
    view.setUint16(20, 1, true); // PCM
    view.setUint16(22, 1, true); // Mono
    view.setUint32(24, sampleRate, true);
    view.setUint32(28, sampleRate * 2, true);
    view.setUint16(32, 2, true);
    view.setUint16(34, 16, true);
    writeString(view, 36, 'data');
    view.setUint32(40, samples.length * 2, true);
    let offset = 44;
    for (let i = 0; i < samples.length; i++, offset += 2) {
        view.setInt16(offset, samples[i], true);
    }
    return new Blob([view], { type: 'audio/wav' });
}

function stopSession() {
    isStreaming = false;
    if (processor) processor.disconnect();
    if (micStream) micStream.getTracks().forEach(t => t.stop());
    if (websocket) websocket.close();
    statusDiv.innerText = "Status: Disconnected";
    startStopBtn.disabled = false;
    startStopBtn.innerText = "Start Session";
}

function clearLog() {
    chatLog.innerHTML = '';
}

// Bind Events
startStopBtn.addEventListener('click', toggleSession);
clearLogBtn.addEventListener('click', clearLog);
startStopBtn.disabled = true;

// Init
window.onload = () => {
    initAvatar();
};