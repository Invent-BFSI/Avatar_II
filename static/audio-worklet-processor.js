// static/audio-worklet-processor.js
class AudioSenderProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.port.onmessage = (event) => {
      // We can receive messages from the main thread here if needed
    };
  }

  process(inputs, outputs, parameters) {
    const input = inputs[0];
    if (input.length > 0) {
      const pcmData = input[0];
      const int16Data = new Int16Array(pcmData.length);
      for (let i = 0; i < pcmData.length; i++) {
        int16Data[i] = Math.max(-1, Math.min(1, pcmData[i])) * 32767;
      }
      this.port.postMessage(int16Data.buffer, [int16Data.buffer]);
    }
    return true; // Keep the processor alive
  }
}

registerProcessor('audio-sender-processor', AudioSenderProcessor);
