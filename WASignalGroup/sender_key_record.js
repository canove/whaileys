const SenderKeyState = require('./sender_key_state');

class SenderKeyRecord {
    MAX_STATES = 5;
  
    constructor(serialized) {
      this.senderKeyStates = [];
  
      if (serialized) {
        const list = serialized;
        for (let i = 0; i < list.length; i++) {
          const structure = list[i];
          // Skip empty/corrupt states: they accumulate in the array when a
          // key rotation is interrupted (retry receipt, reconnect), and
          // getSenderKeyState() would return one of them, making
          // getSenderChainKey() throw "Cannot read properties of null
          // (reading 'iteration')" on every subsequent group send.
          if (!structure || !structure.senderChainKey) continue;
          this.senderKeyStates.push(
            new SenderKeyState(null, null, null, null, null, null, structure)
          );
        }
      }
    }
  
    isEmpty() {
      return this.senderKeyStates.length === 0;
    }
  
    getSenderKeyState(keyId) {
      if (!keyId && this.senderKeyStates.length) {
        // Most recent VALID state instead of a blind [0]: defensive, since
        // an empty state can still reach the array through paths that do
        // not go through deserialization.
        for (let i = this.senderKeyStates.length - 1; i >= 0; i--) {
          const state = this.senderKeyStates[i];
          if (
            state &&
            state.senderKeyStateStructure &&
            state.senderKeyStateStructure.senderChainKey
          ) {
            return state;
          }
        }
        return this.senderKeyStates[0];
      }
      for (let i = 0; i < this.senderKeyStates.length; i++) {
        const state = this.senderKeyStates[i];
        if (state.getKeyId() === keyId) {
          return state;
        }
      }
      throw new Error(`No keys for: ${keyId}`);
    }
  
    addSenderKeyState(id, iteration, chainKey, signatureKey) {
      this.senderKeyStates.push(new SenderKeyState(id, iteration, chainKey, null, signatureKey));
    }
  
    setSenderKeyState(id, iteration, chainKey, keyPair) {
      this.senderKeyStates.length = 0;
      this.senderKeyStates.push(new SenderKeyState(id, iteration, chainKey, keyPair));
    }
  
    serialize() {
      const recordStructure = [];
      for (let i = 0; i < this.senderKeyStates.length; i++) {
        const senderKeyState = this.senderKeyStates[i];
        recordStructure.push(senderKeyState.getStructure());
      }
      return recordStructure;
    }
  }
  
  module.exports = SenderKeyRecord;