import makeWASocket from "./Socket";

export * from "../WAProto";
export * from "./Utils";
export * from "./Types";
export * from "./Store";
export * from "./Defaults";
export * from "./WABinary";
export * from "./WAUSync";

export type WASocket = ReturnType<typeof makeWASocket>;

export default makeWASocket;
