import { DEFAULT_CONNECTION_CONFIG } from "../Defaults";
import { UserFacingSocketConfig } from "../Types";
import { makeNewsletterSocket as _makeSocket } from "./newsletter";

// export the last socket layer
const makeWASocket = (config: UserFacingSocketConfig) =>
  _makeSocket({
    ...DEFAULT_CONNECTION_CONFIG,
    ...config
  });

export default makeWASocket;
