// Exports the path to the extension for those using
// crsqlite in a Node.js environment.
import * as url from "url";
import { join } from "path";
const __dirname = url.fileURLToPath(new URL(".", import.meta.url));

export const extensionPath = join(__dirname, "dist", "crsqlite");
