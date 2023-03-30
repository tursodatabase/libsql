/**
 * 1. Checks the current OS and CPU architecture
 * 2. Copies pre-built binaries from the `binaries` directory to the `dist` directory if one exists
 * 3. Otherwise, lets the standard install process via `make` take over
 */
import { join } from "path";
import fs from "fs";
import https from "https";
import pkg from "./package.json" assert { type: "json" };
import { exec } from "child_process";
let { version } = pkg;

let arch = process.arch;
let os = process.platform;
let ext = "unknown";
version = "v" + version;

if (process.env.CRSQLITE_NOPREBUILD) {
  console.log("CRSQLITE_NOPREBUILD env variable is set. Building from source.");
  buildFromSource();
} else {
  // todo: check msys?
  if (["win32", "cygwin"].includes(process.platform)) {
    os = "windows";
  }

  // manual ovverides for testing
  // arch = "x86_64";
  // os = "linux";
  // version = "prebuild-test.11";

  switch (os) {
    case "darwin":
      ext = "dylib";
      break;
    case "linux":
      ext = "so";
      break;
    case "windows":
      ext = "dll";
      break;
  }

  switch (arch) {
    case "x64":
      arch = "x86_64";
      break;
    case "arm64":
      arch = "aarch64";
      break;
  }

  const binaryUrl = `https://github.com/vlcn-io/cr-sqlite/releases/download/${version}/crsqlite-${os}-${arch}.${ext}`;
  console.log(`Look for prebuilt binary from ${binaryUrl}`);
  const distPath = join("dist", `crsqlite.${ext}`);

  if (!fs.existsSync(join(".", "dist"))) {
    fs.mkdirSync(join(".", "dist"));
  }

  if (fs.existsSync(distPath)) {
    console.log("Binary already present and installed.");
    process.exit(0);
  }

  // download the file at the url, if it exists
  let redirectCount = 0;
  function get(url, cb) {
    https.get(url, (res) => {
      if (res.statusCode === 302 || res.statusCode === 301) {
        ++redirectCount;
        if (redirectCount > 5) {
          throw new Error("Too many redirects");
        }
        get(res.headers.location, cb);
      } else if (res.statusCode === 200) {
        cb(res);
      } else {
        cb(null);
      }
    });
  }

  get(binaryUrl, (res) => {
    if (res == null) {
      console.log("No prebuilt binary available. Building from source.");
      buildFromSource();
      return;
    }

    const file = fs.createWriteStream(distPath);
    res.pipe(file);
    file.on("finish", () => {
      file.close();
      console.log("Prebuilt binary downloaded");
      process.exit(0);
    });
  });
}

function buildFromSource() {
  console.log("Building from source");
  exec("make loadable", (err, stdout, stderr) => {
    if (err) {
      console.log("Error building from source");
      console.log(err.message);
      process.exit(1);
    }
    if (stderr) {
      console.log(stderr);
    }
    console.log("Built from source");
    console.log(stdout);
    process.exit(0);
  });
}
