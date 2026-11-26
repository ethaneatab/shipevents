import { execSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, createReadStream } from "node:fs";
import path from "node:path";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

const root = path.resolve(__dirname, "..");
const repo = JSON.parse(readFileSync(path.join(root, "repo.config.json"), "utf8")) as { repoUrl: string };
const gitSha = process.env.VITE_GIT_SHA || (() => {
  try { return execSync("git rev-parse HEAD", { cwd: root }).toString().trim(); } catch { return "HEAD"; }
})();
const videosDir = path.join(root, "docs/videos");

/** Serves docs/videos at /videos in dev (404 when missing, never the SPA fallback) and copies it into dist on build. */
function videos(): Plugin {
  const serve = (req: { url?: string }, res: any, next: () => void) => {
    if (!req.url?.startsWith("/videos/")) return next();
    const rel = decodeURIComponent(req.url.split("?")[0]!.slice("/videos/".length));
    const file = path.join(videosDir, rel);
    if (!file.startsWith(videosDir) || !existsSync(file) || !statSync(file).isFile()) {
      res.statusCode = 404;
      return res.end("video not generated yet");
    }
    res.setHeader("content-type", "video/mp4");
    createReadStream(file).pipe(res);
  };
  return {
    name: "shipevents-videos",
    configureServer: (s) => void s.middlewares.use(serve),
    configurePreviewServer: (s) => void s.middlewares.use(serve),
    closeBundle() {
      const copy = (from: string, to: string) => {
        if (!existsSync(from)) return;
        mkdirSync(to, { recursive: true });
        for (const n of readdirSync(from)) {
          const p = path.join(from, n);
          if (statSync(p).isDirectory()) copy(p, path.join(to, n));
          else if (n.endsWith(".mp4")) copyFileSync(p, path.join(to, n));
        }
      };
      copy(videosDir, path.resolve(__dirname, "dist/videos"));
    },
  };
}

const proxy = {
  "/api": { target: "http://127.0.0.1:8901", rewrite: (p: string) => p.replace(/^\/api/, "") },
  "/proc": { target: "http://127.0.0.1:8900", rewrite: (p: string) => p.replace(/^\/proc/, "") },
};

export default defineConfig({
  plugins: [react(), videos()],
  define: {
    "import.meta.env.VITE_REPO_URL": JSON.stringify(process.env.VITE_REPO_URL || repo.repoUrl),
    "import.meta.env.VITE_GIT_SHA": JSON.stringify(gitSha),
  },
  server: { port: 5190, strictPort: true, host: "127.0.0.1", proxy, fs: { allow: [root] } },
  preview: { port: 5190, strictPort: true, host: "127.0.0.1", proxy },
});
