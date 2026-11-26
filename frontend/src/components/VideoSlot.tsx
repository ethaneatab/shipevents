import { useEffect, useState } from "react";

/** Plays /videos/... if the file exists; otherwise says it has not been generated. Never throws. */
export function VideoSlot({ src, testId }: { src: string; testId?: string }) {
  const [state, setState] = useState<"checking" | "ready" | "missing">("checking");
  useEffect(() => {
    let alive = true;
    fetch(src, { method: "HEAD" })
      .then((r) => alive && setState(r.ok && (r.headers.get("content-type") ?? "").startsWith("video") ? "ready" : "missing"))
      .catch(() => alive && setState("missing"));
    return () => { alive = false; };
  }, [src]);
  if (state === "ready") return <video className="video" src={src} controls preload="metadata" data-testid={testId} />;
  return <div className="video-missing" data-testid={testId} data-src={src}>{state === "checking" ? "检查视频…" : "视频待生成"}</div>;
}
