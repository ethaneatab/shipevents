import anchors from "./generated/anchors.json";

type Anchor = { path: string; start: number; end: number };
const table = anchors as Record<string, Anchor>;

export const REPO_URL: string = import.meta.env.VITE_REPO_URL;
export const GIT_SHA: string = import.meta.env.VITE_GIT_SHA;

/** GitHub link to a @learn anchor, pinned to the commit this page was built from (never `main`). */
export function srcAnchor(id: string): { href: string; label: string } {
  const a = table[id];
  if (!a) return { href: "#missing-anchor-" + id, label: `missing anchor ${id}` };
  return { href: `${REPO_URL}/blob/${GIT_SHA}/${a.path}#L${a.start}-L${a.end}`, label: `${a.path}:${a.start}-${a.end}` };
}

export function anchorHref(id: string): string {
  return srcAnchor(id).href;
}
