import { Marked } from "marked";
import { anchorHref, GIT_SHA, REPO_URL } from "../links";

/** Relative doc links: problems.md opens the in-app tab; other docs go to the same commit on GitHub. */
function docHref(href: string): string {
  if (/^(https?:|#|\/)/.test(href)) return href;
  if (href.startsWith("problems.md")) return "/learn/problems";
  if (href.startsWith("interview.md")) return "/learn/interview";
  return `${REPO_URL}/blob/${GIT_SHA}/docs/${href.replace(/^\.\//, "")}`;
}

// Links written as [text](path#Lx-Ly "anchor:<id>") become commit-pinned GitHub links.
const md = new Marked({
  renderer: {
    link({ href, title, tokens }) {
      const text = this.parser.parseInline(tokens);
      if (title?.startsWith("anchor:")) {
        return `<a class="src-link" target="_blank" rel="noreferrer" data-anchor="${title.slice(7)}" href="${anchorHref(title.slice(7))}">${text}</a>`;
      }
      return `<a href="${docHref(href)}" target="_blank" rel="noreferrer">${text}</a>`;
    },
  },
});

export function Markdown({ text, className }: { text: string; className?: string }) {
  return <div className={`md ${className ?? ""}`} dangerouslySetInnerHTML={{ __html: md.parse(text) as string }} />;
}

export interface Section {
  title: string;
  body: string;
  children: Section[];
  id?: string;
}

/** Splits markdown into ## groups and ### items (the shape interview.md and problems.md use). */
export function sections(text: string, level: 2 | 3): { intro: string; items: Section[] } {
  const marker = "#".repeat(level) + " ";
  const parts = text.split(new RegExp(`^${marker}`, "m"));
  const intro = parts.shift() ?? "";
  return {
    intro,
    items: parts.map((p) => {
      const nl = p.indexOf("\n");
      const title = (nl < 0 ? p : p.slice(0, nl)).trim();
      const body = nl < 0 ? "" : p.slice(nl + 1);
      const id = body.match(/<!--\s*id:\s*([a-z0-9-]+)\s*-->/)?.[1];
      return { title, body, children: level === 2 ? sections(body, 3).items : [], ...(id ? { id } : {}) };
    }),
  };
}
