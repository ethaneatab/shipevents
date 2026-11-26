import text from "../../../docs/interview.md?raw";
import { Markdown, sections } from "../components/Markdown";

export function Interview() {
  const { intro, items } = sections(text, 2);
  return (
    <div className="qa" data-testid="interview">
      <section className="panel"><Markdown text={intro} /></section>
      {items.map((g) => (
        <section key={g.title} className="panel">
          <h2>{g.title}</h2>
          {g.children.map((q, i) => (
            <details key={q.title} className="q" data-testid={`q-${g.title.slice(0, 2)}-${i}`}>
              <summary>{q.title}</summary>
              <Markdown text={q.body} className="answer" />
            </details>
          ))}
        </section>
      ))}
    </div>
  );
}
