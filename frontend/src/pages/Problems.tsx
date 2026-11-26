import text from "../../../docs/problems.md?raw";
import { Markdown, sections } from "../components/Markdown";
import { VideoSlot } from "../components/VideoSlot";

export function Problems() {
  const { intro, items } = sections(text, 2);
  return (
    <div className="qa" data-testid="problems">
      <section className="panel"><Markdown text={intro} /></section>
      {items.map((p) => (
        <details key={p.title} className="panel q problem" data-testid={`problem-${p.id}`}>
          <summary>{p.title}</summary>
          <VideoSlot src={`/videos/problems/${p.id}.mp4`} testId={`problem-video-${p.id}`} />
          <Markdown text={p.body} className="answer" />
        </details>
      ))}
    </div>
  );
}
