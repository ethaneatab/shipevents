import { useEffect, useState } from "react";
import { Track } from "./pages/Track";
import { Ops } from "./pages/Ops";
import { Learn } from "./pages/Learn";
import { Interview } from "./pages/Interview";
import { Problems } from "./pages/Problems";

export function navigate(to: string) {
  window.history.pushState({}, "", to);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function Link(props: { to: string; children: React.ReactNode; className?: string; testId?: string }) {
  return (
    <a href={props.to} className={props.className} data-testid={props.testId} onClick={(e) => { e.preventDefault(); navigate(props.to); }}>
      {props.children}
    </a>
  );
}

function usePath(): string {
  const [p, setP] = useState(window.location.pathname);
  useEffect(() => {
    const on = () => setP(window.location.pathname);
    window.addEventListener("popstate", on);
    return () => window.removeEventListener("popstate", on);
  }, []);
  return p;
}

export function App() {
  const path = usePath();
  const top = path.startsWith("/learn") ? "learn" : path.startsWith("/ops") ? "ops" : "track";
  let page: React.ReactNode;
  if (path === "/learn/interview") page = <Interview />;
  else if (path === "/learn/problems") page = <Problems />;
  else if (path.startsWith("/learn")) page = <Learn />;
  else if (path.startsWith("/ops")) page = <Ops />;
  else page = <Track />;
  return (
    <div className="shell">
      <header className="masthead">
        <div className="brand"><span className="brand-mark">▣</span> shipevents</div>
        <nav>
          <Link to="/track" className={top === "track" ? "on" : ""}>运单追踪</Link>
          <Link to="/ops" className={top === "ops" ? "on" : ""}>运维</Link>
          <Link to="/learn" className={top === "learn" ? "on" : ""} testId="nav-learn">教学</Link>
        </nav>
      </header>
      {top === "learn" && (
        <div className="subnav" role="tablist">
          <Link to="/learn" className={path === "/learn" || path === "/learn/" ? "on" : ""} testId="tab-flow">处理流程</Link>
          <Link to="/learn/interview" className={path === "/learn/interview" ? "on" : ""} testId="tab-interview">面试题</Link>
          <Link to="/learn/problems" className={path === "/learn/problems" ? "on" : ""} testId="tab-problems">日常问题</Link>
        </div>
      )}
      <main>{page}</main>
    </div>
  );
}
