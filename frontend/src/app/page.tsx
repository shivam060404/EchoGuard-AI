import { getPlatformHealth } from "@/lib/api";

export const dynamic = "force-dynamic";

const services = [
  { id: "kafka", name: "Event transport", detail: "Apache Kafka" },
  { id: "mongo", name: "Operational store", detail: "MongoDB" },
];

export default async function Home() {
  const health = await getPlatformHealth();
  const ready = health?.status === "ready";

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark">E</span><span>EchoGuard <strong>AI</strong></span></div>
        <span className="phase">Phase 0 · Foundation</span>
      </header>

      <section className="hero">
        <p className="eyebrow">INCIDENT INTELLIGENCE PLATFORM</p>
        <h1>From scattered signals to a coordinated response.</h1>
        <p className="lead">The event and API foundation is taking shape. This page checks the local platform while the detection and review workflows are built.</p>
      </section>

      <section className="panel" aria-labelledby="status-heading">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">LIVE SYSTEM CHECK</p>
            <h2 id="status-heading">Platform status</h2>
          </div>
          <span className={`status ${ready ? "status-ready" : "status-degraded"}`}>
            <span className="status-dot" />{ready ? "Ready" : "Waiting for services"}
          </span>
        </div>
        <div className="service-grid">
          {services.map((service) => {
            const online = health?.dependencies[service.id] === true;
            return (
              <div className="service" key={service.id}>
                <div className="service-icon">{service.id === "kafka" ? "↗" : "◫"}</div>
                <div><h3>{service.name}</h3><p>{service.detail}</p></div>
                <span className={`service-indicator ${online ? "online" : "offline"}`} aria-label={online ? "online" : "offline"} />
              </div>
            );
          })}
        </div>
        <p className="footnote">Status checks the API connection to Kafka and MongoDB. Incident signals and agent actions arrive in later phases.</p>
      </section>

      <footer>EchoGuard AI · Local development environment</footer>
    </main>
  );
}
