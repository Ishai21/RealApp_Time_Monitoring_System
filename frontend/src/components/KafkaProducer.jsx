import React, { useState, useRef } from 'react';
import kafkaService from '../service/kafkaService';

// Stages shown in the pipeline strip; they light up green after a
// successful click to visualize the event's journey.
const STAGES = ['Click', 'Producer :8080', 'Kafka :9092', 'Consumer', 'Prometheus', 'Grafana'];

const KafkaProducer = () => {
  const [counts, setCounts] = useState({ funny: 0, serious: 0 });
  const [toasts, setToasts] = useState([]);
  const [litStages, setLitStages] = useState(0);
  const toastId = useRef(0);

  const showToast = (text, isError) => {
    const id = ++toastId.current;
    setToasts((t) => [...t, { id, text, isError }]);
    setTimeout(() => setToasts((t) => t.filter((toast) => toast.id !== id)), 3000);
  };

  // Light the pipeline stages one by one, left to right.
  const animatePipeline = () => {
    setLitStages(0);
    STAGES.forEach((_, i) => {
      setTimeout(() => setLitStages(i + 1), i * 180);
    });
    setTimeout(() => setLitStages(0), STAGES.length * 180 + 1200);
  };

  const sendEvent = async (kind) => {
    try {
      const call =
        kind === 'funny'
          ? kafkaService.produceEventToFunnyTopic
          : kafkaService.produceEventToSeriousTopic;
      await call();
      setCounts((c) => ({ ...c, [kind]: c[kind] + 1 }));
      showToast(`✓ Event delivered to Kafka topic "${kind}"`, false);
      animatePipeline();
    } catch (error) {
      console.error('Error sending event:', error);
      showToast('✗ Could not reach the producer service (is it running on :8080?)', true);
    }
  };

  return (
    <div className="page">
      <header className="header">
        <div className="badge">
          <span className="dot" />
          LIVE EVENT PIPELINE
        </div>
        <h1>Real-Time Monitoring System</h1>
        <p>
          Every click below travels through Kafka and lands on a Grafana dashboard
          within seconds. Go ahead — generate some traffic.
        </p>
      </header>

      <div className="cards">
        <div className="card funny">
          <span className="emoji" role="img" aria-label="graduation cap">🎓</span>
          <h2>Kafka Mastery Course</h2>
          <p className="desc">
            Master event streaming from producers to consumer groups.
            Each enrollment fires a real event through the pipeline.
          </p>
          <span className="topic">
            publishes to <code>funny</code>
          </span>
          <button onClick={() => sendEvent('funny')}>
            Enroll Now <span className="count">{counts.funny}</span>
          </button>
        </div>

        <div className="card serious">
          <span className="emoji" role="img" aria-label="chart">📈</span>
          <h2>Monitoring Pro Bundle</h2>
          <p className="desc">
            Prometheus + Grafana toolkit for real-time dashboards.
            Each purchase becomes a data point on the graph.
          </p>
          <span className="topic">
            publishes to <code>serious</code>
          </span>
          <button onClick={() => sendEvent('serious')}>
            Buy Now <span className="count">{counts.serious}</span>
          </button>
        </div>
      </div>

      <div className="pipeline">
        {STAGES.map((stage, i) => (
          <React.Fragment key={stage}>
            {i > 0 && <span className="arrow">→</span>}
            <span className={`stage${i < litStages ? ' lit' : ''}`}>{stage}</span>
          </React.Fragment>
        ))}
      </div>

      <footer className="footer">
        Watch the results live: Prometheus query <code>rate(kafka_funny_events_total[1m])</code>{' '}
        or the Grafana dashboard.
      </footer>

      <div className="toasts">
        {toasts.map((t) => (
          <div key={t.id} className={`toast${t.isError ? ' error' : ''}`}>
            {t.text}
          </div>
        ))}
      </div>
    </div>
  );
};

export default KafkaProducer;
