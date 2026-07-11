import React, { useState, useRef } from 'react';
import kafkaService from '../service/kafkaService';

const KafkaProducer = () => {
  const [counts, setCounts] = useState({ funny: 0, serious: 0 });
  const [status, setStatus] = useState({ text: '', isError: false });
  const statusTimer = useRef(null);

  const showStatus = (text, isError) => {
    setStatus({ text, isError });
    clearTimeout(statusTimer.current);
    statusTimer.current = setTimeout(() => setStatus({ text: '', isError: false }), 3000);
  };

  const sendEvent = async (kind) => {
    try {
      const call =
        kind === 'funny'
          ? kafkaService.produceEventToFunnyTopic
          : kafkaService.produceEventToSeriousTopic;
      await call();
      setCounts((c) => ({ ...c, [kind]: c[kind] + 1 }));
      showStatus(`Event delivered to Kafka topic "${kind}"`, false);
    } catch (error) {
      console.error('Error sending event:', error);
      showStatus('Could not reach the producer service (is it running on port 8080?)', true);
    }
  };

  return (
    <div className="page">
      <header className="header">
        <h1>Real-Time Monitoring System</h1>
        <p>
          Each button click is sent to Kafka and shows up on the Grafana
          dashboard a few seconds later.
        </p>
      </header>

      <div className="cards">
        <div className="card">
          <h2>Kafka Mastery Course</h2>
          <p className="desc">
            Example "course" offer. Clicking simulates a user enrolling.
          </p>
          <span className="topic">
            publishes to topic <code>funny</code>
          </span>
          <button onClick={() => sendEvent('funny')}>
            Enroll Now <span className="count">{counts.funny}</span>
          </button>
        </div>

        <div className="card">
          <h2>Monitoring Pro Bundle</h2>
          <p className="desc">
            Example "product" offer. Clicking simulates a user purchasing.
          </p>
          <span className="topic">
            publishes to topic <code>serious</code>
          </span>
          <button onClick={() => sendEvent('serious')}>
            Buy Now <span className="count">{counts.serious}</span>
          </button>
        </div>
      </div>

      <div className="pipeline">
        Click → Producer (8080) → Kafka (9092) → Consumer → Prometheus → Grafana
      </div>

      <p className={`status${status.isError ? ' error' : ''}`}>{status.text}</p>
    </div>
  );
};

export default KafkaProducer;
