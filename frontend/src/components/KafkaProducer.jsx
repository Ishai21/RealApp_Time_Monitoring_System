import React from 'react';
import kafkaService from '../service/kafkaService';

const KafkaProducer = () => {
  const handleFunnySubmit = async () => {
    try {
      const response = await kafkaService.produceEventToFunnyTopic();
      console.log('Funny button click event sent successfully:', response.data);
    } catch (error) {
      console.error('Error sending funny button click event:', error);
    }
  };

  const handleSeriousSubmit = async () => {
    try {
      const response = await kafkaService.produceEventToSeriousTopic();
      console.log('Serious button click event sent successfully:', response.data);
    } catch (error) {
      console.error('Error sending serious button click event:', error);
    }
  };

  return (
    <div>
      <h1>Kafka Producer</h1>
      <br />
      <button onClick={handleFunnySubmit}>Enroll Now</button>
      <button onClick={handleSeriousSubmit}>Buy Now</button>
    </div>
  );
};

export default KafkaProducer;