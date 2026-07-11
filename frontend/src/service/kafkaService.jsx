import axios from 'axios';

const API_URL = 'http://localhost:8080/producer';

const produceEventToFunnyTopic = () => {
  return axios.post(`${API_URL}/event/funny`);
};

const produceEventToSeriousTopic = () => {
  return axios.post(`${API_URL}/event/serious`);
};

export default {
  produceEventToFunnyTopic,
  produceEventToSeriousTopic,
};