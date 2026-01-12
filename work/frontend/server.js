const express = require("express");
const path = require("path");
const { Kafka } = require("kafkajs");

const app = express();

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

const kafka = new Kafka({
  clientId: "weather-dashboard",
  brokers: ["localhost:29092"]
});

const consumer = kafka.consumer({ groupId: "dashboard-group" });

let anomalies = [];
let realtime = [];

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: "weather_anomalies" });
  await consumer.subscribe({ topic: "weather_transformed" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const data = JSON.parse(message.value.toString());

      if (topic === "weather_anomalies") anomalies.unshift(data);
      if (topic === "weather_transformed") realtime.unshift(data);

      anomalies = anomalies.slice(0, 50);
      realtime = realtime.slice(0, 50);
    }
  });
}

startKafka();

// API endpoints
app.get("/api/realtime", (req, res) => res.json(realtime));
app.get("/api/anomalies", (req, res) => res.json(anomalies));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});


app.listen(3000, () => {
  console.log("Dashboard backend running on port 3000");
});
