let tempChart = null;
let windChart = null;

function initCharts() {
  const tempCanvas = document.getElementById("tempChart");
  const windCanvas = document.getElementById("windChart");

  if (!tempCanvas || !windCanvas) return;

  tempChart = new Chart(tempCanvas.getContext("2d"), {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label: "Température (°C)",
        data: [],
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      animation: false
    }
  });

  windChart = new Chart(windCanvas.getContext("2d"), {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label: "Vent (m/s)",
        data: [],
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      animation: false
    }
  });
}

async function refresh() {
  if (!tempChart || !windChart) return;

  const realtimeEl = document.getElementById("realtime");
  const anomaliesEl = document.getElementById("anomalies");

  if (!realtimeEl || !anomaliesEl) return;

  const realtime = await fetch("/api/realtime").then(r => r.json());
  const anomalies = await fetch("/api/anomalies").then(r => r.json());

  // 🔹 Liste temps réel
  realtimeEl.innerHTML = realtime.slice(0, 10).map(r =>
    `<li>📍 ${r.city} — 🌡 ${r.temperature}°C • 💨 ${r.windspeed} m/s</li>`
  ).join("");

  // 🔹 Liste anomalies
  anomaliesEl.innerHTML =
    anomalies.length === 0
      ? "<li>✅ Aucune anomalie détectée</li>"
      : anomalies.slice(0, 10).map(a =>
          `<li>⚠️ ${a.city} — ${a.variable} : ${a.observed_value}</li>`
        ).join("");

  // 🔹 Charts (20 derniers points)
  const data = realtime.slice(0, 20).reverse();

  tempChart.data.labels = data.map((_, i) => i);
  tempChart.data.datasets[0].data = data.map(r => r.temperature);
  tempChart.update();

  windChart.data.labels = data.map((_, i) => i);
  windChart.data.datasets[0].data = data.map(r => r.windspeed);
  windChart.update();
}

document.addEventListener("DOMContentLoaded", () => {
  initCharts();
  refresh();
  setInterval(refresh, 3000);
});
