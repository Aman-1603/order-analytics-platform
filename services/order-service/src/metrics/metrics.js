const client = require("prom-client");

// Auto-collect default Node.js metrics (CPU, memory, event loop lag)
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// ─── SIGNAL 1: LATENCY ───────────────────────────────────────────────────────
// How long requests are taking
const httpRequestDuration = new client.Histogram({
  name: "http_request_duration_seconds",
  help: "Duration of HTTP requests in seconds",
  labelNames: ["method", "route", "status_code"],
  buckets: [0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5], // in seconds
});

// ─── SIGNAL 2: TRAFFIC ───────────────────────────────────────────────────────
// How many requests are coming in
const httpRequestTotal = new client.Counter({
  name: "http_requests_total",
  help: "Total number of HTTP requests",
  labelNames: ["method", "route", "status_code"],
});

// ─── SIGNAL 3: ERRORS ────────────────────────────────────────────────────────
// How many things are failing
const orderErrorCounter = new client.Counter({
  name: "order_errors_total",
  help: "Total number of order processing errors",
  labelNames: ["handler"],
});

const kafkaDLQCounter = new client.Counter({
  name: "kafka_dlq_messages_total",
  help: "Total number of messages sent to the Dead Letter Queue",
  labelNames: ["topic"],
});

// ─── SIGNAL 4: SATURATION ────────────────────────────────────────────────────
// How "full" the system is
const kafkaConsumerLag = new client.Gauge({
  name: "kafka_consumer_lag",
  help: "Number of messages behind in the Kafka consumer group",
  labelNames: ["topic", "partition"],
});

// ─── BUSINESS METRIC (bonus — great to mention in interviews) ────────────────
const orderCreatedCounter = new client.Counter({
  name: "orders_created_total",
  help: "Total number of orders successfully created",
  labelNames: ["userId"],
});

// Register all metrics
register.registerMetric(httpRequestDuration);
register.registerMetric(httpRequestTotal);
register.registerMetric(orderErrorCounter);
register.registerMetric(kafkaDLQCounter);
register.registerMetric(kafkaConsumerLag);
register.registerMetric(orderCreatedCounter);

module.exports = {
  register,
  httpRequestDuration,
  httpRequestTotal,
  orderErrorCounter,
  kafkaDLQCounter,
  kafkaConsumerLag,
  orderCreatedCounter,
};