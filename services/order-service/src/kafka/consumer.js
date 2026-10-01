const { Kafka } = require("kafkajs");
const Order = require("../models/orderModel");

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "order-processing-group" });
const dlqProducer = kafka.producer(); // separate producer just for DLQ writes

const TOPICS = {
  MAIN: "order.created",
  DLQ: "order.created.dlq",
};

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 500;

// Exponential backoff: 500ms, 1000ms, 2000ms
const getBackoffDelay = (attempt) => BASE_DELAY_MS * Math.pow(2, attempt);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Core business logic — update order status to PROCESSING
const processOrder = async (payload) => {
  const { orderId, userId, items } = payload;

  const order = await Order.findById(orderId);
  if (!order) throw new Error(`Order ${orderId} not found in DB`);

  order.status = "PROCESSING";
  await order.save();

  console.log(`[Consumer] Order ${orderId} status → PROCESSING`);
};

// Send to DLQ after all retries exhausted
const sendToDLQ = async (originalMessage, payload, errorMessage) => {
  await dlqProducer.send({
    topic: TOPICS.DLQ,
    messages: [
      {
        key: payload.orderId,
        value: JSON.stringify({
          ...payload,
          failedAt: new Date().toISOString(),
          reason: errorMessage,
        }),
        headers: {
          originalTopic: TOPICS.MAIN,
          errorMessage,
          retriesExhausted: String(MAX_RETRIES),
        },
      },
    ],
  });

  // Mark order as FAILED in MongoDB
  await Order.findByIdAndUpdate(payload.orderId, {
    status: "FAILED",
    errorMessage,
    retryCount: MAX_RETRIES,
  });

  console.error(`[DLQ] Order ${payload.orderId} sent to DLQ. Reason: ${errorMessage}`);
};

// Retry wrapper with exponential backoff
const processWithRetry = async (payload, originalMessage) => {
  let lastError;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      await processOrder(payload);

      // Update retryCount in DB so we can observe it
      if (attempt > 0) {
        await Order.findByIdAndUpdate(payload.orderId, { retryCount: attempt });
      }

      return; // success — exit retry loop
    } catch (error) {
      lastError = error;
      const delay = getBackoffDelay(attempt);
      console.warn(
        `[Consumer] Attempt ${attempt + 1}/${MAX_RETRIES} failed for order ${payload.orderId}. ` +
        `Retrying in ${delay}ms... Error: ${error.message}`
      );
      await sleep(delay);
    }
  }

  // All retries exhausted — send to DLQ
  await sendToDLQ(originalMessage, payload, lastError.message);
};

const startConsumer = async () => {
  await consumer.connect();
  await dlqProducer.connect();
  console.log("[Kafka Consumer] Connected");

  await consumer.subscribe({ topic: TOPICS.MAIN, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let payload;
      try {
        payload = JSON.parse(raw);
      } catch (e) {
        console.error("[Consumer] Failed to parse message:", raw);
        return; // malformed message — skip, don't retry
      }

      console.log(`[Consumer] Received order.created for orderId: ${payload.orderId}`);
      await processWithRetry(payload, message);
    },
  });
};

const stopConsumer = async () => {
  await consumer.disconnect();
  await dlqProducer.disconnect();
  console.log("[Kafka Consumer] Disconnected");
};

module.exports = { startConsumer, stopConsumer };