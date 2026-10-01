const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "order-service",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
});

const producer = kafka.producer();
let isConnected = false;

const connectProducer = async () => {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    console.log("[Kafka Producer] Connected");
  }
};

const publishOrderCreated = async (orderPayload) => {
  try {
    await connectProducer();

    await producer.send({
      topic: "order.created",
      messages: [
        {
          key: orderPayload.orderId,         // partition by orderId — same order always hits same partition
          value: JSON.stringify(orderPayload),
          headers: {
            eventType: "order.created",
            source: "order-service",
            timestamp: Date.now().toString(),
          },
        },
      ],
    });

    console.log(`[Kafka Producer] Published order.created for orderId: ${orderPayload.orderId}`);
  } catch (error) {
    console.error("[Kafka Producer] Failed to publish:", error.message);
    throw error; // bubble up so the controller can handle it
  }
};

const disconnectProducer = async () => {
  if (isConnected) {
    await producer.disconnect();
    isConnected = false;
    console.log("[Kafka Producer] Disconnected");
  }
};

module.exports = { publishOrderCreated, disconnectProducer };