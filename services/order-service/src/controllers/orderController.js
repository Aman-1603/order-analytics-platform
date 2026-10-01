const Order = require("../models/orderModel");
const { publishOrderCreated } = require("../kafka/producer");
const redisClient = require("../config/redis");
const { orderCreatedCounter, orderErrorCounter } = require("../metrics/metrics");

// POST /orders
const createOrderHandler = async (req, res) => {
  const { userId, items } = req.body;

  if (!userId || !items || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ message: "Missing or invalid required fields" });
  }

  try {
    // 1. Persist to MongoDB
    const order = await Order.create({ userId, items });

    // 2. Publish event to Kafka — fire and don't block the HTTP response
    await publishOrderCreated({
      orderId: order._id.toString(),
      userId: order.userId,
      items: order.items,
      status: order.status,
    });

    // 3. Increment Prometheus counter
    orderCreatedCounter.inc({ userId });

    return res.status(201).json(order);
  } catch (error) {
    console.error("createOrderHandler error:", error.message);
    orderErrorCounter.inc({ handler: "createOrder" });
    return res.status(500).json({ message: "Internal server error" });
  }
};

// GET /orders/:id
const getOrderHandler = async (req, res) => {
  const { id } = req.params;

  try {
    // 1. Check Redis cache first (cache-aside pattern)
    const cacheKey = `order:${id}`;
    const cached = await redisClient.get(cacheKey);

    if (cached) {
      console.log(`[CACHE HIT] order:${id}`);
      return res.status(200).json(JSON.parse(cached));
    }

    // 2. Cache miss — go to MongoDB
    console.log(`[CACHE MISS] order:${id} — fetching from DB`);
    const order = await Order.findById(id);

    if (!order) {
      return res.status(404).json({ message: "Order not found" });
    }

    // 3. Write to Redis with a 60-second TTL
    await redisClient.setEx(cacheKey, 60, JSON.stringify(order));

    return res.status(200).json(order);
  } catch (error) {
    console.error("getOrderHandler error:", error.message);
    orderErrorCounter.inc({ handler: "getOrder" });
    return res.status(500).json({ message: "Internal server error" });
  }
};

module.exports = {
  createOrderHandler,
  getOrderHandler,
};