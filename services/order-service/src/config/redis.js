const { createClient } = require("redis");

const redisClient = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("[Redis] Max reconnection attempts reached. Giving up.");
        return new Error("Redis max retries exceeded");
      }
      const delay = Math.min(retries * 100, 3000); // cap at 3 seconds
      console.warn(`[Redis] Reconnecting... attempt ${retries}, waiting ${delay}ms`);
      return delay;
    },
  },
});

redisClient.on("connect", () => console.log("[Redis] Connected"));
redisClient.on("error", (err) => console.error("[Redis] Error:", err.message));
redisClient.on("reconnecting", () => console.warn("[Redis] Reconnecting..."));

const connectRedis = async () => {
  if (!redisClient.isOpen) {
    await redisClient.connect();
  }
};

connectRedis().catch((err) =>
  console.error("[Redis] Initial connection failed:", err.message)
);

module.exports = redisClient;