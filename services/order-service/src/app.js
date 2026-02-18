const express = require("express");
const cors = require("cors");

const orderRoutes = require("./routes/orderRoutes"); // 👈 THIS MUST BE HERE

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({ status: "Order Service is running" });
});


app.use("/orders", orderRoutes);

module.exports = app;
