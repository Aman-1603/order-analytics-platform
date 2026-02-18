const { createOrder, getOrderById } = require("../models/orderModel.js");

const createOrderHandler = (req, res) => {
  const { userId, items } = req.body;

  if (!userId || !items) {
    return res.status(400).json({ message: "Missing required fields" });
  }

  const order = createOrder({ userId, items });

  res.status(201).json(order);
};

const getOrderHandler = (req, res) => {
  const { id } = req.params;

  const order = getOrderById(id);

  if (!order) {
    return res.status(404).json({ message: "Order not found" });
  }

  res.status(200).json(order);
};

module.exports = {
  createOrderHandler,
  getOrderHandler,
};
