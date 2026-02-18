const express = require("express");
const router = express.Router();
const {
  createOrderHandler,
  getOrderHandler,
} = require("../controllers/orderController");

router.post("/", createOrderHandler);
router.get("/:id", getOrderHandler);

module.exports = router;
