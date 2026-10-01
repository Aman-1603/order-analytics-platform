const mongoose = require("mongoose");

const orderSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      trim: true,
    },
    items: {
      type: [String],
      required: true,
      validate: {
        validator: (arr) => arr.length > 0,
        message: "Order must contain at least one item",
      },
    },
    status: {
      type: String,
      enum: ["CREATED", "PROCESSING", "COMPLETED", "FAILED"],
      default: "CREATED",
    },
    retryCount: {
      type: Number,
      default: 0,
    },
    errorMessage: {
      type: String,
      default: null,
    },
  },
  {
    timestamps: true, // adds createdAt + updatedAt automatically
  }
);

// Index for fast lookup by userId (useful when we query orders by user)
orderSchema.index({ userId: 1 });
orderSchema.index({ status: 1 });

module.exports = mongoose.model("Order", orderSchema);