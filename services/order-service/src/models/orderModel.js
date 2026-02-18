const orders = [];

const createOrder = (orderData) => {
  const newOrder = {
    id: Date.now().toString(),
    ...orderData,
    status: "CREATED",
    createdAt: new Date(),
  };

  orders.push(newOrder);
  return newOrder;
};

const getOrderById = (id) => {
  return orders.find((order) => order.id === id);
};

module.exports = {
  createOrder,
  getOrderById,
};
