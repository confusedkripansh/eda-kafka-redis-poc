import express from 'express';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import crypto from 'crypto';

const app = express();
app.use(express.json());

// 1. Initialize Redis Client
const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('Redis Error', err));
await redisClient.connect();

// 2. Initialize Kafka Producer
const kafka = new Kafka({
  clientId: 'order-api-service',
  brokers: ['localhost:9092']
});
const producer = kafka.producer();
await producer.connect();

// 3. Endpoint to Create an Order
app.post('/orders', async (req, res) => {
    const { item, quantity } = req.body;
    const orderId = crypto.randomUUID();
    
    const orderData = { orderId, item, quantity, status: 'PENDING' };
    
    // Save initial state to Redis
    await redisClient.set(`order:${orderId}`, JSON.stringify(orderData));
    
    // Emit Event to Kafka
    await producer.send({
        topic: 'order_created',
        messages: [{ key: orderId, value: JSON.stringify(orderData) }],
    });
    
    console.log(`[API] Order ${orderId} created and event published.`);
    res.status(202).json({ message: 'Order accepted for processing', orderId });
});

// 4. Endpoint to Check Order Status (Reads from Redis)
app.get('/orders/:id', async (req, res) => {
    const data = await redisClient.get(`order:${req.params.id}`);
    if (!data) return res.status(404).json({ error: 'Order not found' });
    
    res.json(JSON.parse(data));
});

app.listen(3000, () => console.log('🚀 Order API running on http://localhost:3000'));
