import { Kafka } from 'kafkajs';
import { createClient } from 'redis';

// 1. Initialize Redis Client
const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('Redis Error', err));
await redisClient.connect();

// 2. Initialize Kafka Consumer
const kafka = new Kafka({
  clientId: 'fulfillment-service',
  brokers: ['localhost:9092']
});

// Consumers with the same groupId share the load (Consumer Group)
const consumer = kafka.consumer({ groupId: 'fulfillment-group' });
await consumer.connect();

// Subscribe to the topic
await consumer.subscribe({ topic: 'order_created', fromBeginning: true });

console.log('👷 Fulfillment Worker listening for events...');

// 3. Process Events
await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        const orderId = message.key.toString();
        const orderData = JSON.parse(message.value.toString());
        
        console.log(`\n[Worker] Received 'order_created' event for ID: ${orderId}`);
        console.log(`[Worker] Processing items: ${orderData.quantity}x ${orderData.item}...`);
        
        // Simulate a time-consuming business process (e.g., inventory deduction)
        await new Promise(resolve => setTimeout(resolve, 4000));
        
        // Update state in Redis to 'FULFILLED'
        orderData.status = 'FULFILLED';
        await redisClient.set(`order:${orderId}`, JSON.stringify(orderData));
        
        console.log(`[Worker] Order ${orderId} successfully FULFILLED!`);
    },
});
