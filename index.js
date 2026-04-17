const { Kafka } = require('kafkajs');
const sql = require('mssql');
require('dotenv').config();

// Kafka config (HARDCODED)
const kafka = new Kafka({
  clientId: 'car-consumer',
  brokers: ['my-cluster-kafka-bootstrap.kafka.svc:9092'],
});

const consumer = kafka.consumer({
  groupId: 'car-group',
});

// MSSQL config
const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  port: parseInt(process.env.DB_PORT, 10),
  options: {
    encrypt: true,                // REQUIRED for your DB
    trustServerCertificate: true // Fix self-signed cert error
  },
};

let pool;

// Initialize DB
async function initDB() {
  try {
    pool = await sql.connect(dbConfig);
    console.log('✅ Connected to MSSQL');
  } catch (err) {
    console.error('❌ DB Connection Failed:', err);
    process.exit(1);
  }
}

// UPSERT function
async function upsertCarData(data) {
  const query = `
    MERGE car_data AS target
    USING (SELECT 
        @carId AS carId,
        @carName AS carName,
        @speed AS speed,
        @fuelLevel AS fuelLevel,
        @headlight AS headlight,
        @engineTemp AS engineTemp,
        @latitude AS latitude,
        @longitude AS longitude
    ) AS source
    ON target.carId = source.carId

    WHEN MATCHED THEN
      UPDATE SET
        carName = source.carName,
        speed = source.speed,
        fuelLevel = source.fuelLevel,
        headlight = source.headlight,
        engineTemp = source.engineTemp,
        latitude = source.latitude,
        longitude = source.longitude

    WHEN NOT MATCHED THEN
      INSERT (carId, carName, speed, fuelLevel, headlight, engineTemp, latitude, longitude)
      VALUES (source.carId, source.carName, source.speed, source.fuelLevel, source.headlight, source.engineTemp, source.latitude, source.longitude);
  `;

  await pool.request()
    .input('carId', sql.VarChar, data.carId)
    .input('carName', sql.VarChar, data.carName)
    .input('speed', sql.Float, data.speed)
    .input('fuelLevel', sql.Int, data.fuelLevel)
    .input('headlight', sql.Bit, data.headlight ? 1 : 0)
    .input('engineTemp', sql.Float, data.engineTemp)
    .input('latitude', sql.Float, data.location?.latitude || null)
    .input('longitude', sql.Float, data.location?.longitude || null)
    .query(query);

  console.log(`🚀 UPSERT done for carId: ${data.carId}`);
}

// Main function
async function run() {
  await initDB();

  await consumer.connect();
  console.log('✅ Kafka connected');

  await consumer.subscribe({
    topic: process.env.KAFKA_TOPIC,
    fromBeginning: false,
  });

  console.log(`📡 Subscribed to topic: ${process.env.KAFKA_TOPIC}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const value = JSON.parse(message.value.toString());
        await upsertCarData(value);
      } catch (err) {
        console.error('❌ Error processing message:', err);
      }
    },
  });
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('🔻 Shutting down...');
  await consumer.disconnect();
  await sql.close();
  process.exit(0);
});

run();