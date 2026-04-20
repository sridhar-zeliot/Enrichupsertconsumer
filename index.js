const sql = require('mssql');
require('dotenv').config();

// ✅ MSSQL Config
const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  port: parseInt(process.env.DB_PORT, 10),
  options: {
    encrypt: true,
    trustServerCertificate: true
  },
};

let pool;

// ✅ Init DB
async function initDB() {
  try {
    pool = await sql.connect(dbConfig);
    console.log('✅ Connected to MSSQL');
  } catch (err) {
    console.error('❌ DB Connection Failed:', err);
    process.exit(1);
  }
}

// ✅ Store carId → carName mapping
const carMap = new Map();

// ✅ Generate Random Car Number
function generateCarNumber() {
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

  const state = 'KA';
  const rto = Math.floor(10 + Math.random() * 90);

  const series =
    letters[Math.floor(Math.random() * 26)] +
    letters[Math.floor(Math.random() * 26)];

  const number = Math.floor(1000 + Math.random() * 9000);

  return `${state}${rto}${series}${number}`;
}

// ✅ UPSERT Function
async function upsertCarData(data) {
  try {
    const query = `
      MERGE dbo.car_data AS target
      USING (SELECT 
        @car_id AS car_id,
        @car_name AS car_name,
        @speed AS speed,
        @fuel_level AS fuel_level,
        @headlight AS headlight,
        @engine_temp AS engine_temp,
        @latitude AS latitude,
        @longitude AS longitude
      ) AS source
      ON target.car_id = source.car_id

      WHEN MATCHED THEN
        UPDATE SET
          car_name = source.car_name,
          speed = source.speed,
          fuel_level = source.fuel_level,
          headlight = source.headlight,
          engine_temp = source.engine_temp,
          latitude = source.latitude,
          longitude = source.longitude

      WHEN NOT MATCHED THEN
        INSERT (
          car_id, car_name, speed, fuel_level,
          headlight, engine_temp, latitude, longitude, created_at
        )
        VALUES (
          source.car_id, source.car_name, source.speed, source.fuel_level,
          source.headlight, source.engine_temp, source.latitude, source.longitude, GETDATE()
        )

      OUTPUT $action AS action, INSERTED.*;
    `;

    const result = await pool.request()
      .input('car_id', sql.Int, Number(data.carId))
      .input('car_name', sql.VarChar(50), data.carName)
      .input('speed', sql.Float, data.speed)
      .input('fuel_level', sql.Int, data.fuelLevel)
      .input('headlight', sql.Bit, data.headlight ? 1 : 0)
      .input('engine_temp', sql.Float, data.engineTemp)
      .input('latitude', sql.Float, data.location?.latitude ?? null)
      .input('longitude', sql.Float, data.location?.longitude ?? null)
      .query(query);

    const row = result.recordset[0];

    const time = new Date().toISOString();
    if (row.action === 'INSERT') {
      console.log('\n================ INSERT OPERATION ================');
      console.log(`Timestamp     : ${time}`);
      console.log(`Operation     : NEW RECORD CREATED`);
    } else {
      console.log('\n================ UPDATE OPERATION ================');
      console.log(`Timestamp     : ${time}`);
      console.log(`Operation     : EXISTING RECORD MODIFIED`);
    }
    console.log(`Car ID        : ${row.car_id}`);
    console.log(`Car Name      : ${row.car_name}`);
    console.log(`Speed         : ${row.speed}`);
    console.log(`Fuel Level    : ${row.fuel_level}`);
    console.log(`Engine Temp   : ${row.engine_temp}`);
    console.log(`Latitude      : ${row.latitude}`);
    console.log(`Longitude     : ${row.longitude}`);
    console.log('=================================================\n');

  } catch (err) {
    console.error('❌ UPSERT Error:', err.message);
  }
}

// ✅ ENV CONFIG
const CAR_ID_MIN = parseInt(process.env.CAR_ID_MIN, 10) || 1;
const CAR_ID_MAX = parseInt(process.env.CAR_ID_MAX, 10) || 15;
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS, 10) || 5000;

// ✅ Validation
if (CAR_ID_MIN > CAR_ID_MAX) {
  throw new Error('CAR_ID_MIN cannot be greater than CAR_ID_MAX');
}

// ✅ Random carId
function getRandomCarId() {
  return Math.floor(Math.random() * (CAR_ID_MAX - CAR_ID_MIN + 1)) + CAR_ID_MIN;
}

// ✅ Main Logic
async function run() {
  await initDB();

  let currentCarId = getRandomCarId();

  while (true) {
    try {
      const useSameId = Math.random() > 0.5;

      if (!useSameId) {
        currentCarId = getRandomCarId();
      }

      // ✅ Maintain mapping (IMPORTANT 🔥)
      if (!carMap.has(currentCarId)) {
        carMap.set(currentCarId, generateCarNumber());
      }

      const carName = carMap.get(currentCarId);

      const data = {
        carId: currentCarId,
        carName: carName, 
        speed: Math.random() * 100,
        fuelLevel: Math.floor(Math.random() * 100),
        headlight: Math.random() > 0.5,
        engineTemp: 70 + Math.random() * 30,
        location: {
          latitude: 60 + Math.random() * 10,
          longitude: 80 + Math.random() * 10
        }
      };

      await upsertCarData(data);

    } catch (err) {
      console.error('❌ Loop Error:', err.message);
    }

    // wait interval
    await new Promise(res => setTimeout(res, INTERVAL_MS));
  }
}

// ✅ Start App
run();

// ✅ Graceful Shutdown
process.on('SIGINT', async () => {
  console.log('🔻 Shutting down...');
  await sql.close();
  process.exit(0);
});