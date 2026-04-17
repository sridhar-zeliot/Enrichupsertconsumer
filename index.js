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

// ✅ UPSERT Function
async function upsertCarData(data) {
  try {
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
        INSERT (
          carId, carName, speed, fuelLevel, 
          headlight, engineTemp, latitude, longitude
        )
        VALUES (
          source.carId, source.carName, source.speed, source.fuelLevel, 
          source.headlight, source.engineTemp, source.latitude, source.longitude
        );
    `;

    await pool.request()
      .input('carId', sql.Int, data.carId)
      .input('carName', sql.VarChar, data.carName)
      .input('speed', sql.Float, data.speed)
      .input('fuelLevel', sql.Int, data.fuelLevel)
      .input('headlight', sql.Bit, data.headlight ? 1 : 0)
      .input('engineTemp', sql.Float, data.engineTemp)
      .input('latitude', sql.Float, data.location?.latitude || null)
      .input('longitude', sql.Float, data.location?.longitude || null)
      .query(query);

    console.log(`🚀 UPSERT done for carId: ${data.carId}`);

  } catch (err) {
    console.error('❌ UPSERT Error:', err.message);
  }
}

// ✅ Random carId (1–15)
function getRandomCarId() {
  return Math.floor(Math.random() * 15) + 1;
}

// ✅ Main Logic
async function run() {
  await initDB();

  let currentCarId = getRandomCarId();

  setInterval(async () => {
    try {
      // 🔥 50% chance: same ID (update) or new ID (insert)
      const useSameId = Math.random() > 0.5;

      if (!useSameId) {
        currentCarId = getRandomCarId(); // new → INSERT
      }
      // else same → UPDATE

      const data = {
        carId: currentCarId,
        carName: "KA07JB007",
        speed: Math.random() * 100,
        fuelLevel: Math.floor(Math.random() * 100),
        headlight: Math.random() > 0.5,
        engineTemp: 70 + Math.random() * 30,
        location: {
          latitude: 60 + Math.random() * 10,
          longitude: 80 + Math.random() * 10
        }
      };

      console.log(
        useSameId
          ? `♻️ Updating carId: ${currentCarId}`
          : `🆕 Inserting carId: ${currentCarId}`
      );

      await upsertCarData(data);

    } catch (err) {
      console.error('❌ Interval Error:', err.message);
    }
  }, 5000); // ✅ 5 seconds
}

// ✅ Start App
run();

// ✅ Graceful Shutdown
process.on('SIGINT', async () => {
  console.log('🔻 Shutting down...');
  await sql.close();
  process.exit(0);
});