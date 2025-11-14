import 'dotenv/config';
import express from 'express';
import { Pool } from 'pg';

const app = express();
const PORT = process.env.PORT || 8080;

// Simple CORS middleware (configurable via CORS_ORIGIN env var)
// - CORS_ORIGIN can be '*' or a comma-separated list of allowed origins.
const ALLOWED_ORIGIN = process.env.CORS_ORIGIN ?? '*';
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGIN === '*') {
    res.setHeader('Access-Control-Allow-Origin', '*');
  } else if (origin) {
    const allowed = ALLOWED_ORIGIN.split(',').map(s => s.trim());
    if (allowed.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
    }
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// parse JSON bodies for POST /app
app.use(express.json());

// Neon requires SSL. We respect PGSSLMODE=require from .env
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false
});

// === Create ONLY TWO TABLES ===
// 1) input_readings  (your "input schema")
//    coat_type, breed, dog_name, height, weight, sex, temperature_irgun, collar_orientation
// 2) output_metrics  (your "output schema")
//    temperature, stepcount, caloriecount
//    linked to input_readings via collar_id
async function createTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS input_readings (
      id BIGSERIAL PRIMARY KEY,
      collar_id TEXT,
      dog_name TEXT NOT NULL,
      breed TEXT,
      coat_type TEXT,
      height NUMERIC,
      weight NUMERIC,
      sex TEXT,
      temperature_irgun NUMERIC,
      collar_orientation TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS output_metrics (
      id BIGSERIAL PRIMARY KEY,
      collar_id TEXT,
      temperature NUMERIC,
      stepcount BIGINT,
      caloriecount NUMERIC,
      accel_x NUMERIC,
      accel_y NUMERIC,
      accel_z NUMERIC,
      gyro_x NUMERIC,
      gyro_y NUMERIC,
      gyro_z NUMERIC,
      npl_time TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

// helper for numeric coercion
function toNum(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}


/**
 * New POST /app endpoint to accept JSON body for input_readings
 * Expected JSON body shape (example):
 * {
 *   "collar_id":"C123",
 *   "dog_name":"Muffin",
 *   "breed":"Indie",
 *   "coat_type":"single",
 *   "height":45,
 *   "weight":18,
 *   "sex":"male",
 *   "temperature_irgun":38.6,
 *   "collar_orientation":"top"
 * }
 */
app.post('/app', async (req, res) => {
  const {
    collar_id,
    dog_name,
    breed,
    coat_type,
    height,
    weight,
    sex,
    temperature_irgun,
    collar_orientation
  } = req.body || {};

  if (!dog_name) return res.status(400).json({ ok: false, error: 'dog_name is required' });

  const heightN = toNum(height);
  const weightN = toNum(weight);
  const tempIrN = toNum(temperature_irgun);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ins = await client.query(
      `
        INSERT INTO input_readings
          (collar_id, dog_name, breed, coat_type, height, weight, sex, temperature_irgun, collar_orientation)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        RETURNING id, created_at, collar_id;
      `,
      [collar_id ?? null, dog_name, breed ?? null, coat_type ?? null, heightN, weightN, sex ?? null, tempIrN, collar_orientation ?? null]
    );
    await client.query('COMMIT');
    res.json({ ok: true, inserted: ins.rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

app.get('/', (_req, res) => {
  res.json({ ok: true, msg: 'Use POST /app to store input_readings and GET /collar to query or send output metrics' });
});

app.get('/collar', async (req, res) => {
  const {
    collar_id,
    limit = '100',
    offset = '0',
    temperature,
    stepcount,
    caloriecount,
    accel_x,
    accel_y,
    accel_z,
    gyro_x,
    gyro_y,
    gyro_z,
    npl_time
  } = req.query;

  if (!collar_id) return res.status(400).json({ ok: false, error: 'collar_id is required' });

  // If any output metric query param is present, treat this GET as an ingest of output data
  const hasOutputData = (
    temperature !== undefined || stepcount !== undefined || caloriecount !== undefined ||
    accel_x !== undefined || accel_y !== undefined || accel_z !== undefined ||
    gyro_x !== undefined || gyro_y !== undefined || gyro_z !== undefined ||
    npl_time !== undefined
  );

  const lim = Math.max(1, Math.min(1000, parseInt(limit, 10) || 100));
  const off = Math.max(0, parseInt(offset, 10) || 0);

  try {
    if (hasOutputData) {
      // Insert output_metrics directly with collar_id (no need to lookup input_readings)
      const tempN = toNum(temperature);
      const stepN = toNum(stepcount);
      const kcalN = toNum(caloriecount);
      const ax = toNum(accel_x);
      const ay = toNum(accel_y);
      const az = toNum(accel_z);
      const gx = toNum(gyro_x);
      const gy = toNum(gyro_y);
      const gz = toNum(gyro_z);
      const nplT = npl_time ? new Date(npl_time) : null;

      const ins = await pool.query(
        `
          INSERT INTO output_metrics (collar_id, temperature, stepcount, caloriecount, accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z, npl_time)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          RETURNING id, created_at;
        `,
        [collar_id, tempN, stepN, kcalN, ax, ay, az, gx, gy, gz, nplT]
      );

      const outRow = ins.rows[0];
      return res.json({ ok: true, output: { id: outRow.id, created_at: outRow.created_at, temperature: tempN, stepcount: stepN, caloriecount: kcalN, accel_x: ax, accel_y: ay, accel_z: az, gyro_x: gx, gyro_y: gy, gyro_z: gz, npl_time: nplT } });
    }

    // Enforce insert-only behavior: reject read/query requests
    return res.status(405).json({ ok: false, error: 'GET /collar is insert-only; provide output metric query parameters to insert' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Short routes /1 .. /6 that return the latest record for that collar_id
// Example: GET /1  -> returns latest rows for collar_id = '1'
app.get('/:num([1-6])', async (req, res) => {
  const collar_id = req.params.num;
  try {
    const q = await pool.query(
      `
        SELECT
          id AS output_id,
          collar_id,
          temperature,
          stepcount,
          caloriecount,
          accel_x,
          accel_y,
          accel_z,
          gyro_x,
          gyro_y,
          gyro_z,
          npl_time,
          created_at AS output_created_at
        FROM output_metrics
        WHERE collar_id = $1
        ORDER BY COALESCE(npl_time, created_at) DESC
        LIMIT 1
      `,
      [collar_id]
    );

    if (!q.rowCount) return res.status(404).json({ ok: false, error: 'no output data for collar_id' });

    const r = q.rows[0];

    // fetch latest input_readings by collar_id (if any)
    const inQ = await pool.query(
      `SELECT id, collar_id, dog_name, breed, coat_type, height, weight, sex, temperature_irgun, collar_orientation, created_at
       FROM input_readings WHERE collar_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [collar_id]
    );
    const inputRow = inQ.rowCount ? inQ.rows[0] : null;

    const input = inputRow ? {
      id: inputRow.id,
      collar_id: inputRow.collar_id,
      dog_name: inputRow.dog_name,
      breed: inputRow.breed,
      coat_type: inputRow.coat_type,
      height: inputRow.height,
      weight: inputRow.weight,
      sex: inputRow.sex,
      temperature_irgun: inputRow.temperature_irgun,
      collar_orientation: inputRow.collar_orientation,
      created_at: inputRow.created_at
    } : null;

    const output = {
      id: r.output_id,
      collar_id: r.collar_id,
      temperature: r.temperature,
      stepcount: r.stepcount,
      caloriecount: r.caloriecount,
      accel_x: r.accel_x,
      accel_y: r.accel_y,
      accel_z: r.accel_z,
      gyro_x: r.gyro_x,
      gyro_y: r.gyro_y,
      gyro_z: r.gyro_z,
      npl_time: r.npl_time,
      created_at: r.output_created_at
    };

    return res.json({ ok: true, input, output });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});
(async () => {
  try {
    await createTables();
    app.listen(PORT, () => console.log(`Server listening on :${PORT}`));
  } catch (e) {
    console.error('Startup error:', e);
    process.exit(1);
  }
})();
