import 'dotenv/config';
import express from 'express';
import { Pool } from 'pg';

const app = express();
const PORT = process.env.PORT || 8080;

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
//    linked to input_readings via input_id
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
      input_id BIGINT NOT NULL REFERENCES input_readings(id) ON DELETE CASCADE,
      temperature NUMERIC,
      stepcount BIGINT,
      caloriecount NUMERIC,
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
 * Single GET endpoint to ingest & persist.
 * Call like:
 * /ingest?dog_name=Muffin&breed=Indie&coat_type=short&height=45&weight=18&sex=male
 *        &temperature_irgun=38.6&collar_orientation=top
 *        &temperature=38.2&stepcount=1234&caloriecount=56
 *
 * This will:
 * 1) insert into input_readings
 * 2) insert into output_metrics (linked via input_id) if any output fields present
 */
app.get('/ingest', async (req, res) => {
  const {
    coat_type,
    breed,
    collar_id,
    dog_name,
    height,
    weight,
    sex,
    temperature_irgun,
    collar_orientation,
    temperature,
    stepcount,
    caloriecount
  } = req.query;

  if (!dog_name) {
    return res.status(400).json({ ok: false, error: 'dog_name is required' });
  }

  const heightN = toNum(height);
  const weightN = toNum(weight);
  const tempIrN = toNum(temperature_irgun);
  const tempOutN = toNum(temperature);
  const stepN    = toNum(stepcount);
  const kcalN    = toNum(caloriecount);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // insert input row
    const insInput = await client.query(
      `
        INSERT INTO input_readings
          (collar_id, dog_name, breed, coat_type, height, weight, sex, temperature_irgun, collar_orientation)
        VALUES
          ($1,        $2,      $3,    $4,        $5,     $6,     $7,  $8,               $9)
        RETURNING id, created_at, collar_id;
      `,
      [
        collar_id ?? null,
        dog_name,
        breed ?? null,
        coat_type ?? null,
        heightN,
        weightN,
        sex ?? null,
        tempIrN,
        collar_orientation ?? null
      ]
    );
    const inputId = insInput.rows[0].id;

    // optional output insertion
    let outputRow = null;
    if (tempOutN !== null || stepN !== null || kcalN !== null) {
      const insOutput = await client.query(
        `
          INSERT INTO output_metrics (input_id, temperature, stepcount, caloriecount)
          VALUES ($1, $2, $3, $4)
          RETURNING id, created_at;
        `,
        [inputId, tempOutN, stepN, kcalN]
      );
      outputRow = insOutput.rows[0];
    }

    await client.query('COMMIT');

    res.json({
      ok: true,
      inserted: {
        input_readings: { id: inputId, ...insInput.rows[0] },
        output_metrics: outputRow
      }
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

app.get('/', (_req, res) => {
  res.json({ ok: true, msg: 'Use /ingest?... to store data into Neon Postgres' });
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
