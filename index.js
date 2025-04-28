const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const bodyParser = require('body-parser');
const axios = require('axios');

const app = express();
const port = process.env.PORT || 3000;

// --- Environment Variables ---
const MYSQL_HOST = process.env.MYSQL_HOST;
const MYSQL_USER = process.env.MYSQL_USER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE;
const MYSQL_PORT = process.env.MYSQL_PORT;
const POWER_AUTOMATE_WEBHOOK_URL = process.env.POWER_AUTOMATE_WEBHOOK_URL;

// Check if the webhook URL is set
if (!POWER_AUTOMATE_WEBHOOK_URL) {
    console.warn("POWER_AUTOMATE_WEBHOOK_URL environment variable is not set. New record notifications to Power Automate will be disabled.");
}

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Create database connection pool
let pool;
let lastKnownFormSubmitId = 0;
let isInitialCheckComplete = false;

async function initializeDatabase() {
  try {
    pool = mysql.createPool({
      host: MYSQL_HOST,
      user: MYSQL_USER,
      password: MYSQL_PASSWORD,
      database: MYSQL_DATABASE,
      port: MYSQL_PORT,
      connectTimeout: 60000,
      acquireTimeout: 60000,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });
    
    console.log('Database pool created successfully');
    
    // Create tracking table for external submissions if needed
    await createTrackingTable();
    
    // Initialize the lastKnownFormSubmitId
    await initializeLastKnownId();
    
    // Start polling for new external form submissions
    if (POWER_AUTOMATE_WEBHOOK_URL) {
      startFormSubmissionPolling();
    }
    
  } catch (err) {
    console.error('Failed to create database pool:', err);
    process.exit(1);
  }
}

async function createTrackingTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS webhook_processed_records (
        table_name VARCHAR(100) NOT NULL,
        last_processed_id INT NOT NULL DEFAULT 0,
        last_check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (table_name)
      )
    `);
    
    // Insert a row for form_submits if it doesn't exist
    await pool.query(`
      INSERT IGNORE INTO webhook_processed_records (table_name, last_processed_id)
      VALUES ('form_submits', 0)
    `);
    
    console.log('Tracking table created successfully');
  } catch (err) {
    console.error('Error creating tracking table:', err);
  }
}

async function initializeLastKnownId() {
  try {
    // Get the last processed ID from the tracking table
    const [rows] = await pool.query(`
      SELECT last_processed_id FROM webhook_processed_records
      WHERE table_name = 'form_submits'
    `);
    
    if (rows.length > 0) {
      lastKnownFormSubmitId = rows[0].last_processed_id;
      console.log(`Initialized lastKnownFormSubmitId to ${lastKnownFormSubmitId}`);
    } else {
      // If no record exists, find the highest existing ID
      const [maxRows] = await pool.query(`
        SELECT COALESCE(MAX(id), 0) AS max_id FROM form_submits
      `);
      
      lastKnownFormSubmitId = maxRows[0].max_id || 0;
      console.log(`No tracking record found. Set lastKnownFormSubmitId to current max: ${lastKnownFormSubmitId}`);
      
      // Update the tracking table
      await pool.query(`
        INSERT INTO webhook_processed_records (table_name, last_processed_id)
        VALUES ('form_submits', ?)
        ON DUPLICATE KEY UPDATE last_processed_id = ?
      `, [lastKnownFormSubmitId, lastKnownFormSubmitId]);
    }
    
    // Mark that we've completed the initial check and setup
    isInitialCheckComplete = true;
  } catch (err) {
    console.error('Error initializing lastKnownFormSubmitId:', err);
  }
}

function startFormSubmissionPolling() {
  // First immediate check
  checkForNewFormSubmissions();
  
  // Then check every 10 seconds
  setInterval(checkForNewFormSubmissions, 10000);
}

async function checkForNewFormSubmissions() {
  if (!POWER_AUTOMATE_WEBHOOK_URL || !isInitialCheckComplete) {
    return;
  }
  
  try {
    // Get current tracked ID from database to ensure we're using the latest value
    const [trackingRow] = await pool.query(`
      SELECT last_processed_id FROM webhook_processed_records
      WHERE table_name = 'form_submits'
    `);
    
    if (trackingRow.length === 0) {
      console.error('No tracking record found for form_submits');
      return;
    }
    
    const currentTrackedId = trackingRow[0].last_processed_id;
    
    // Check for new form submissions with ID greater than the tracked ID
    const [newSubmissions] = await pool.query(`
      SELECT * FROM form_submits
      WHERE id > ?
      ORDER BY id ASC
      LIMIT 50
    `, [currentTrackedId]);
    
    if (newSubmissions.length === 0) {
      return;
    }
    
    console.log(`Found ${newSubmissions.length} new form submissions to process (IDs > ${currentTrackedId})`);
    
    let highestProcessedId = currentTrackedId;
    
    for (const submission of newSubmissions) {
      try {
        if (submission.id > highestProcessedId) {
          // Send the webhook notification
          await sendWebhookNotification('form_submits', submission);
          
          // Update our tracking variable
          highestProcessedId = submission.id;
          
          console.log(`Successfully processed form submission ID: ${submission.id}`);
        } else {
          console.log(`Skipping already processed form submission ID: ${submission.id}`);
        }
      } catch (err) {
        console.error(`Failed to process form submission ID ${submission.id}:`, err);
        // Continue with next submission even if this one fails
      }
    }
    
    // Only update the database if we actually processed new records
    if (highestProcessedId > currentTrackedId) {
      // Update our in-memory tracking
      lastKnownFormSubmitId = highestProcessedId;
      
      // Update the tracking table after processing all submissions
      await pool.query(`
        UPDATE webhook_processed_records
        SET last_processed_id = ?, last_check_time = CURRENT_TIMESTAMP
        WHERE table_name = 'form_submits'
      `, [highestProcessedId]);
      
      console.log(`Updated last processed ID to ${highestProcessedId}`);
    }
  } catch (err) {
    console.error('Error checking for new form submissions:', err);
  }
}

async function sendWebhookNotification(tableName, recordData) {
  if (!POWER_AUTOMATE_WEBHOOK_URL) {
    return;
  }
  
  console.log(`Sending webhook notification for ${tableName} record ID: ${recordData.id}`);
  
  try {
    const notificationPayload = {
      tableName,
      recordDetails: recordData
    };
    
    const response = await axios.post(POWER_AUTOMATE_WEBHOOK_URL, notificationPayload, {
      headers: { 'Content-Type': 'application/json' }
    });
    
    console.log(`Webhook notification sent. Status: ${response.status}`);
    return true;
  } catch (error) {
    console.error(
      `Failed to send webhook:`,
      error.response?.status,
      error.response?.data || error.message
    );
    throw error;
  }
}

// Test connection endpoint
app.get('/api/test', async (req, res) => {
  try {
    const [results] = await pool.query('SELECT 1+1 AS result');
    res.json({ message: 'Database connection successful', result: results[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get table data endpoint
app.get('/api/tables/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  try {
    const [results] = await pool.query(`SELECT * FROM ${tableName}`);
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get specific record
app.get('/api/tables/:tableName/:id', async (req, res) => {
  const tableName = req.params.tableName;
  const id = req.params.id;
  
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  try {
    const [results] = await pool.query(`SELECT * FROM ${tableName} WHERE id = ?`, [id]);
    res.json(results[0] || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Create record with more flexible body handling
app.post('/api/tables/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  let data;
  
  try {
    // More flexible handling of the input
    if (req.body.body && req.body.body.$) {
      // Original format from test tab: body.$ as string
      const recordDataString = req.body.body.$;
      
      if (typeof recordDataString === 'string') {
        data = JSON.parse(recordDataString);
      } else {
        // If it's already an object
        data = recordDataString;
      }
    } else if (req.body.body) {
      // Flow might send body as the direct container
      if (typeof req.body.body === 'string') {
        data = JSON.parse(req.body.body);
      } else {
        data = req.body.body;
      }
    } else {
      // Direct body content as fallback
      data = req.body;
    }
    
    // Validate we got an object
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Failed to extract a valid JSON object from request" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error processing request body:`, e, req.body);
    return res.status(400).json({ error: "Invalid data format: Unable to parse JSON payload" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.log(`[${tableName}] Invalid table name:`, tableName);
    return res.status(400).json({ error: 'Invalid table name' });
  }

  if (data.id !== undefined) {
    console.warn(`[${tableName}] ID property found in create data, removing it:`, data.id);
    delete data.id;
  }

  if (Object.keys(data).length === 0) {
    console.log(`[${tableName}] No insert data provided after processing.`);
    return res.status(400).json({ error: 'No insert data provided' });
  }

  try {
    console.log(`[${tableName}] Executing INSERT query with data:`, data);
    const [result] = await pool.query(`INSERT INTO ${tableName} SET ?`, data);
    const newRecordId = result.insertId;
    console.log(`[${tableName}] Insert successful, new id: ${newRecordId}`);

    // For form_submits table, update the lastKnownFormSubmitId to avoid duplicate webhook sending
    if (tableName === 'form_submits') {
      // Update the tracking table immediately to prevent duplicate processing
      lastKnownFormSubmitId = Math.max(lastKnownFormSubmitId, newRecordId);
      await pool.query(`
        UPDATE webhook_processed_records
        SET last_processed_id = ?, last_check_time = CURRENT_TIMESTAMP
        WHERE table_name = 'form_submits'
      `, [lastKnownFormSubmitId]);
      console.log(`Updated form_submits tracking to ID: ${lastKnownFormSubmitId}`);
    }

    // Send webhook notification for all tables
    if (POWER_AUTOMATE_WEBHOOK_URL) {
      try {
        await sendWebhookNotification(tableName, { id: newRecordId, ...data });
      } catch (error) {
        console.error(`[${tableName}] Failed to send webhook notification, but will continue.`);
      }
    }

    res.json({ id: newRecordId, ...data });
  } catch (err) {
    console.error(`[${tableName}] Database error during INSERT:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Update record with similar flexible body handling
app.put('/api/tables/:tableName/:id', async (req, res) => {
  const tableName = req.params.tableName;
  const id = req.params.id;
  let data;
  
  try {
    // More flexible handling of the input
    if (req.body.body && req.body.body.$) {
      // Original format from test tab: body.$ as string
      const recordDataString = req.body.body.$;
      
      if (typeof recordDataString === 'string') {
        data = JSON.parse(recordDataString);
      } else {
        // If it's already an object
        data = recordDataString;
      }
    } else if (req.body.body) {
      // Flow might send body as the direct container
      if (typeof req.body.body === 'string') {
        data = JSON.parse(req.body.body);
      } else {
        data = req.body.body;
      }
    } else {
      // Direct body content as fallback
      data = req.body;
    }
    
    // Validate we got an object
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data for UpdateRecord is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Failed to extract a valid JSON object from request" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error processing request body for UpdateRecord:`, e, req.body);
    return res.status(400).json({ error: "Invalid data format: Unable to parse JSON payload" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.log(`[${tableName}] Invalid table name for UpdateRecord:`, tableName);
    return res.status(400).json({ error: 'Invalid table name' });
  }

  if (data.id !== undefined) {
    console.log(`[${tableName}] Removing id property from data:`, data.id);
    delete data.id;
  }

  if (Object.keys(data).length === 0) {
    console.log(`[${tableName}] No update data provided after processing.`);
    return res.status(400).json({ error: 'No update data provided or data format is incorrect' });
  }

  try {
    console.log(`[${tableName}] Executing UPDATE query for id: ${id} with data:`, data);
    await pool.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, id]);
    console.log(`[${tableName}] Update successful for id: ${id}`);
    res.json({ id, ...data });
  } catch (err) {
    console.error(`[${tableName}] Database error during UPDATE:`, err);
    res.status(500).json({ error: err.message });
  }
});