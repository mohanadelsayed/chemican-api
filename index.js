const express = require('express');
const mysql = require('mysql2/promise'); // Using promise-based MySQL for async/await
const cors = require('cors');
const bodyParser = require('body-parser');
const axios = require('axios');

const app = express();
const port = process.env.PORT || 3000;

// --- Environment Variables ---
// Ensure these environment variables are set in your Railway project settings
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
    
    // Create a database trigger to monitor the form_submits table
    if (POWER_AUTOMATE_WEBHOOK_URL) {
      await setupFormSubmitsTrigger();
    }
    
  } catch (err) {
    console.error('Failed to create database pool:', err);
    process.exit(1);
  }
}

// Function to set up the database trigger and event handling
async function setupFormSubmitsTrigger() {
  try {
    const connection = await pool.getConnection();
    
    // First, check if the trigger already exists and drop it if it does
    const [triggerCheck] = await connection.query(`
      SELECT TRIGGER_NAME 
      FROM information_schema.TRIGGERS 
      WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = 'after_form_submit_insert'
    `, [MYSQL_DATABASE]);
    
    if (triggerCheck.length > 0) {
      console.log('Dropping existing form_submits trigger...');
      await connection.query('DROP TRIGGER IF EXISTS after_form_submit_insert');
    }
    
    // Create a new table to store notifications that need to be sent
    console.log('Creating webhook_notifications table if not exists...');
    await connection.query(`
      CREATE TABLE IF NOT EXISTS webhook_notifications (
        id INT AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL,
        record_id INT NOT NULL,
        record_data JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed BOOLEAN DEFAULT FALSE,
        process_attempts INT DEFAULT 0
      )
    `);
    
    // Create trigger on form_submits table
    console.log('Creating trigger for form_submits table...');
    await connection.query(`
      CREATE TRIGGER after_form_submit_insert
      AFTER INSERT ON form_submits
      FOR EACH ROW
      BEGIN
        INSERT INTO webhook_notifications (table_name, record_id, record_data)
        VALUES ('form_submits', NEW.id, JSON_OBJECT(
          'id', NEW.id
          ${await getFormSubmitsColumns()}
        ));
      END
    `);
    
    connection.release();
    console.log('Form submissions trigger setup successfully');
    
    // Start the webhook notification processor
    startWebhookProcessor();
    
  } catch (err) {
    console.error('Error setting up form_submits trigger:', err);
  }
}

// Helper function to get all columns from form_submits to include in the trigger
async function getFormSubmitsColumns() {
  try {
    const [columns] = await pool.query(`
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'form_submits' AND COLUMN_NAME != 'id'
    `, [MYSQL_DATABASE]);
    
    return columns.map(col => `, '${col.COLUMN_NAME}', NEW.${col.COLUMN_NAME}`).join('');
  } catch (err) {
    console.error('Error getting form_submits columns:', err);
    return '';
  }
}

// Function to process webhook notifications periodically
function startWebhookProcessor() {
  console.log('Starting webhook notification processor...');
  
  // Process immediately when starting up
  processWebhookNotifications();
  
  // Then process every 30 seconds
  setInterval(processWebhookNotifications, 30000);
}

// Function to process unprocessed webhook notifications
async function processWebhookNotifications() {
  if (!POWER_AUTOMATE_WEBHOOK_URL) {
    return;
  }
  
  let connection;
  try {
    connection = await pool.getConnection();
    
    // Get unprocessed notifications, limit to 10 at a time to avoid overwhelming
    const [notifications] = await connection.query(`
      SELECT * FROM webhook_notifications 
      WHERE processed = FALSE AND process_attempts < 3
      ORDER BY created_at ASC
      LIMIT 10
    `);
    
    if (notifications.length === 0) {
      return;
    }
    
    console.log(`Processing ${notifications.length} webhook notifications...`);
    
    for (const notification of notifications) {
      try {
        // Send the notification to Power Automate
        const response = await axios.post(POWER_AUTOMATE_WEBHOOK_URL, {
          tableName: notification.table_name,
          recordDetails: JSON.parse(notification.record_data)
        }, {
          headers: { 'Content-Type': 'application/json' }
        });
        
        if (response.status >= 200 && response.status < 300) {
          // Update the record to mark as processed
          await connection.query(`
            UPDATE webhook_notifications 
            SET processed = TRUE 
            WHERE id = ?
          `, [notification.id]);
          
          console.log(`Successfully sent webhook notification for ${notification.table_name} record ${notification.record_id}`);
        } else {
          throw new Error(`Received status code ${response.status}`);
        }
      } catch (err) {
        // Increment the attempts counter
        await connection.query(`
          UPDATE webhook_notifications 
          SET process_attempts = process_attempts + 1 
          WHERE id = ?
        `, [notification.id]);
        
        console.error(`Failed to send webhook notification for ${notification.table_name} record ${notification.record_id}:`, err.message);
      }
    }
  } catch (err) {
    console.error('Error processing webhook notifications:', err);
  } finally {
    if (connection) {
      connection.release();
    }
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

// Create record (already sends webhook notification)
app.post('/api/tables/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  let recordDataString;
  
  try {
    recordDataString = req.body.body.$;
    if (typeof recordDataString !== 'string') {
      console.error(`[${tableName}] Expected req.body.body.$ to be a string, but received:`, typeof recordDataString);
      return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error accessing nested data in request body:`, e);
    return res.status(400).json({ error: "Invalid request body structure" });
  }

  let data;
  try {
    data = JSON.parse(recordDataString);
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Parsed payload is not a JSON object" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error parsing JSON string:`, e);
    return res.status(400).json({ error: "Invalid data format: Payload under 'body.$' is not valid JSON" });
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
    console.log(`[${tableName}] Executing INSERT query...`);
    const [result] = await pool.query(`INSERT INTO ${tableName} SET ?`, data);
    const newRecordId = result.insertId;
    console.log(`[${tableName}] Insert successful, new id: ${newRecordId}`);

    // --- Webhook Notification to Power Automate ---
    if (POWER_AUTOMATE_WEBHOOK_URL) {
      const notificationPayload = {
        tableName,
        recordDetails: { id: newRecordId, ...data }
      };

      console.log(`[${tableName}] Sending webhook notification to Power Automate...`);
      try {
        const response = await axios.post(POWER_AUTOMATE_WEBHOOK_URL, notificationPayload, {
          headers: { 'Content-Type': 'application/json' }
        });
        console.log(`[${tableName}] Webhook notification sent. Status: ${response.status}`);
      } catch (error) {
        console.error(
          `[${tableName}] Failed to send webhook:`,
          error.response?.status,
          error.response?.data || error.message
        );
      }
    }

    res.json({ id: newRecordId, ...data });
  } catch (err) {
    console.error(`[${tableName}] Database error during INSERT:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Update record
app.put('/api/tables/:tableName/:id', async (req, res) => {
  const tableName = req.params.tableName;
  const id = req.params.id;
  let recordDataString;
  
  try {
    recordDataString = req.body.body.$;
    if (typeof recordDataString !== 'string') {
      console.error(`[${tableName}] Expected req.body.body.$ to be a string for UpdateRecord, but received:`, typeof recordDataString);
      return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error accessing nested data in request body for UpdateRecord:`, e);
    return res.status(400).json({ error: "Invalid request body structure" });
  }

  let data;
  try {
    data = JSON.parse(recordDataString);
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data for UpdateRecord is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Parsed payload is not a JSON object" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error parsing JSON string from req.body.body.$ for UpdateRecord:`, e);
    return res.status(400).json({ error: "Invalid data format: Payload under 'body.$' is not valid JSON" });
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
    console.log(`[${tableName}] Executing UPDATE query for id: ${id}...`);
    await pool.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, id]);
    console.log(`[${tableName}] Update successful for id: ${id}`);
    res.json({ id, ...data });
  } catch (err) {
    console.error(`[${tableName}] Database error during UPDATE:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Delete record
app.delete('/api/tables/:tableName/:id', async (req, res) => {
  const tableName = req.params.tableName;
  const id = req.params.id;
  
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  try {
    await pool.query(`DELETE FROM ${tableName} WHERE id = ?`, [id]);
    res.json({ message: 'Record deleted successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Initialize the database and start the server
initializeDatabase().then(() => {
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
}).catch(err => {
  console.error('Failed to initialize application:', err);
  process.exit(1);
});