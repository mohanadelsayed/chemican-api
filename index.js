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

// Helper function to extract data from request body in different formats
function extractDataFromRequest(req) {
  // If req.body.body.$ exists as a string (test tab format)
  if (req.body?.body?.$) {
    const bodyContent = req.body.body.$;
    if (typeof bodyContent === 'string') {
      try {
        return JSON.parse(bodyContent);
      } catch (e) {
        return bodyContent; // Return as is if not valid JSON
      }
    } else {
      return bodyContent; // Maybe it's already an object
    }
  } 
  // Try req.body.body (Power Automate flow format)
  else if (req.body?.body) {
    const bodyContent = req.body.body;
    if (typeof bodyContent === 'string') {
      try {
        return JSON.parse(bodyContent);
      } catch (e) {
        return bodyContent; // Return as is if not valid JSON
      }
    } else {
      return bodyContent; // Maybe it's already an object
    }
  }
  // Fallback to req.body itself
  else {
    return req.body;
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

// Get specific record by ID or GUID
app.get('/api/tables/:tableName/:idOrGuid', async (req, res) => {
  const tableName = req.params.tableName;
  const idOrGuid = req.params.idOrGuid;
  
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  try {
    // First, try to find by id (assuming id is numeric)
    let results;
    
    if (!isNaN(idOrGuid)) {
      // If the parameter is numeric, try to find by id first
      [results] = await pool.query(`SELECT * FROM ${tableName} WHERE id = ?`, [idOrGuid]);
      
      if (results.length > 0) {
        return res.json(results[0]);
      }
    }
    
    // If not found by id or the parameter is not numeric, try to find by guid
    [results] = await pool.query(`SELECT * FROM ${tableName} WHERE guid = ?`, [idOrGuid]);
    
    res.json(results[0] || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Create record with flexible body handling
app.post('/api/tables/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  let data;
  
  try {
    data = extractDataFromRequest(req);
    
    // Validate we got an object
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Failed to extract a valid JSON object from request" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error processing request body:`, e);
    console.error('Request body structure:', JSON.stringify(req.body, null, 2));
    return res.status(400).json({ error: "Invalid data format: Unable to parse JSON payload" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.log(`[${tableName}] Invalid table name:`, tableName);
    return res.status(400).json({ error: 'Invalid table name' });
  }

  // Remove ID if present to allow auto-increment
  if (data.id !== undefined) {
    console.warn(`[${tableName}] ID property found in create data, removing it:`, data.id);
    delete data.id;
  }

  if (Object.keys(data).length === 0) {
    console.log(`[${tableName}] No insert data provided after processing.`);
    return res.status(400).json({ error: 'No insert data provided' });
  }

  try {
    console.log(`[${tableName}] Executing INSERT query with data:`, JSON.stringify(data));
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

// Update record with flexible body handling
app.put('/api/tables/:tableName/:idOrGuid', async (req, res) => {
  const tableName = req.params.tableName;
  const idOrGuid = req.params.params.idOrGuid; // Corrected access to path parameters
  let data;

  try {
    data = extractDataFromRequest(req);

    // Validate we got an object
    if (typeof data !== 'object' || data === null) {
      console.error(`[${tableName}] Parsed data for UpdateRecord is not an object:`, data);
      return res.status(400).json({ error: "Invalid data format: Failed to extract a valid JSON object from request" });
    }
  } catch (e) {
    console.error(`[${tableName}] Error processing request body for UpdateRecord:`, e);
    console.error('Request body structure:', JSON.stringify(req.body, null, 2));
    return res.status(400).json({ error: "Invalid data format: Unable to parse JSON payload" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.log(`[${tableName}] Invalid table name for UpdateRecord:`, tableName);
    return res.status(400).json({ error: 'Invalid table name' });
  }

  // Create a copy of the data to modify for the SET clause
  const updateData = { ...data };

  // Always remove ID from the update data to prevent primary key modification
  if (updateData.id !== undefined) {
    console.log(`[${tableName}] Removing id property from data:`, updateData.id);
    delete updateData.id;
  }

  // Determine if the record is identified by ID (numeric) or GUID (non-numeric) in the URL
  const isIdentifyingByIdUrl = !isNaN(idOrGuid);

  // Remove guid from the updateData ONLY if the record is being identified by GUID in the URL.
  // This prevents the user from changing the GUID that was used to find the record *via this specific request*.
  // If the user updates by ID, they *can* set or update the GUID via the body.
  if (!isIdentifyingByIdUrl && updateData.guid !== undefined) {
      console.log(`[${tableName}] Removing guid property from data because update is by guid: ${idOrGuid}`, updateData.guid);
      delete updateData.guid;
  }

  // Check if there's any data left to update after removing id and potentially guid
  // Only proceed if there are actual fields to update other than the identifiers
  const finalUpdateKeys = Object.keys(updateData);
  if (finalUpdateKeys.length === 0) {
     // Check if the original data had any fields besides id and the identifier field in the URL
     const originalKeysMinusIdentifier = Object.keys(data).filter(key => {
        if (key === 'id') return false; // Always exclude id
        if (!isIdentifyingByIdUrl && key === 'guid') return false; // Exclude guid if identifying by guid
        return true; // Include other keys that are not the identifier being used
     });

     if (originalKeysMinusIdentifier.length === 0) {
         console.log(`[${tableName}] No update data provided after processing (only contained id or identifier field).`);
         // If the record exists but no updatable data was provided, maybe return success or 400?
         // Returning 400 is clearer that the payload was insufficient for an update.
         return res.status(400).json({ error: 'No valid update data provided' });
     }
      // If we reached here, it means updateData was empty *but* original data had fields
      // that *should* have been included. This indicates an issue with the filtering logic
      // or unexpected data format. Log and return 400.
      console.error(`[${tableName}] Logic error: updateData is empty but original data had updatable fields.`);
      return res.status(500).json({ error: 'Internal processing error with update data' });
  }


  try {
    let queryCondition, queryParams;
    let updateSuccessful = false;

    // Attempt update by ID or GUID based on idOrGuid from the URL
    if (isIdentifyingByIdUrl) {
      console.log(`[${tableName}] Attempting UPDATE by id: ${idOrGuid} with data:`, JSON.stringify(updateData));
      queryCondition = 'id = ?';
      queryParams = [updateData, idOrGuid];
    } else {
      console.log(`[${tableName}] Attempting UPDATE by guid: ${idOrGuid} with data:`, JSON.stringify(updateData));
      queryCondition = 'guid = ?';
      queryParams = [updateData, idOrGuid];
    }

    const [updateResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE ${queryCondition}`, queryParams);

    if (updateResult.affectedRows > 0) {
        updateSuccessful = true;
        console.log(`[${tableName}] Update successful using ${isIdentifyingByIdUrl ? 'id' : 'guid'}: ${idOrGuid}`);
    } else {
        // Implement the retry logic from the original code if the initial attempt failed
        let retryAttempted = false;
        if (isIdentifyingByIdUrl && idOrGuid.match(/^[0-9a-fA-F-]+$/)) { // Tried ID, but idOrGuid looks like a GUID?
             console.log(`[${tableName}] No rows updated by id ${idOrGuid}, trying guid...`);
             queryCondition = 'guid = ?';
             queryParams = [updateData, idOrGuid]; // Use original idOrGuid for the retry condition
             [updateResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE ${queryCondition}`, queryParams);
             retryAttempted = true;
             if (updateResult.affectedRows > 0) {
                 updateSuccessful = true;
                 console.log(`[${tableName}] Update successful via guid after id failed for ${idOrGuid}.`);
             }
        } else if (!isIdentifyingByIdUrl && idOrGuid.match(/^\d+$/)) { // Tried GUID, but idOrGuid looks like an ID?
             console.log(`[${tableName}] No rows updated by guid ${idOrGuid}, trying id...`);
             queryCondition = 'id = ?';
             queryParams = [updateData, idOrGuid]; // Use original idOrGuid for the retry condition
             [updateResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE ${queryCondition}`, queryParams);
             retryAttempted = true;
             if (updateResult.affectedRows > 0) {
                updateSuccessful = true;
                console.log(`[${tableName}] Update successful via id after guid failed for ${idOrGuid}.`);
             }
        }

        if (!updateSuccessful) {
           console.log(`[${tableName}] Record not found for update with identifier: ${idOrGuid}`);
           return res.status(404).json({ error: 'Record not found' });
        }
    }

    // If update was successful, fetch the updated record using the original identifier logic
    console.log(`[${tableName}] Fetching updated record with identifier: ${idOrGuid}`);
    const [fetchedRecords] = await pool.query(`
      SELECT * FROM ${tableName} WHERE ${!isNaN(idOrGuid) ? 'id' : 'guid'} = ?
      ${!isNaN(idOrGuid) ? 'OR guid = ?' : ''} LIMIT 1
    `, !isNaN(idOrGuid) ? [idOrGuid, idOrGuid] : [idOrGuid]);


    res.json(fetchedRecords[0] || { message: 'Record updated successfully (could not refetch)' });

  } catch (err) {
    console.error(`[${tableName}] Database error during UPDATE:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Delete record
app.delete('/api/tables/:tableName/:idOrGuid', async (req, res) => {
  const tableName = req.params.tableName;
  const idOrGuid = req.params.idOrGuid;
  
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  try {
    // Try to delete by ID first if the parameter looks like a number
    let deleteResult;
    
    if (!isNaN(idOrGuid)) {
      console.log(`[${tableName}] Executing DELETE for id: ${idOrGuid}`);
      [deleteResult] = await pool.query(`DELETE FROM ${tableName} WHERE id = ?`, [idOrGuid]);
      
      if (deleteResult.affectedRows > 0) {
        return res.json({ message: 'Record deleted successfully', identifier: 'id', value: idOrGuid });
      }
    }
    
    // If no rows affected or not a number, try by GUID
    console.log(`[${tableName}] Executing DELETE for guid: ${idOrGuid}`);
    [deleteResult] = await pool.query(`DELETE FROM ${tableName} WHERE guid = ?`, [idOrGuid]);
    
    if (deleteResult.affectedRows > 0) {
      return res.json({ message: 'Record deleted successfully', identifier: 'guid', value: idOrGuid });
    }
    
    // If still no records affected, return 404
    return res.status(404).json({ error: 'Record not found' });
  } catch (err) {
    console.error(`[${tableName}] Database error during DELETE:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Get current tracking state
app.get('/api/tracking-status', async (req, res) => {
  try {
    const [trackingInfo] = await pool.query(`
      SELECT * FROM webhook_processed_records
    `);
    
    res.json({
      trackingRecords: trackingInfo,
      inMemoryLastId: lastKnownFormSubmitId,
      isInitialCheckComplete
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Reset tracking for a table
app.post('/api/reset-tracking/:tableName', async (req, res) => {
  try {
    const tableName = req.params.tableName;
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      return res.status(400).json({ error: 'Invalid table name' });
    }
    
    // Reset to specified ID or find current max
    let resetToId = req.body.resetToId;
    
    if (resetToId === undefined) {
      // Reset to current max ID
      const [maxRows] = await pool.query(`
        SELECT COALESCE(MAX(id), 0) AS max_id FROM ${tableName}
      `);
      resetToId = maxRows[0].max_id || 0;
    }
    
    // Update the tracking table
    await pool.query(`
      UPDATE webhook_processed_records
      SET last_processed_id = ?, last_check_time = CURRENT_TIMESTAMP
      WHERE table_name = ?
    `, [resetToId, tableName]);
    
    // Update in-memory variable if it's form_submits
    if (tableName === 'form_submits') {
      lastKnownFormSubmitId = resetToId;
    }
    
    res.json({ 
      message: `Tracking for ${tableName} reset to ID ${resetToId}`,
      resetToId
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Health check endpoint for monitoring
app.get('/health', async (req, res) => {
  try {
    // Check database connection
    await pool.query('SELECT 1');
    
    // Return current tracking state
    const [trackingInfo] = await pool.query(`
      SELECT * FROM webhook_processed_records
      WHERE table_name = 'form_submits'
    `);
    
    res.json({
      status: 'healthy',
      lastProcessedId: lastKnownFormSubmitId,
      isInitialCheckComplete,
      trackingInfo: trackingInfo[0] || null,
      webhookUrl: POWER_AUTOMATE_WEBHOOK_URL ? '(configured)' : '(not configured)'
    });
  } catch (err) {
    res.status(500).json({
      status: 'unhealthy',
      error: err.message
    });
  }
});

// Force check for new submissions (for testing)
app.post('/api/force-check', async (req, res) => {
  try {
    await checkForNewFormSubmissions();
    res.json({
      status: 'check initiated',
      lastProcessedId: lastKnownFormSubmitId
    });
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