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

// Create record with flexible body handling and foreign key resolution
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
    // Handle foreign key references that might be using GUIDs instead of IDs
    const processedData = await processForeignKeyReferences(tableName, data);
    
    console.log(`[${tableName}] Executing INSERT query with processed data:`, JSON.stringify(processedData));
    const [result] = await pool.query(`INSERT INTO ${tableName} SET ?`, processedData);
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
        await sendWebhookNotification(tableName, { id: newRecordId, ...processedData });
      } catch (error) {
        console.error(`[${tableName}] Failed to send webhook notification, but will continue.`);
      }
    }

    res.json({ id: newRecordId, ...processedData });
  } catch (err) {
    console.error(`[${tableName}] Database error during INSERT:`, err);
    // Provide more specific error message for foreign key constraint violations
    if (err.code === 'ER_NO_REFERENCED_ROW_2') {
      res.status(400).json({ 
        error: 'Foreign key constraint violation. One or more referenced records do not exist.',
        details: err.message
      });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

// Update record with flexible body handling and foreign key resolution
app.put('/api/tables/:tableName/:idOrGuid', async (req, res) => {
  const tableName = req.params.tableName;
  const idOrGuid = req.params.idOrGuid;
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

  // Remove ID if present to avoid overwriting the record ID
  if (data.id !== undefined) {
    console.log(`[${tableName}] Removing id property from data:`, data.id);
    delete data.id;
  }

  // Remove guid if present to avoid overwriting the record guid
  if (data.guid !== undefined) {
    console.log(`[${tableName}] Removing guid property from data:`, data.guid);
    delete data.guid;
  }

  if (Object.keys(data).length === 0) {
    console.log(`[${tableName}] No update data provided after processing.`);
    return res.status(400).json({ error: 'No update data provided or data format is incorrect' });
  }

  try {
    // Process any foreign key references that might be GUIDs
    const processedData = await processForeignKeyReferences(tableName, data);
    
    let queryCondition, queryParams;
    
    // Check if idOrGuid is numeric (likely an ID) or string (likely a GUID)
    if (!isNaN(idOrGuid)) {
      console.log(`[${tableName}] Executing UPDATE query for id: ${idOrGuid} with processed data:`, JSON.stringify(processedData));
      queryCondition = 'id = ?';
      queryParams = [processedData, idOrGuid];
    } else {
      console.log(`[${tableName}] Executing UPDATE query for guid: ${idOrGuid} with processed data:`, JSON.stringify(processedData));
      queryCondition = 'guid = ?';
      queryParams = [processedData, idOrGuid];
    }
    
    const [updateResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE ${queryCondition}`, queryParams);
    
    if (updateResult.affectedRows === 0) {
      // If no rows were updated with the first condition, try the other one
      if (!isNaN(idOrGuid)) {
        // If we tried ID first, now try GUID
        console.log(`[${tableName}] No rows updated by id, trying guid: ${idOrGuid}`);
        const [retryResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE guid = ?`, [processedData, idOrGuid]);
        if (retryResult.affectedRows === 0) {
          return res.status(404).json({ error: 'Record not found' });
        }
      } else {
        // If we tried GUID first, now try ID (if it could potentially be numeric)
        if (idOrGuid.match(/^\d+$/)) {
          console.log(`[${tableName}] No rows updated by guid, trying id: ${idOrGuid}`);
          const [retryResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [processedData, idOrGuid]);
          if (retryResult.affectedRows === 0) {
            return res.status(404).json({ error: 'Record not found' });
          }
        } else {
          return res.status(404).json({ error: 'Record not found' });
        }
      }
    }
    
    console.log(`[${tableName}] Update successful for idOrGuid: ${idOrGuid}`);
    
    // Fetch the updated record to return it
    let [updatedRecord] = await pool.query(
      `SELECT * FROM ${tableName} WHERE id = ? OR guid = ? LIMIT 1`, 
      [idOrGuid, idOrGuid]
    );
    
    res.json(updatedRecord[0] || { message: 'Record updated successfully' });
  } catch (err) {
    console.error(`[${tableName}] Database error during UPDATE:`, err);
    // Provide more specific error message for foreign key constraint violations
    if (err.code === 'ER_NO_REFERENCED_ROW_2') {
      res.status(400).json({ 
        error: 'Foreign key constraint violation. One or more referenced records do not exist.',
        details: err.message
      });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

// Delete record - augmented with foreign key handling
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
    
    // For GUID deletion, we need to find the numeric ID first if the table has foreign key relationships
    // Get the ID first using the GUID, then delete using the ID
    const [rows] = await pool.query(`SELECT id FROM ${tableName} WHERE guid = ?`, [idOrGuid]);
    
    if (rows.length > 0) {
      const numericId = rows[0].id;
      console.log(`[${tableName}] Found record with guid: ${idOrGuid}, deleting with id: ${numericId}`);
      [deleteResult] = await pool.query(`DELETE FROM ${tableName} WHERE id = ?`, [numericId]);
      
      if (deleteResult.affectedRows > 0) {
        return res.json({ 
          message: 'Record deleted successfully', 
          identifier: 'guid', 
          value: idOrGuid,
          id: numericId 
        });
      }
    } else {
      // If we couldn't find by GUID, return 404
      return res.status(404).json({ error: 'Record not found' });
    }
    
    // If still no records affected, return 404
    return res.status(404).json({ error: 'Record not found' });
  } catch (err) {
    console.error(`[${tableName}] Database error during DELETE:`, err);
    res.status(500).json({ error: err.message });
  }
});

// New function to process foreign key references in data objects
async function processForeignKeyReferences(tableName, data) {
  // Clone the data to avoid modifying the original
  const processedData = { ...data };
  
  try {
    // Get the table structure to identify foreign key fields
    const [tableInfo] = await pool.query(`
      SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_NAME = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL
      AND CONSTRAINT_SCHEMA = DATABASE()
    `, [tableName]);
    
    // No foreign keys to process
    if (tableInfo.length === 0) {
      return processedData;
    }
    
    console.log(`[${tableName}] Found ${tableInfo.length} foreign key fields to process`);
    
    // Process each foreign key field
    for (const fkInfo of tableInfo) {
      const fieldName = fkInfo.COLUMN_NAME;
      const referencedTable = fkInfo.REFERENCED_TABLE_NAME;
      const referencedColumn = fkInfo.REFERENCED_COLUMN_NAME;
      
      // If this foreign key field exists in the data and has a value
      if (processedData[fieldName] !== undefined && processedData[fieldName] !== null) {
        const fieldValue = processedData[fieldName];
        
        // If the value is not a number, it might be a GUID - try to resolve it
        if (isNaN(fieldValue) && typeof fieldValue === 'string') {
          console.log(`[${tableName}] Resolving foreign key ${fieldName} with GUID value: ${fieldValue}`);
          
          // Look up the numeric ID from the referenced table using the GUID
          const [refRows] = await pool.query(
            `SELECT ${referencedColumn} FROM ${referencedTable} WHERE guid = ?`, 
            [fieldValue]
          );
          
          if (refRows.length > 0) {
            const numericId = refRows[0][referencedColumn];
            console.log(`[${tableName}] Resolved foreign key ${fieldName} GUID ${fieldValue} to ID ${numericId}`);
            processedData[fieldName] = numericId;
          } else {
            console.error(`[${tableName}] Failed to resolve foreign key ${fieldName} GUID ${fieldValue}`);
            throw new Error(`Referenced record with GUID ${fieldValue} not found in ${referencedTable}`);
          }
        }
      }
    }
  } catch (error) {
    console.error(`[${tableName}] Error processing foreign keys:`, error);
    throw error;
  }
  
  return processedData;
}

// Helper function to get database table schema information
async function getTableSchema(tableName) {
  try {
    const [columns] = await pool.query(`
      SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
        IS_NULLABLE,
        COLUMN_KEY
      FROM 
        INFORMATION_SCHEMA.COLUMNS 
      WHERE 
        TABLE_NAME = ? 
        AND TABLE_SCHEMA = DATABASE()
    `, [tableName]);
    
    const [foreignKeys] = await pool.query(`
      SELECT 
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
      FROM 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE 
        TABLE_NAME = ?
        AND REFERENCED_TABLE_NAME IS NOT NULL
        AND CONSTRAINT_SCHEMA = DATABASE()
    `, [tableName]);
    
    return { columns, foreignKeys };
  } catch (error) {
    console.error(`Error fetching schema for ${tableName}:`, error);
    throw error;
  }
}