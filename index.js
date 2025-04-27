const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');
const axios = require('axios'); // Import axios for making HTTP requests

const app = express();
const port = process.env.PORT || 3000;

// --- Environment Variables ---
// Ensure these environment variables are set in your Railway project settings
const MYSQL_HOST = process.env.MYSQL_HOST;
const MYSQL_USER = process.env.MYSQL_USER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE;
const MYSQL_PORT = process.env.MYSQL_PORT;
const POWER_AUTOMATE_WEBHOOK_URL = process.env.POWER_AUTOMATE_WEBHOOK_URL; // New environment variable for the webhook URL

// Check if the webhook URL is set
if (!POWER_AUTOMATE_WEBHOOK_URL) {
    console.warn("POWER_AUTOMATE_WEBHOOK_URL environment variable is not set. New record notifications to Power Automate will be disabled.");
}


// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Database connection
const db = mysql.createPool({
 host: MYSQL_HOST,
 user: MYSQL_USER,
 password: MYSQL_PASSWORD,
 database: MYSQL_DATABASE,
 port: MYSQL_PORT,
 connectTimeout: 60000, // Increase timeout to 60 seconds
 acquireTimeout: 60000 // Increase acquire timeout to 60 seconds
});

// Test connection endpoint
app.get('/api/test', (req, res) => {
 db.query('SELECT 1+1 AS result', (err, results) => {
  if (err) {
   return res.status(500).json({ error: err.message });
  }
  res.json({ message: 'Database connection successful', result: results[0] });
 });
});

// Get table data endpoint
app.get('/api/tables/:tableName', (req, res) => {
 const tableName = req.params.tableName;
 
 // Basic validation to prevent SQL injection
 if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
  return res.status(400).json({ error: 'Invalid table name' });
 }
 
 db.query(`SELECT * FROM ${tableName}`, (err, results) => {
  if (err) {
   return res.status(500).json({ error: err.message });
  }
  res.json(results);
 });
});

// Get specific record
app.get('/api/tables/:tableName/:id', (req, res) => {
 const tableName = req.params.tableName;
 const id = req.params.id;
 
 // Basic validation
 if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
  return res.status(400).json({ error: 'Invalid table name' });
 }
 
 db.query(`SELECT * FROM ${tableName} WHERE id = ?`, [id], (err, results) => {
  if (err) {
   return res.status(500).json({ error: err.message });
  }
  res.json(results[0] || {});
 });
});

// Create record (Modified to send webhook notification)
app.post('/api/tables/:tableName', (req, res) => {
 const tableName = req.params.tableName;

  let recordDataString;

  // Safely try to access the nested string data from Power Automate body
  try {
    // Assuming Power Automate nests the actual data under 'body' and '$'
    recordDataString = req.body.body.$; 
    if (typeof recordDataString !== 'string') {
        console.error(`[${tableName}] Expected req.body.body.$ to be a string for CreateRecord, but received:`, typeof recordDataString);
        return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
    }
  } catch (e) {
      console.error(`[${tableName}] Error accessing nested data in request body for CreateRecord:`, e);
      return res.status(400).json({ error: "Invalid request body structure" });
  }

  let data;

  // Try to parse the string as JSON
  try {
    data = JSON.parse(recordDataString);
     if (typeof data !== 'object' || data === null) {
         console.error(`[${tableName}] Parsed data for CreateRecord is not an object:`, data);
         return res.status(400).json({ error: "Invalid data format: Parsed payload is not a JSON object" });
    }
  } catch (e) {
      console.error(`[${tableName}] Error parsing JSON string from req.body.body.$ for CreateRecord:`, e);
      return res.status(400).json({ error: "Invalid data format: Payload under 'body.$' is not valid JSON" });
  }
 
 // Basic validation for table name
 if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
   console.log(`[${tableName}] Invalid table name for CreateRecord:`, tableName);
  return res.status(400).json({ error: 'Invalid table name' });
 }

  // Optional: Remove 'id' if it's mistakenly sent in a creation request body
  if (data.id !== undefined) {
      console.warn(`[${tableName}] ID property found in create data body, removing it:`, data.id);
      delete data.id;
  }

  console.log(`[${tableName}] Data object prepared for insert query:`, data);

  // Check if there's any data left to insert
  if (Object.keys(data).length === 0) {
      console.log(`[${tableName}] No insert data provided after processing.`); 
      return res.status(400).json({ error: 'No insert data provided' });
  }

  console.log(`[${tableName}] Executing INSERT query...`);

 db.query(`INSERT INTO ${tableName} SET ?`, data, (err, result) => {
  if (err) {
      console.error(`[${tableName}] Database error during INSERT:`, err);
   return res.status(500).json({ error: err.message });
  }

    const newRecordId = result.insertId;
    console.log(`[${tableName}] Insert successful, new id: ${newRecordId}`);

    // --- Webhook Notification to Power Automate ---
    if (POWER_AUTOMATE_WEBHOOK_URL) {
        const notificationPayload = {
            tableName: tableName,
            recordDetails: {
                id: newRecordId, // Include the newly generated ID
                ...data // Include the rest of the inserted data
            }
        };

        console.log(`[${tableName}] Sending webhook notification to Power Automate...`);

        axios.post(POWER_AUTOMATE_WEBHOOK_URL, notificationPayload)
            .then(response => {
                console.log(`[${tableName}] Webhook notification sent successfully. Status: ${response.status}`);
            })
            .catch(error => {
                // Log the error but do NOT return an error response to the client
                // The database insert was successful, the webhook is a secondary action.
                console.error(`[${tableName}] Failed to send webhook notification to Power Automate:`, error.message);
                // You might want more detailed error logging here depending on needs
                // console.error(error.response?.data || error.message); 
            });
    } else {
        console.log(`[${tableName}] Webhook URL not set, skipping notification.`);
    }
    // --- End Webhook Notification ---

    // Respond to the client indicating successful creation
  res.json({ id: newRecordId, ...data }); 
 });
});

// Update record (Kept the fix from previous interaction)
app.put('/api/tables/:tableName/:id', (req, res) => {
 const tableName = req.params.tableName;
 const id = req.params.id;
 
 // console.log("--- Update Request Details ---");
 // console.log("Received PUT request body:", req.body); 

  let recordDataString;

  // Safely try to access the nested string data
  try {
    recordDataString = req.body.body.$; // Access the value under 'body' and '$'
    if (typeof recordDataString !== 'string') {
        console.error(`[${tableName}] Expected req.body.body.$ to be a string for UpdateRecord, but received:`, typeof recordDataString);
        return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
    }
  } catch (e) {
      console.error(`[${tableName}] Error accessing nested data in request body for UpdateRecord:`, e);
      return res.status(400).json({ error: "Invalid request body structure" });
  }

  let data;

  // Try to parse the string as JSON
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
  
 // Basic validation for table name
 if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
   console.log(`[${tableName}] Invalid table name for UpdateRecord:`, tableName);
  return res.status(400).json({ error: 'Invalid table name' });
 }

  // It's good practice to remove the 'id' from the data object
  // as you are using it in the WHERE clause and shouldn't update the ID itself.
  if (data.id !== undefined) { 
      console.log(`[${tableName}] Removing id property from data:`, data.id);
      delete data.id;
  }
  
  console.log(`[${tableName}] Data object prepared for update query:`, data);

  // Check if there's any data left to update after removing id
  if (Object.keys(data).length === 0) {
      console.log(`[${tableName}] No update data provided after processing.`); 
      return res.status(400).json({ error: 'No update data provided or data format is incorrect' });
  }

  console.log(`[${tableName}] Executing UPDATE query for id: ${id}...`);

 db.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, id], (err) => {
  if (err) {
      console.error(`[${tableName}] Database error during UPDATE:`, err);
   return res.status(500).json({ error: err.message });
  }
    console.log(`[${tableName}] Update successful for id: ${id}`);

    // Respond with the updated record structure - use the parsed data
  res.json({ id, ...data }); 
 });
});

// Delete record
app.delete('/api/tables/:tableName/:id', (req, res) => {
 const tableName = req.params.tableName;
 const id = req.params.id;
 
 // Basic validation
 if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
  return res.status(400).json({ error: 'Invalid table name' });
 }
 
 db.query(`DELETE FROM ${tableName} WHERE id = ?`, [id], (err) => {
  if (err) {
   return res.status(500).json({ error: err.message });
  }
  res.json({ message: 'Record deleted successfully' });
 });
});

app.listen(port, () => {
 console.log(`Server running on port ${port}`);
});
