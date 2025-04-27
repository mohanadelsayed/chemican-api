const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Database connection
const db = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT,
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
  const data = req.body;

  console.log("Update request body:", data); // For debugging
  
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

// Create record
app.post('/api/tables/:tableName', (req, res) => {
   const tableName = req.params.tableName;
  
    // Log the full received body for debugging if needed later
    // console.log("--- Create Request Details ---");
   // console.log("Received POST request body:", req.body); 
  
    let recordDataString;
  
    // Safely try to access the nested string data
    try {
      // Access the value under 'body' and '$' - adjust if Power Automate uses a different key like 'record'
      recordDataString = req.body.body.$; 
      if (typeof recordDataString !== 'string') {
          console.error("Expected req.body.body.$ to be a string for CreateRecord, but received:", typeof recordDataString);
          return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
      }
    } catch (e) {
        // Catch errors if req.body, req.body.body, or req.body.body.$ doesn't exist
        console.error("Error accessing nested data in request body for CreateRecord:", e);
        return res.status(400).json({ error: "Invalid request body structure" });
    }
  
    let data;
  
    // Try to parse the string as JSON
    try {
      data = JSON.parse(recordDataString);
       if (typeof data !== 'object' || data === null) {
           console.error("Parsed data for CreateRecord is not an object:", data);
           return res.status(400).json({ error: "Invalid data format: Parsed payload is not a JSON object" });
      }
    } catch (e) {
        // Catch errors if the string is not valid JSON
        console.error("Error parsing JSON string from req.body.body.$ for CreateRecord:", e);
        return res.status(400).json({ error: "Invalid data format: Payload under 'body.$' is not valid JSON" });
    }
   
   // Basic validation for table name
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
     console.log("Invalid table name for CreateRecord:", tableName);
   return res.status(400).json({ error: 'Invalid table name' });
   }
  
    // Optional: Remove 'id' if it's mistakenly sent in a creation request body
    // The database should assign the ID automatically.
    if (data.id !== undefined) {
        console.warn("ID property found in create data body, removing it:", data.id);
        delete data.id;
    }
  
    console.log("Data object prepared for create query:", data); // Log the final data object
  
    // Check if there's any data left to insert
    if (Object.keys(data).length === 0) {
        console.log("No insert data provided after processing."); 
        // Depending on your schema, an empty insert might be valid or not.
        // Returning 400 here assumes you always need at least one column to insert.
        return res.status(400).json({ error: 'No insert data provided' });
    }
  
    console.log("Executing INSERT query for table:", tableName, "with data:", data); // Log just before query
  
   db.query(`INSERT INTO ${tableName} SET ?`, data, (err, result) => {
   if (err) {
        console.error("Database error during INSERT:", err); // Log the actual error on the server
  return res.status(500).json({ error: err.message });
 }
      console.log("Insert successful, new id:", result.insertId); // Log success
  
      // Respond with the new id and the data that was inserted
  res.json({ id: result.insertId, ...data }); 
 });
  });;

// Update record
app.put('/api/tables/:tableName/:id', (req, res) => {
   const tableName = req.params.tableName;
   const id = req.params.id;
   
    // Log the full received body for debugging if needed later
    // console.log("--- Update Request Details ---");
  // console.log("Received PUT request body:", req.body); 
  
    let recordDataString;
  
    // Safely try to access the nested string data
    try {
      recordDataString = req.body.body.$; // Access the value under 'body' and '$'
      if (typeof recordDataString !== 'string') {
          // If it's not a string, the structure is not what we expected
          console.error("Expected req.body.body.$ to be a string, but received:", typeof recordDataString);
          return res.status(400).json({ error: "Invalid data format: Expected a string payload under 'body.$'" });
      }
    } catch (e) {
        // Catch errors if req.body, req.body.body, or req.body.body.$ doesn't exist
        console.error("Error accessing nested data in request body:", e);
        return res.status(400).json({ error: "Invalid request body structure" });
    }
  
    let data;
  
    // Try to parse the string as JSON
    try {
      data = JSON.parse(recordDataString);
      if (typeof data !== 'object' || data === null) {
           console.error("Parsed data is not an object:", data);
           return res.status(400).json({ error: "Invalid data format: Parsed payload is not a JSON object" });
      }
    } catch (e) {
        // Catch errors if the string is not valid JSON
        console.error("Error parsing JSON string from req.body.body.$:", e);
        return res.status(400).json({ error: "Invalid data format: Payload under 'body.$' is not valid JSON" });
    }
    
   // Basic validation for table name
   if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
     console.log("Invalid table name:", tableName);
   return res.status(400).json({ error: 'Invalid table name' });
   }
  
    // It's good practice to remove the 'id' from the data object
    // as you are using it in the WHERE clause and shouldn't update the ID itself.
    if (data.id !== undefined) { // Check if id exists in the parsed object
        console.log("Removing id property from data:", data.id);
        delete data.id;
    }
    
    console.log("Data object prepared for update query:", data); // Log the final data object
  
    // Check if there's any data left to update after removing id
    if (Object.keys(data).length === 0) {
        console.log("No update data provided after processing."); 
        return res.status(400).json({ error: 'No update data provided' });
    }
  
    console.log("Executing UPDATE query for table:", tableName, "id:", id); // Log just before query
  
  db.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, id], (err) => {
   if (err) {
        console.error("Database error during UPDATE:", err); // Log the actual error on the server
   return res.status(500).json({ error: err.message });
  }
      console.log("Update successful for id:", id); // Log success
  
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