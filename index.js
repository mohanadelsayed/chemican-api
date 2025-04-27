const express = require('express');
const mysql = require('mysql');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

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
  const data = req.body;
  
  // Basic validation
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  db.query(`INSERT INTO ${tableName} SET ?`, data, (err, result) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json({ id: result.insertId, ...data });
  });
});

// Update record
app.put('/api/tables/:tableName/:id', (req, res) => {
  const tableName = req.params.tableName;
  const id = req.params.id;
  const data = req.body;
  
  // Basic validation
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  db.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, id], (err) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
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