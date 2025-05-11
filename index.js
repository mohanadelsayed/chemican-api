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
let lastKnownSubscriberId = 0;
let lastKnownBlogCommentId = 0;
let lastKnownBlogPostsState = {}; // To track view_count changes
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
    
    // Initialize the lastKnownIds for various tables
    await initializeLastKnownIds();
    
    // Start polling for new records in monitored tables
    if (POWER_AUTOMATE_WEBHOOK_URL) {
      startRecordPolling();
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
    
    // Insert rows for monitored tables if they don't exist
    await pool.query(`
      INSERT IGNORE INTO webhook_processed_records (table_name, last_processed_id)
      VALUES 
        ('form_submits', 0),
        ('subscribers', 0),
        ('blog_posts', 0),
        ('blog_comments', 0)
    `);
    
    console.log('Tracking table created successfully');
  } catch (err) {
    console.error('Error creating tracking table:', err);
  }
}

async function initializeLastKnownIds() {
  try {
    // Get the last processed IDs from the tracking table for all monitored tables
    const [rows] = await pool.query(`
      SELECT table_name, last_processed_id FROM webhook_processed_records
      WHERE table_name IN ('form_submits', 'subscribers', 'blog_posts', 'blog_comments')
    `);
    
    // Process each table's tracking info
    for (const row of rows) {
      const tableName = row.table_name;
      const lastProcessedId = row.last_processed_id;
      
      if (tableName === 'form_submits') {
        lastKnownFormSubmitId = lastProcessedId;
        console.log(`Initialized lastKnownFormSubmitId to ${lastKnownFormSubmitId}`);
      }
      else if (tableName === 'subscribers') {
        lastKnownSubscriberId = lastProcessedId;
        console.log(`Initialized lastKnownSubscriberId to ${lastKnownSubscriberId}`);
      }
      else if (tableName === 'blog_comments') {
        lastKnownBlogCommentId = lastProcessedId;
        console.log(`Initialized lastKnownBlogCommentId to ${lastKnownBlogCommentId}`);
      }
      
      // For tables where we need to track specific column changes (like blog_posts)
      if (tableName === 'blog_posts') {
        // Initialize the state of blog_posts view_count for tracking changes
        await initializeBlogPostsState();
      }
    }
    
    // For any table without an existing row, find the highest existing ID
    if (!rows.find(r => r.table_name === 'form_submits')) {
      await initializeTableMaxId('form_submits', 'lastKnownFormSubmitId');
    }
    
    if (!rows.find(r => r.table_name === 'subscribers')) {
      await initializeTableMaxId('subscribers', 'lastKnownSubscriberId');
    }
    
    if (!rows.find(r => r.table_name === 'blog_comments')) {
      await initializeTableMaxId('blog_comments', 'lastKnownBlogCommentId');
    }
    
    if (!rows.find(r => r.table_name === 'blog_posts')) {
      await initializeTableMaxId('blog_posts');
      await initializeBlogPostsState();
    }
    
    // Mark that we've completed the initial check and setup
    isInitialCheckComplete = true;
  } catch (err) {
    console.error('Error initializing lastKnownIds:', err);
  }
}

async function initializeTableMaxId(tableName, variableName = null) {
  try {
    // Find the highest existing ID
    const [maxRows] = await pool.query(`
      SELECT COALESCE(MAX(id), 0) AS max_id FROM ${tableName}
    `);
    
    const maxId = maxRows[0].max_id || 0;
    
    // Set the variable if provided
    if (variableName) {
      global[variableName] = maxId;
      console.log(`No tracking record found for ${tableName}. Set ${variableName} to current max: ${maxId}`);
    } else {
      console.log(`No tracking record found for ${tableName}. Set max ID to: ${maxId}`);
    }
    
    // Update the tracking table
    await pool.query(`
      INSERT INTO webhook_processed_records (table_name, last_processed_id)
      VALUES (?, ?)
      ON DUPLICATE KEY UPDATE last_processed_id = ?
    `, [tableName, maxId, maxId]);
    
    return maxId;
  } catch (err) {
    console.error(`Error initializing max ID for ${tableName}:`, err);
    return 0;
  }
}

async function initializeBlogPostsState() {
  try {
    // Get current state of all blog posts view_counts
    const [posts] = await pool.query(`
      SELECT id, view_count FROM blog_posts
    `);
    
    // Store in our tracking object
    lastKnownBlogPostsState = {};
    posts.forEach(post => {
      lastKnownBlogPostsState[post.id] = post.view_count;
    });
    
    console.log(`Initialized state for ${posts.length} blog posts`);
  } catch (err) {
    console.error('Error initializing blog posts state:', err);
    lastKnownBlogPostsState = {};
  }
}

function startRecordPolling() {
  // First immediate check
  checkForNewRecords();
  
  // Then check every 10 seconds
  setInterval(checkForNewRecords, 10000);
}

async function checkForNewRecords() {
  if (!POWER_AUTOMATE_WEBHOOK_URL || !isInitialCheckComplete) {
    return;
  }
  
  try {
    // Check for new records in each monitored table
    await checkForNewFormSubmissions();
    await checkForNewSubscribers();
    await checkForNewBlogComments();
    await checkForBlogPostViewCountChanges();
  } catch (err) {
    console.error('Error during record polling:', err);
  }
}

async function checkForNewFormSubmissions() {
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
      
      console.log(`Updated form_submits last processed ID to ${highestProcessedId}`);
    }
  } catch (err) {
    console.error('Error checking for new form submissions:', err);
  }
}

async function checkForNewSubscribers() {
  try {
    // Get current tracked ID from database to ensure we're using the latest value
    const [trackingRow] = await pool.query(`
      SELECT last_processed_id FROM webhook_processed_records
      WHERE table_name = 'subscribers'
    `);
    
    if (trackingRow.length === 0) {
      console.error('No tracking record found for subscribers');
      return;
    }
    
    const currentTrackedId = trackingRow[0].last_processed_id;
    
    // Check for new subscribers with ID greater than the tracked ID
    const [newSubscribers] = await pool.query(`
      SELECT * FROM subscribers
      WHERE id > ?
      ORDER BY id ASC
      LIMIT 50
    `, [currentTrackedId]);
    
    if (newSubscribers.length === 0) {
      return;
    }
    
    console.log(`Found ${newSubscribers.length} new subscribers to process (IDs > ${currentTrackedId})`);
    
    let highestProcessedId = currentTrackedId;
    
    for (const subscriber of newSubscribers) {
      try {
        if (subscriber.id > highestProcessedId) {
          // Send the webhook notification
          await sendWebhookNotification('subscribers', subscriber);
          
          // Update our tracking variable
          highestProcessedId = subscriber.id;
          
          console.log(`Successfully processed subscriber ID: ${subscriber.id}`);
        } else {
          console.log(`Skipping already processed subscriber ID: ${subscriber.id}`);
        }
      } catch (err) {
        console.error(`Failed to process subscriber ID ${subscriber.id}:`, err);
        // Continue with next subscriber even if this one fails
      }
    }
    
    // Only update the database if we actually processed new records
    if (highestProcessedId > currentTrackedId) {
      // Update our in-memory tracking
      lastKnownSubscriberId = highestProcessedId;
      
      // Update the tracking table after processing all subscribers
      await pool.query(`
        UPDATE webhook_processed_records
        SET last_processed_id = ?, last_check_time = CURRENT_TIMESTAMP
        WHERE table_name = 'subscribers'
      `, [highestProcessedId]);
      
      console.log(`Updated subscribers last processed ID to ${highestProcessedId}`);
    }
  } catch (err) {
    console.error('Error checking for new subscribers:', err);
  }
}

async function checkForNewBlogComments() {
  try {
    // Get current tracked ID from database to ensure we're using the latest value
    const [trackingRow] = await pool.query(`
      SELECT last_processed_id FROM webhook_processed_records
      WHERE table_name = 'blog_comments'
    `);
    
    if (trackingRow.length === 0) {
      console.error('No tracking record found for blog_comments');
      return;
    }
    
    const currentTrackedId = trackingRow[0].last_processed_id;
    
    // Check for new blog comments with ID greater than the tracked ID
    const [newComments] = await pool.query(`
      SELECT * FROM blog_comments
      WHERE id > ?
      ORDER BY id ASC
      LIMIT 50
    `, [currentTrackedId]);
    
    if (newComments.length === 0) {
      return;
    }
    
    console.log(`Found ${newComments.length} new blog comments to process (IDs > ${currentTrackedId})`);
    
    let highestProcessedId = currentTrackedId;
    
    for (const comment of newComments) {
      try {
        if (comment.id > highestProcessedId) {
          // Send the webhook notification
          await sendWebhookNotification('blog_comments', comment);
          
          // Update our tracking variable
          highestProcessedId = comment.id;
          
          console.log(`Successfully processed blog comment ID: ${comment.id}`);
        } else {
          console.log(`Skipping already processed blog comment ID: ${comment.id}`);
        }
      } catch (err) {
        console.error(`Failed to process blog comment ID ${comment.id}:`, err);
        // Continue with next comment even if this one fails
      }
    }
    
    // Only update the database if we actually processed new records
    if (highestProcessedId > currentTrackedId) {
      // Update our in-memory tracking
      lastKnownBlogCommentId = highestProcessedId;
      
      // Update the tracking table after processing all comments
      await pool.query(`
        UPDATE webhook_processed_records
        SET last_processed_id = ?, last_check_time = CURRENT_TIMESTAMP
        WHERE table_name = 'blog_comments'
      `, [highestProcessedId]);
      
      console.log(`Updated blog_comments last processed ID to ${highestProcessedId}`);
    }
  } catch (err) {
    console.error('Error checking for new blog comments:', err);
  }
}

async function checkForBlogPostViewCountChanges() {
  try {
    // Get all current blog posts with their view counts
    const [currentPosts] = await pool.query(`
      SELECT id, view_count FROM blog_posts
    `);
    
    const postsWithChangedViewCount = [];
    
    // Check for changes in view_count
    for (const post of currentPosts) {
      const previousViewCount = lastKnownBlogPostsState[post.id];
      
      // If this is a new post or the view_count has changed
      if (previousViewCount === undefined || previousViewCount !== post.view_count) {
        // Get full post details for the webhook
        const [fullPostDetails] = await pool.query(`
          SELECT * FROM blog_posts WHERE id = ?
        `, [post.id]);
        
        if (fullPostDetails.length > 0) {
          // Add change information
          const postWithChange = fullPostDetails[0];
          postWithChange._change_details = {
            previous_view_count: previousViewCount === undefined ? 0 : previousViewCount,
            new_view_count: post.view_count,
            difference: previousViewCount === undefined ? post.view_count : post.view_count - previousViewCount
          };
          
          postsWithChangedViewCount.push(postWithChange);
          
          // Update our tracking state
          lastKnownBlogPostsState[post.id] = post.view_count;
        }
      }
    }
    
    if (postsWithChangedViewCount.length === 0) {
      return;
    }
    
    console.log(`Found ${postsWithChangedViewCount.length} blog posts with changed view_count to process`);
    
    // Send webhook notifications for each changed post
    for (const post of postsWithChangedViewCount) {
      try {
        await sendWebhookNotification('blog_posts_view_count_change', post);
        console.log(`Successfully processed blog post ID: ${post.id} view_count change to ${post.view_count}`);
      } catch (err) {
        console.error(`Failed to process blog post ID ${post.id} view_count change:`, err);
      }
    }
    
    // Update the tracking table with current timestamp to show we checked
    const highestBlogPostId = Math.max(...currentPosts.map(post => post.id), 0);
    await pool.query(`
      UPDATE webhook_processed_records
      SET last_check_time = CURRENT_TIMESTAMP, last_processed_id = ?
      WHERE table_name = 'blog_posts'
    `, [highestBlogPostId]);
    
  } catch (err) {
    console.error('Error checking for blog post view_count changes:', err);
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
    // if (POWER_AUTOMATE_WEBHOOK_URL) {
    //   try {
    //     await sendWebhookNotification(tableName, { id: newRecordId, ...data });
    //   } catch (error) {
    //     console.error(`[${tableName}] Failed to send webhook notification, but will continue.`);
    //   }
    // }

    res.json({ id: newRecordId, ...data });
  } catch (err) {
    console.error(`[${tableName}] Database error during INSERT:`, err);
    res.status(500).json({ error: err.message });
  }
});

// Update record with flexible body handling
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
    let queryCondition, queryParams;
    
    // Check if idOrGuid is numeric (likely an ID) or string (likely a GUID)
    if (!isNaN(idOrGuid)) {
      console.log(`[${tableName}] Executing UPDATE query for id: ${idOrGuid} with data:`, JSON.stringify(data));
      queryCondition = 'id = ?';
      queryParams = [data, idOrGuid];
    } else {
      console.log(`[${tableName}] Executing UPDATE query for guid: ${idOrGuid} with data:`, JSON.stringify(data));
      queryCondition = 'guid = ?';
      queryParams = [data, idOrGuid];
    }
    
    const [updateResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE ${queryCondition}`, queryParams);
    
    if (updateResult.affectedRows === 0) {
      // If no rows were updated with the first condition, try the other one
      if (!isNaN(idOrGuid)) {
        // If we tried ID first, now try GUID
        console.log(`[${tableName}] No rows updated by id, trying guid: ${idOrGuid}`);
        const [retryResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE guid = ?`, [data, idOrGuid]);
        if (retryResult.affectedRows === 0) {
          return res.status(404).json({ error: 'Record not found' });
        }
      } else {
        // If we tried GUID first, now try ID (if it could potentially be numeric)
        if (idOrGuid.match(/^\d+$/)) {
          console.log(`[${tableName}] No rows updated by guid, trying id: ${idOrGuid}`);
          const [retryResult] = await pool.query(`UPDATE ${tableName} SET ? WHERE id = ?`, [data, idOrGuid]);
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