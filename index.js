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

// Check if the webhook  URL is set
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
    // Polls regardless of webhook — also handles direct email notifications
    startRecordPolling();
    
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
  if (!isInitialCheckComplete) {
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
          // Send email notification directly
          await sendFormSubmissionEmail(submission);

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
          // Send email notification directly
          await sendSubscriberWelcomeEmail(subscriber);

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
          // Send email notification directly
          await sendCommentNotificationEmail(comment);

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
      headers: { 'Content-Type': 'application/json' },
      timeout: 10000
    });

    console.log(`Webhook notification sent. Status: ${response.status}`);
    return true;
  } catch (error) {
    // Log but don't throw — webhook failures should not break the polling loop
    console.warn(
      `Webhook notification failed (non-blocking):`,
      error.response?.status || error.code,
      error.response?.statusText || error.message
    );
    return false;
  }
}

// ============================================================
// EMAIL NOTIFICATION TEMPLATES (replaces Power Automate email)
// ============================================================
const ADMIN_EMAIL = 'mohanad.elsayed@chemican.ca';
const ADMIN_BCC = 'mohanad.elsayed@chemican.ca;support@chemican.ca';

function decodeHtmlEntities(str) {
  if (!str) return '';
  return str.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&#39;/g, "'");
}

function formatDate() {
  const now = new Date();
  return now.toLocaleDateString('en-GB');  // dd/MM/yyyy
}

// BU 0 = Consulting (chemican.ca), BU 1 = Solutions (solutions.chemican.ca)
function getBUConfig(bu) {
  if (parseInt(bu) === 1) {
    return {
      name: 'ChemiCan Solutions',
      logo: 'https://www.solutions.chemican.ca/images/Pictussre1.png',
      color: '#0078d4',
      from: SMTP_FROM || 'info@solutions.chemican.ca',
      website: 'https://www.solutions.chemican.ca/',
      email: 'info@solutions.chemican.ca',
      phone: '+1 (825) 609-8387',
      serviceLabel: 'digital services',
      ctaText: 'Explore our services',
      bcc: ADMIN_EMAIL
    };
  }
  return {
    name: 'ChemiCan Consulting',
    logo: 'https://www.chemican.ca/images/logo/chemican-logo.png',
    color: '#e63946',
    from: SMTP_FROM || 'support@chemican.ca',
    website: 'https://www.chemican.ca/',
    email: 'support@chemican.ca',
    phone: '+1 (403) 80-4343',
    serviceLabel: 'services',
    ctaText: 'Explore our services',
    bcc: ADMIN_BCC
  };
}

function buildEmailWrapper(bu, title, subtitle, bodyContent, ctaUrl, ctaText) {
  const cfg = getBUConfig(bu);
  return `<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>body, table, td { font-family: 'Arial', sans-serif !important; } a { color: ${cfg.color}; text-decoration: none; }</style>
</head>
<body>
  <div style="background:white;min-height:100vh;color:#333;font-size:14px;">
    <table border="0" cellpadding="0" cellspacing="0" width="100%" height="100%">
      <tr><td></td><td width="640">
        <table border="0" cellpadding="0" cellspacing="0" style="min-width:100%;background:white;">
          <tr><td style="padding:24px 24px 30px;"><img src="${cfg.logo}" width="100" height="auto" alt="${cfg.name}"></td></tr>
          <tr><td style="font-size:28px;padding:0 24px;font-weight:bold;color:${cfg.color}">${title}</td></tr>
          <tr><td style="color:#333;padding:20px 24px 30px 24px;"><span style="font-weight:600">${subtitle}</span></td></tr>
          <tr><td style="padding:0 24px 44px;">
            <div><table style="font-size:100%;" width="100%" cellpadding="0" cellspacing="0" border="0" align="center">
              ${bodyContent}
            </table></div>
            ${ctaUrl ? `<div style="padding-top:10px;"><a href="${ctaUrl}" target="_blank" style="background-color:${cfg.color};color:white;padding:10px 20px;text-decoration:none;border-radius:4px;display:inline-block;">${ctaText || cfg.ctaText}</a></div>` : ''}
          </td></tr>
          <tr><td>
            <table border="0" cellpadding="0" cellspacing="0" width="100%" style="min-width:100%;background-color:#F3F3F3;">
              <tr><td style="padding:20px 25px 10px;text-align:center;"></td></tr>
              <tr><td style="padding:10px 25px 25px;font-size:12px;text-align:center;">
                <div style="color:#666;margin-bottom:10px;">${cfg.name}<br>Alberta, Canada<br><a href="mailto:${cfg.email}" style="color:${cfg.color};">${cfg.email}</a></div>
                <div style="margin-top:15px;"><img src="${cfg.logo}" width="40" height="auto" alt="${cfg.name}"></div>
              </td></tr>
            </table>
          </td></tr>
        </table>
      </td><td></td></tr>
    </table>
  </div>
</body>
</html>`;
}

async function sendFormSubmissionEmail(submission) {
  if (!emailTransporter) return;
  const cfg = getBUConfig(submission.BU);
  const refId = submission.id;
  const name = decodeHtmlEntities(submission.name);
  const message = decodeHtmlEntities(submission.message);

  const bodyContent = `
    <p style="margin-top:0">We've received your inquiry and we'll get back to you shortly. Your interest in our ${cfg.serviceLabel} is much appreciated.</p>
    <p><b style="font-weight:600">Your inquiry details:</b></p>
    <ul>
      <li>Reference Number: INQ-${refId}</li>
      <li>Submitted: ${formatDate()}</li>
    </ul>
    <p><b style="font-weight:600">What happens next?</b></p>
    <p>One of our specialists will review your request and be in touch within 1-2 business days. If you have any immediate questions, feel free to call us at <a href="tel:${cfg.phone}">${cfg.phone}</a></p>
    <br>`;

  const html = buildEmailWrapper(submission.BU,
    "We've Got Your Message!",
    "Thanks for reaching out!",
    bodyContent,
    cfg.website,
    cfg.ctaText
  );

  try {
    await emailTransporter.sendMail({
      from: cfg.from,
      to: decodeHtmlEntities(submission.email),
      bcc: cfg.bcc,
      subject: `Your ${cfg.name} Inquiry INQ-${refId} - We've Received Your Message!`,
      html
    });
    console.log(`Form submission email sent to ${submission.email} (INQ-${refId})`);
  } catch (err) {
    console.error('Email send error:', err.message);
  }
}

async function sendSubscriberWelcomeEmail(subscriber) {
  if (!emailTransporter) return;
  const cfg = getBUConfig(subscriber.BU);
  const blogUrl = parseInt(subscriber.BU) === 1
    ? 'https://www.solutions.chemican.ca/insights/blog.html'
    : 'https://www.chemican.ca/insights/blog.html';

  const revolution = parseInt(subscriber.BU) === 1
    ? 'Digitatl Transformation revolution'
    : 'sustainability revolution';
  const insightType = parseInt(subscriber.BU) === 1
    ? 'industry insights, digitalization tips, and the latest innovative solutions'
    : 'industry insights, sustainability tips, and the latest environmental innovations';
  const resourceType = parseInt(subscriber.BU) === 1
    ? 'immediate technology updates'
    : 'immediate sustainability wisdom';

  const bodyContent = `
    <p style="margin-top:0;line-height:1.6;">We're thrilled to have you join the ChemiCan community! Your inbox is now set to receive our weekly digest packed with ${insightType}.</p>
    <p style="line-height:1.6;"><b style="font-weight:600;color:${cfg.color};">Our promise to you:</b></p>
    <ul style="line-height:1.6;padding-left:20px;">
      <li><b>No inbox flooding</b> - Just one awesome weekly digest</li>
      <li><b>Quality content only</b> - Curated insights you can actually use</li>
      <li><b>Easy opt-out</b> - Freedom to leave anytime (but we hope you'll stay!)</li>
    </ul>
    <p style="line-height:1.6;margin-top:25px;">Your first digest will arrive next week. Until then, feel free to explore our <a href="${blogUrl}" style="color:${cfg.color};font-weight:bold;">resource center</a> for ${resourceType}!</p>
    <br>`;

  const html = buildEmailWrapper(subscriber.BU,
    "You're In! \u{1F389}",
    `Thanks for joining our ${revolution}!`,
    bodyContent,
    blogUrl,
    'Check out our latest articles'
  );

  try {
    await emailTransporter.sendMail({
      from: cfg.from,
      to: subscriber.email,
      bcc: cfg.bcc,
      subject: parseInt(subscriber.BU) === 1
        ? "Welcome to the ChemiCan community! Your weekly dose of Technology insights"
        : "Welcome to the ChemiCan community! Your weekly dose of sustainability insights",
      html
    });
    console.log(`Subscriber welcome email sent to ${subscriber.email}`);
  } catch (err) {
    console.error('Email send error:', err.message);
  }
}

async function sendCommentNotificationEmail(comment) {
  if (!emailTransporter) return;
  // Look up the blog post title
  let postTitle = 'Unknown Post';
  try {
    const [posts] = await pool.query('SELECT title FROM blog_posts WHERE id = ?', [comment.post_id]);
    if (posts.length) postTitle = posts[0].title;
  } catch (e) { /* ignore */ }

  try {
    await emailTransporter.sendMail({
      from: SMTP_FROM || 'support@chemican.ca',
      to: ADMIN_EMAIL,
      subject: 'New Comment is waiting for your approval',
      html: `<p>Hello<br><br>A new comment has been submitted and is waiting for your approval.<br><br>
        <b>Post:</b> ${postTitle}<br>
        <b>Author:</b> ${decodeHtmlEntities(comment.author_name)} (${decodeHtmlEntities(comment.author_email)})<br>
        <b>Comment:</b> ${decodeHtmlEntities(comment.content)}<br><br>
        Please review and approve or reject the comment in the <a href="https://org49aacc6f.crm3.dynamics.com">ChemiCan ERP</a>.</p>`
    });
    console.log(`Comment notification email sent for post: ${postTitle}`);
  } catch (err) {
    console.error('Email send error:', err.message);
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

// Search records by field value (e.g. GET /api/tables/blog_posts/search?field=slug&value=my-post)
app.get('/api/tables/:tableName/search', async (req, res) => {
  const tableName = req.params.tableName;
  const { field, value } = req.query;

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  if (!field || !value) {
    return res.status(400).json({ error: 'Both field and value query params are required' });
  }
  if (!/^[a-zA-Z0-9_]+$/.test(field)) {
    return res.status(400).json({ error: 'Invalid field name' });
  }

  try {
    const [results] = await pool.query(`SELECT * FROM ${tableName} WHERE ?? = ? LIMIT 1`, [field, value]);
    res.json(results[0] || null);
  } catch (err) {
    console.error(`[${tableName}] Search error:`, err);
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

  // Allow guid updates only when explicitly the sole field (migration/sync use case)
  // Previously stripped guid to prevent accidental overwrites, but sync needs it

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

// Delete by multiple field conditions (for junction tables with composite PKs)
// e.g. DELETE /api/tables/blog_post_tags/where?post_id=76&tag_id=5
app.delete('/api/tables/:tableName/where', async (req, res) => {
  const tableName = req.params.tableName;
  const conditions = req.query;

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }

  const fields = Object.keys(conditions);
  if (fields.length === 0) {
    return res.status(400).json({ error: 'At least one query parameter is required' });
  }
  if (fields.some(f => !/^[a-zA-Z0-9_]+$/.test(f))) {
    return res.status(400).json({ error: 'Invalid field name' });
  }

  try {
    const whereClauses = fields.map(f => `\`${f}\` = ?`).join(' AND ');
    const values = fields.map(f => conditions[f]);
    console.log(`[${tableName}] Executing DELETE WHERE ${whereClauses}`, values);
    const [deleteResult] = await pool.query(`DELETE FROM ${tableName} WHERE ${whereClauses}`, values);

    if (deleteResult.affectedRows > 0) {
      return res.json({ message: 'Record(s) deleted successfully', affectedRows: deleteResult.affectedRows });
    }
    return res.status(404).json({ error: 'No matching records found' });
  } catch (err) {
    console.error(`[${tableName}] Database error during DELETE WHERE:`, err);
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

// ============================================================
// EMAIL NOTIFICATIONS (via nodemailer)
// ============================================================
const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = process.env.SMTP_PORT || 587;
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;
const SMTP_FROM = process.env.SMTP_FROM || 'info@solutions.chemican.ca';

let emailTransporter = null;

async function initializeEmailTransporter() {
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS) {
    console.warn('SMTP not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASS env vars to enable email.');
    return;
  }
  try {
    const nodemailer = require('nodemailer');
    const smtpPort = parseInt(SMTP_PORT);
    emailTransporter = nodemailer.createTransport({
      host: SMTP_HOST,
      port: smtpPort,
      secure: smtpPort === 465,
      requireTLS: smtpPort === 587,
      auth: {
        user: SMTP_USER,
        pass: SMTP_PASS
      },
      tls: { rejectUnauthorized: false },
      connectionTimeout: 30000,
      greetingTimeout: 30000,
      socketTimeout: 60000,
      logger: true,
      debug: true
    });
    console.log(`Email transporter configured (${SMTP_HOST}:${smtpPort}, user: ${SMTP_USER})`);
    // Verify connection — await so we know immediately
    try {
      await emailTransporter.verify();
      console.log('SMTP connection verified successfully — emails will be sent');
    } catch (err) {
      console.error('========================================');
      console.error('SMTP VERIFICATION FAILED:', err.message);
      console.error('Error code:', err.code);
      if (err.message.includes('535') || err.message.includes('Authentication')) {
        console.error('>>> AUTH FAILURE: If MFA/2FA is enabled on this Office 365 account,');
        console.error('>>> basic SMTP auth is blocked. Create an App Password at:');
        console.error('>>> https://mysignins.microsoft.com/security-info');
        console.error('>>> Or enable SMTP AUTH in M365 Admin > Users > Mail > Manage email apps');
      }
      console.error('========================================');
      // Keep the transporter — it may work for some messages even if verify fails
    }
  } catch (e) {
    console.warn('nodemailer not installed or SMTP config error:', e.message);
    console.warn('Run: npm install nodemailer');
  }
}

// Proxy-fetch a public image URL (bypasses CORS for browser clients)
app.post('/api/fetch-image', async (req, res) => {
  const { url } = req.body;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url is required' });
  }
  try {
    const response = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 30000,
      maxContentLength: 25 * 1024 * 1024,
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    });
    const contentType = response.headers['content-type'] || 'image/jpeg';
    const base64 = Buffer.from(response.data).toString('base64');
    res.json({ data: base64, contentType, size: response.data.length });
  } catch (err) {
    console.error('Fetch-image error:', err.message);
    res.status(502).json({ error: 'Failed to fetch image: ' + (err.response?.status || err.message) });
  }
});

// Send email endpoint
app.post('/api/send-email', async (req, res) => {
  if (!emailTransporter) {
    return res.status(503).json({ error: 'Email service not configured. Set SMTP env vars.' });
  }
  const { to, subject, html, text } = req.body;
  if (!to || !subject) {
    return res.status(400).json({ error: 'Missing required fields: to, subject' });
  }
  try {
    const info = await emailTransporter.sendMail({
      from: SMTP_FROM,
      to,
      subject,
      html: html || undefined,
      text: text || undefined
    });
    console.log('Email sent:', info.messageId);
    res.json({ success: true, messageId: info.messageId });
  } catch (err) {
    console.error('Email send error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Graceful shutdown — let in-flight operations finish
let shuttingDown = false;
process.on('SIGTERM', () => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('SIGTERM received — graceful shutdown in 5s...');
  setTimeout(() => {
    console.log('Shutting down now.');
    process.exit(0);
  }, 5000);
});
process.on('SIGINT', () => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('SIGINT received — shutting down...');
  process.exit(0);
});

// Initialize the database, email, and start the server
initializeDatabase().then(async () => {
  // Initialize email transporter (await so verify completes before polling sends)
  await initializeEmailTransporter();

  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
    console.log(`Polling active: every 10s | Email: ${emailTransporter ? 'configured' : 'disabled'}`);
  });
}).catch(err => {
  console.error('Failed to initialize application:', err);
  process.exit(1);
});
