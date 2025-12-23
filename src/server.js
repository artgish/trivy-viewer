const express = require('express');
const path = require('path');
const { createStorageProvider, getProviderType } = require('./providers');

const app = express();
const PORT = process.env.PORT || 3000;

// Storage Configuration
const STORAGE_LOCATION = process.env.STORAGE_LOCATION;
if (!STORAGE_LOCATION) {
  throw new Error("STORAGE_LOCATION env var is required");
}
const STORAGE_PREFIX = process.env.STORAGE_PREFIX || '';

// Create storage provider based on STORAGE_LOCATION
const storageProvider = createStorageProvider(STORAGE_LOCATION, STORAGE_PREFIX);

// Middleware
app.use(express.json({ limit: '11mb' })); // 10 MiB + buffer for JSON overhead

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} ${res.statusCode} ${duration}ms`);
  });
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

// API Routes

// List JSON files and directories (active path) with pagination
// Supports recursive directory navigation via 'path' query parameter
app.get('/api/files', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 100);
    const continuationToken = req.query.continuationToken || undefined;
    const subPath = req.query.path || '';

    // Build the full path: activePath + optional subPath
    let fullPath = storageProvider.activePath;
    if (subPath) {
      // Sanitize subPath to prevent directory traversal
      const sanitizedPath = subPath.split('/').filter(p => p && p !== '..').join('/');
      fullPath = `${storageProvider.activePath}/${sanitizedPath}`;
    }

    const result = await storageProvider.listFiles(
      fullPath,
      limit,
      continuationToken
    );

    // Include the current path in response for navigation
    res.json({
      ...result,
      currentPath: subPath || ''
    });
  } catch (error) {
    console.error('Error listing files:', error);
    res.status(500).json({ error: 'Failed to list files', details: error.message });
  }
});

const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10 MiB

// Upload a file to active path
app.post('/api/files/upload', async (req, res) => {
  try {
    const { filename, content } = req.body;

    if (!filename || !content) {
      return res.status(400).json({ error: 'Filename and content are required' });
    }

    // Validate filename
    if (!/^[\w\-. ]+\.json$/i.test(filename)) {
      return res.status(400).json({ error: 'Invalid filename. Must be a .json file with alphanumeric characters, dashes, underscores, dots, or spaces.' });
    }

    // Validate content size (approximate check - already limited by express.json middleware)
    const contentStr = JSON.stringify(content);
    if (Buffer.byteLength(contentStr, 'utf8') > MAX_FILE_SIZE) {
      return res.status(400).json({ error: 'File content exceeds 10 MiB limit' });
    }

    // Validate Trivy report structure
    if (!content.Results || !Array.isArray(content.Results)) {
      return res.status(400).json({ error: 'Invalid Trivy report format. Must contain Results array.' });
    }

    // Save file
    const result = await storageProvider.saveFile(filename, content);

    res.json({
      message: 'File uploaded successfully',
      filename: result.filename,
      key: result.key
    });
  } catch (error) {
    console.error('Error uploading file:', error);
    res.status(500).json({ error: 'Failed to upload file', details: error.message });
  }
});

// Archive a file (move from active to archived)
app.post('/api/files/archive', async (req, res) => {
  try {
    var { key } = req.body;

    if (!key) {
      return res.status(400).json({ error: 'File key is required' });
    }
    console.log(key)
    // Verify the file is in the active path
    if (!key.startsWith(storageProvider.activePath)) {
      key = path.join(storageProvider.activePath, key)
    }

    const filename = path.basename(key);
    const isoDate = new Date().toISOString();
    const archivedKey = `${storageProvider.archivedPath}/${filename}.${isoDate}`;

    // Copy file to archived path
    await storageProvider.copyFile(key, archivedKey);

    // Delete original file from active path
    await storageProvider.deleteFile(key);

    res.json({
      message: 'File archived successfully',
      originalKey: key,
      archivedKey: archivedKey
    });
  } catch (error) {
    console.error('Error archiving file:', error);
    res.status(500).json({ error: 'Failed to archive file', details: error.message });
  }
});

// Get a specific file
app.get('/api/files/:key(*)', async (req, res) => {
  try {
    let key = req.params.key;
    if (!key.startsWith(`${storageProvider.prefix}/active/`)) {
      key = `${storageProvider.prefix}/active/${key}`;
    }
    const data = await storageProvider.getFile(key);
    res.json(data);
  } catch (error) {
    console.error('Error fetching file:', error);
    if (error.name === 'NoSuchKey' || error.code === 'ENOENT' || error.code === 404) {
      res.status(404).json({ error: 'File not found' });
    } else {
      res.status(500).json({ error: 'Failed to fetch file', details: error.message });
    }
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Direct file viewer - serves HTML which will auto-load the file
// Supports nested paths like /files/subdir/report.json
app.get('/files/:filepath(*)', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve the main HTML file for all other routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Server error:', error);
  res.status(500).json({ error: 'Internal server error', details: error.message });
});

app.listen(PORT, () => {
  console.log(`Trivy Viewer server running on http://localhost:${PORT}`);
  console.log(`Storage Provider: ${getProviderType(STORAGE_LOCATION)}`);
  console.log(`Storage Location: ${STORAGE_LOCATION}`);
  console.log(`Storage Prefix: ${STORAGE_PREFIX}`);
  console.log(`Active Path: ${storageProvider.activePath}`);
  console.log(`Archived Path: ${storageProvider.archivedPath}`);
});
