const express = require('express');
const path = require('path');
const { S3Client, ListObjectsV2Command, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
const PORT = process.env.PORT || 3000;

// S3 Configuration
const s3Client = new S3Client();

const S3_BUCKET = process.env.S3_BUCKET;
if (!S3_BUCKET) {
  throw new Error("S3_BUCKET env var is required");
}
const S3_PREFIX = process.env.S3_PREFIX || '';
const ACTIVE_PATH = `${S3_PREFIX}/active`;
const ARCHIVED_PATH = `${S3_PREFIX}/archived`;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Helper function to convert stream to string
async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

// API Routes

// List JSON files in S3 bucket (active path only) with pagination
app.get('/api/files', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 1000, 1000);
    const continuationToken = req.query.continuationToken || undefined;

    const command = new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: ACTIVE_PATH,
      MaxKeys: limit,
      ContinuationToken: continuationToken
    });

    const response = await s3Client.send(command);

    const files = (response.Contents || [])
      .filter(obj => obj.Key.endsWith('.json'))
      .map(obj => ({
        key: obj.Key,
        name: path.basename(obj.Key),
        size: obj.Size,
        lastModified: obj.LastModified
      }));

    res.json({
      files,
      nextContinuationToken: response.NextContinuationToken || null,
      hasMore: response.IsTruncated || false
    });
  } catch (error) {
    console.error('Error listing S3 files:', error);
    res.status(500).json({ error: 'Failed to list files from S3', details: error.message });
  }
});

// Archive a file (move from active to archived)
app.post('/api/files/archive', async (req, res) => {
  try {
    const { key } = req.body;

    if (!key) {
      return res.status(400).json({ error: 'File key is required' });
    }

    // Verify the file is in the active path
    if (!key.startsWith(ACTIVE_PATH)) {
      return res.status(400).json({ error: 'File is not in the active path' });
    }

    const filename = path.basename(key);
    const archivedKey = `${ARCHIVED_PATH}/${filename}`;

    // Copy file to archived path
    const copyCommand = new CopyObjectCommand({
      Bucket: S3_BUCKET,
      CopySource: `${S3_BUCKET}/${key}`,
      Key: archivedKey
    });
    await s3Client.send(copyCommand);

    // Delete original file from active path
    const deleteCommand = new DeleteObjectCommand({
      Bucket: S3_BUCKET,
      Key: key
    });
    await s3Client.send(deleteCommand);

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

// Get a specific file from S3
app.get('/api/files/:key(*)', async (req, res) => {
  try {
    const key = req.params.key;

    const command = new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: key
    });

    const response = await s3Client.send(command);
    const content = await streamToString(response.Body);
    const data = JSON.parse(content);

    res.json(data);
  } catch (error) {
    console.error('Error fetching S3 file:', error);
    if (error.name === 'NoSuchKey') {
      res.status(404).json({ error: 'File not found' });
    } else {
      res.status(500).json({ error: 'Failed to fetch file from S3', details: error.message });
    }
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
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
  console.log(`S3 Bucket: ${S3_BUCKET}`);
  console.log(`S3 Prefix: ${S3_PREFIX}`);
  console.log(`Active Path: ${ACTIVE_PATH}`);
  console.log(`Archived Path: ${ARCHIVED_PATH}`);
});
