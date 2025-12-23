const { S3Client, ListObjectsV2Command, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const path = require('path');

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

class S3StorageProvider {
  constructor(bucket, prefix = '') {
    this.bucket = bucket;
    this.prefix = prefix;
    this.client = new S3Client();
    this.activePath = `${prefix}/active`.replace(/^\//, '');
    this.archivedPath = `${prefix}/archived`.replace(/^\//, '');
  }

  async listFiles(subPath, limit = 100, continuationToken = undefined) {
    // Ensure subPath ends with / for proper delimiter behavior
    const prefix = subPath.endsWith('/') ? subPath : `${subPath}/`;

    const command = new ListObjectsV2Command({
      Bucket: this.bucket,
      Prefix: prefix,
      Delimiter: '/',
      MaxKeys: limit,
      ContinuationToken: continuationToken || undefined
    });

    const response = await this.client.send(command);

    // Extract directories (CommonPrefixes)
    const directories = (response.CommonPrefixes || []).map(prefix => {
      const dirPath = prefix.Prefix;
      // Get the directory name (last segment before trailing /)
      const name = dirPath.slice(0, -1).split('/').pop();
      return {
        key: dirPath,
        name: name,
        type: 'directory'
      };
    });

    // Extract files
    const files = (response.Contents || [])
      .filter(obj => obj.Key.endsWith('.json') && obj.Key !== prefix)
      .map(obj => ({
        key: obj.Key,
        name: path.basename(obj.Key),
        size: obj.Size,
        lastModified: obj.LastModified,
        type: 'file'
      }));

    // Combine directories first, then files
    const items = [...directories, ...files];

    return {
      files: items,
      nextContinuationToken: response.NextContinuationToken || null,
      hasMore: response.IsTruncated || false
    };
  }

  async getFile(key) {
    const command = new GetObjectCommand({
      Bucket: this.bucket,
      Key: key
    });

    const response = await this.client.send(command);
    const content = await streamToString(response.Body);
    return JSON.parse(content);
  }

  async copyFile(sourceKey, destKey) {
    const command = new CopyObjectCommand({
      Bucket: this.bucket,
      CopySource: `${this.bucket}/${sourceKey}`,
      Key: destKey
    });
    await this.client.send(command);
  }

  async deleteFile(key) {
    const command = new DeleteObjectCommand({
      Bucket: this.bucket,
      Key: key
    });
    await this.client.send(command);
  }

  async saveFile(filename, content) {
    const key = `${this.activePath}/${filename}`;
    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: key,
      Body: JSON.stringify(content, null, 2),
      ContentType: 'application/json'
    });
    await this.client.send(command);
    return { key, filename };
  }
}

module.exports = S3StorageProvider;
