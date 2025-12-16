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

  async listFiles(subPath, limit = 1000, continuationToken = undefined) {
    const command = new ListObjectsV2Command({
      Bucket: this.bucket,
      Prefix: subPath,
      MaxKeys: limit,
      ContinuationToken: continuationToken || undefined
    });

    const response = await this.client.send(command);

    const files = (response.Contents || [])
      .filter(obj => obj.Key.endsWith('.json'))
      .map(obj => ({
        key: obj.Key,
        name: path.basename(obj.Key),
        size: obj.Size,
        lastModified: obj.LastModified
      }));

    return {
      files,
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
