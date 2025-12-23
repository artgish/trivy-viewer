const { Storage } = require('@google-cloud/storage');
const path = require('path');

class GCSStorageProvider {
  constructor(bucket, prefix = '') {
    this.bucketName = bucket;
    this.prefix = prefix;

    // Uses Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS env var
    this.storage = new Storage();
    this.bucket = this.storage.bucket(bucket);

    this.activePath = prefix ? `${prefix}/active` : 'active';
    this.archivedPath = prefix ? `${prefix}/archived` : 'archived';
  }

  async listFiles(subPath, limit = 100, continuationToken = undefined) {
    const prefix = subPath.endsWith('/') ? subPath : `${subPath}/`;

    const options = {
      prefix: prefix,
      delimiter: '/',
      maxResults: limit,
      autoPaginate: false
    };

    if (continuationToken) {
      options.pageToken = continuationToken;
    }

    const [files, nextQuery, apiResponse] = await this.bucket.getFiles(options);

    // Extract directories from prefixes
    const directories = (apiResponse.prefixes || []).map(dirPath => {
      const name = dirPath.slice(0, -1).split('/').pop();
      return {
        key: dirPath,
        name: name,
        type: 'directory'
      };
    });

    // Extract JSON files
    const jsonFiles = files
      .filter(file => file.name.endsWith('.json') && file.name !== prefix)
      .map(file => ({
        key: file.name,
        name: path.basename(file.name),
        size: parseInt(file.metadata.size, 10),
        lastModified: new Date(file.metadata.updated),
        type: 'file'
      }));

    // Combine directories first, then files
    const items = [...directories, ...jsonFiles];

    return {
      files: items,
      nextContinuationToken: nextQuery?.pageToken || null,
      hasMore: !!nextQuery?.pageToken
    };
  }

  async getFile(key) {
    const file = this.bucket.file(key);
    const [content] = await file.download();
    return JSON.parse(content.toString('utf-8'));
  }

  async copyFile(sourceKey, destKey) {
    const sourceFile = this.bucket.file(sourceKey);
    const destFile = this.bucket.file(destKey);
    await sourceFile.copy(destFile);
  }

  async deleteFile(key) {
    const file = this.bucket.file(key);
    await file.delete();
  }

  async saveFile(filename, content) {
    const key = `${this.activePath}/${filename}`;
    const file = this.bucket.file(key);
    await file.save(JSON.stringify(content, null, 2), {
      contentType: 'application/json'
    });
    return { key, filename };
  }
}

module.exports = GCSStorageProvider;
