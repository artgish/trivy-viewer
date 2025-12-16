const { BlobServiceClient } = require('@azure/storage-blob');
const { DefaultAzureCredential } = require('@azure/identity');
const path = require('path');

class AzureStorageProvider {
  constructor(blobLocation, prefix = '') {
    const splited = blobLocation.split("/");
    this.containerName = splited[1];
    this.prefix = prefix;
    this.account_url = `https://${splited[0]}.blob.core.windows.net`;
    this.creds = new DefaultAzureCredential();
    this.blobServiceClient = new BlobServiceClient(this.account_url, this.creds);
    this.containerClient = this.blobServiceClient.getContainerClient(this.containerName);

    this.activePath = prefix ? `${prefix}/active` : 'active';
    this.archivedPath = prefix ? `${prefix}/archived` : 'archived';
  }

  async listFiles(subPath, limit = 1000, continuationToken = undefined) {
    const files = [];
    let hasMore = false;
    let nextToken = null;

    const options = {
      prefix: subPath.endsWith('/') ? subPath : `${subPath}/`
    };

    const iterator = this.containerClient.listBlobsFlat(options).byPage({
      maxPageSize: limit,
      continuationToken: continuationToken || undefined
    });

    const response = await iterator.next();

    if (!response.done && response.value.segment) {
      for (const blob of response.value.segment.blobItems) {
        if (blob.name.endsWith('.json')) {
          files.push({
            key: blob.name,
            name: path.basename(blob.name),
            size: blob.properties.contentLength,
            lastModified: blob.properties.lastModified
          });
        }
      }
      nextToken = response.value.continuationToken || null;
      hasMore = !!nextToken;
    }

    return {
      files,
      nextContinuationToken: nextToken,
      hasMore
    };
  }

  async getFile(key) {
    const blobClient = this.containerClient.getBlobClient(key);
    const downloadResponse = await blobClient.download();
    const content = await this._streamToString(downloadResponse.readableStreamBody);
    return JSON.parse(content);
  }

  async copyFile(sourceKey, destKey) {
    const sourceBlobClient = this.containerClient.getBlobClient(sourceKey);
    const destBlobClient = this.containerClient.getBlobClient(destKey);

    const copyPoller = await destBlobClient.beginCopyFromURL(sourceBlobClient.url);
    await copyPoller.pollUntilDone();
  }

  async deleteFile(key) {
    const blobClient = this.containerClient.getBlobClient(key);
    await blobClient.delete();
  }

  async _streamToString(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on('data', (data) => {
        chunks.push(data.toString());
      });
      readableStream.on('end', () => {
        resolve(chunks.join(''));
      });
      readableStream.on('error', reject);
    });
  }

  async saveFile(filename, content) {
    const key = `${this.activePath}/${filename}`;
    const blockBlobClient = this.containerClient.getBlockBlobClient(key);
    const data = JSON.stringify(content, null, 2);
    await blockBlobClient.upload(data, data.length, {
      blobHTTPHeaders: { blobContentType: 'application/json' }
    });
    return { key, filename };
  }
}

module.exports = AzureStorageProvider;
