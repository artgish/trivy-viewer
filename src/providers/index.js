const S3StorageProvider = require('./s3');
const FilesystemStorageProvider = require('./filesystem');
const AzureStorageProvider = require('./azure');
const GCSStorageProvider = require('./gcs');

/**
 * @typedef {Object} FileInfo
 * @property {string} key - Full path/key to the file
 * @property {string} name - Filename only
 * @property {number} size - File size in bytes
 * @property {Date} lastModified - Last modification date
 */

/**
 * @typedef {Object} ListFilesResult
 * @property {FileInfo[]} files - Array of file info objects
 * @property {string|null} nextContinuationToken - Token for pagination
 * @property {boolean} hasMore - Whether more files exist
 */

/**
 * @typedef {Object} StorageProvider
 * @property {string} activePath - Path to active files
 * @property {string} archivedPath - Path to archived files
 * @property {function(string, number, string=): Promise<ListFilesResult>} listFiles
 * @property {function(string): Promise<Object>} getFile
 * @property {function(string, string): Promise<void>} copyFile
 * @property {function(string): Promise<void>} deleteFile
 */

/**
 * Parse storage location and create appropriate provider
 * @param {string} storageLocation - Storage location URI or path
 * @param {string} storagePrefix - Optional prefix within storage
 * @returns {StorageProvider}
 */
function createStorageProvider(storageLocation, storagePrefix = '') {
  if (!storageLocation) {
    throw new Error('STORAGE_LOCATION environment variable is required');
  }

  // S3: s3://bucket-name
  if (storageLocation.startsWith('s3://')) {
    const bucket = storageLocation.slice(5); // Remove 's3://'
    return new S3StorageProvider(bucket, storagePrefix);
  }

  // Azure: azure://container-name
  if (storageLocation.startsWith('azure://')) {
    const container = storageLocation.slice(8); // Remove 'azure://'
    return new AzureStorageProvider(container, storagePrefix);
  }

  // GCS: gs://bucket-name or gcs://bucket-name
  if (storageLocation.startsWith('gs://')) {
    const bucket = storageLocation.slice(5); // Remove 'gs://'
    return new GCSStorageProvider(bucket, storagePrefix);
  }
  if (storageLocation.startsWith('gcs://')) {
    const bucket = storageLocation.slice(6); // Remove 'gcs://'
    return new GCSStorageProvider(bucket, storagePrefix);
  }

  // Local filesystem (default)
  return new FilesystemStorageProvider(storageLocation, storagePrefix);
}

/**
 * Get provider type name from storage location
 * @param {string} storageLocation - Storage location URI or path
 * @returns {string} Provider type name
 */
function getProviderType(storageLocation) {
  if (!storageLocation) return 'unknown';
  if (storageLocation.startsWith('s3://')) return 'S3';
  if (storageLocation.startsWith('azure://')) return 'Azure Blob';
  if (storageLocation.startsWith('gs://') || storageLocation.startsWith('gcs://')) return 'GCS';
  return 'Filesystem';
}

module.exports = {
  createStorageProvider,
  getProviderType
};
