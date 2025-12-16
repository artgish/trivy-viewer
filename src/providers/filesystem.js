const fs = require('fs').promises;
const path = require('path');

class FilesystemStorageProvider {
  constructor(basePath, prefix = '') {
    this.basePath = path.resolve(basePath);
    this.prefix = prefix;
    const prefixPath = prefix ? path.join(this.basePath, prefix) : this.basePath;
    this.activePath = path.join(prefixPath, 'active');
    this.archivedPath = path.join(prefixPath, 'archived');
  }

  async listFiles(subPath, limit = 1000, continuationToken = undefined) {
    const dirPath = path.resolve(subPath);

    try {
      await fs.access(dirPath);
    } catch {
      return {
        files: [],
        nextContinuationToken: null,
        hasMore: false
      };
    }

    const entries = await fs.readdir(dirPath, { withFileTypes: true });

    let allFiles = [];
    for (const entry of entries) {
      if (entry.isFile() && entry.name.endsWith('.json')) {
        const filePath = path.join(dirPath, entry.name);
        const stats = await fs.stat(filePath);
        allFiles.push({
          key: filePath,
          name: entry.name,
          size: stats.size,
          lastModified: stats.mtime
        });
      }
    }

    // Sort by lastModified descending
    allFiles.sort((a, b) => b.lastModified - a.lastModified);

    // Handle pagination using offset-based continuation token
    const startIndex = continuationToken ? parseInt(continuationToken, 10) : 0;
    const endIndex = startIndex + limit;
    const paginatedFiles = allFiles.slice(startIndex, endIndex);
    const hasMore = endIndex < allFiles.length;

    return {
      files: paginatedFiles,
      nextContinuationToken: hasMore ? String(endIndex) : null,
      hasMore
    };
  }

  async getFile(key) {
    const filePath = path.join(this.basePath, key);
    const content = await fs.readFile(filePath, 'utf-8');
    return JSON.parse(content);
  }

  async copyFile(sourceKey, destKey) {
    const sourcePath = path.resolve(sourceKey);
    const destPath = path.resolve(destKey);

    // Ensure destination directory exists
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    await fs.copyFile(sourcePath, destPath);
  }

  async deleteFile(key) {
    const filePath = path.resolve(key);
    await fs.unlink(filePath);
  }

  async saveFile(filename, content) {
    const filePath = path.join(this.activePath, filename);

    // Ensure active directory exists
    await fs.mkdir(this.activePath, { recursive: true });

    // Write file
    await fs.writeFile(filePath, JSON.stringify(content, null, 2), 'utf-8');

    return { key: filePath, filename };
  }
}

module.exports = FilesystemStorageProvider;
