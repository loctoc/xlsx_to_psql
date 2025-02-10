import { describe, it, expect, beforeEach, vi } from 'vitest';
import { parseAndTransformCSV } from './csv.mjs';
import fs from 'fs';
import { parse } from 'csv-parse/sync';

// Mock the fs and csv-parse modules
vi.mock('fs', () => ({
  readFileSync: vi.fn(),
  createReadStream: vi.fn()
}));

vi.mock('csv-parse/sync', () => ({
  parse: vi.fn()
}));

describe('parseAndTransformCSV', () => {
  const mockConfig = [
    {
      header: 'Name',
      sqlColumn: 'name',
      fieldType: 'string'
    },
    {
      header: 'Age',
      sqlColumn: 'age',
      fieldType: 'number'
    },
    {
      header: 'Created At',
      sqlColumn: 'created_at',
      fieldType: 'timestamp'
    }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should handle file read errors', async () => {
    fs.readFileSync.mockImplementationOnce(() => {
      throw new Error('File not found');
    });

    await expect(
      parseAndTransformCSV('nonexistent.csv', mockConfig, 'UTC', vi.fn())
    ).rejects.toThrow('File not found');
  });
}); 