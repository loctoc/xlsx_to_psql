import { describe, it, expect, beforeEach, vi } from 'vitest';
import { parseCSV } from './csv.mjs';
import fs from 'fs';
import { parse } from 'csv-parse/sync';

// Mock the fs and csv-parse modules
vi.mock('fs', () => ({
  readFileSync: vi.fn()
}));

vi.mock('csv-parse/sync', () => ({
  parse: vi.fn()
}));

describe('parseCSV', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should successfully parse CSV file', () => {
    // Mock CSV content
    const csvContent = 'name,age,city\nJohn,30,New York\nJane,25,Boston';
    const mockData = [
      { name: 'John', age: '30', city: 'New York' },
      { name: 'Jane', age: '25', city: 'Boston' }
    ];

    // Setup mocks
    fs.readFileSync.mockReturnValue(csvContent);
    parse.mockReturnValue(mockData);

    // Test the function
    const result = parseCSV('test.csv');

    // Verify the result
    expect(result).toEqual({
      columns: ['name', 'age', 'city'],
      rows: mockData
    });

    // Verify that the functions were called correctly
    expect(fs.readFileSync).toHaveBeenCalledWith('test.csv', 'utf-8');
    expect(parse).toHaveBeenCalledWith(csvContent, {
      columns: true,
      skip_empty_lines: true
    });
  });

  it('should throw error when file is empty', () => {
    // Mock empty CSV
    fs.readFileSync.mockReturnValue('header1,header2\n');
    parse.mockReturnValue([]);

    // Test that it throws an error
    expect(() => parseCSV('empty.csv')).toThrow('No data found in the CSV file');
  });

  it('should throw error when file cannot be read', () => {
    // Mock file read error
    fs.readFileSync.mockImplementation(() => {
      throw new Error('File not found');
    });

    // Test that it throws an error
    expect(() => parseCSV('nonexistent.csv')).toThrow('File not found');
  });
}); 