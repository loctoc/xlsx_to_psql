import { describe, it, expect, beforeEach, vi } from 'vitest';
import { parseXLSX } from '../lib/xlsx.mjs';
import * as xlsx from 'xlsx';

// Mock xlsx module
vi.mock('xlsx', () => ({
  readFile: vi.fn(),
  utils: {
    sheet_to_json: vi.fn()
  }
}));

describe('parseXLSX', () => {
  beforeEach(() => {
    // Clear all mocks before each test
    vi.clearAllMocks();
  });

  it('should successfully parse XLSX file', () => {
    // Mock data
    const mockData = [
      { name: 'John', age: '30', city: 'New York' },
      { name: 'Jane', age: '25', city: 'Boston' }
    ];

    // Mock the XLSX functions
    xlsx.readFile.mockReturnValue({
      SheetNames: ['Sheet1'],
      Sheets: {
        Sheet1: 'dummy-sheet-data'
      }
    });
    xlsx.utils.sheet_to_json.mockReturnValue(mockData);

    // Test the function
    const result = parseXLSX('dummy.xlsx');

    // Verify the result
    expect(result).toEqual({
      columns: ['name', 'age', 'city'],
      rows: mockData
    });

    // Verify that the XLSX functions were called correctly
    expect(xlsx.readFile).toHaveBeenCalledWith('dummy.xlsx');
    expect(xlsx.utils.sheet_to_json).toHaveBeenCalledWith('dummy-sheet-data');
  });

  it('should throw error when file is empty', () => {
    // Mock empty data
    xlsx.readFile.mockReturnValue({
      SheetNames: ['Sheet1'],
      Sheets: {
        Sheet1: 'dummy-sheet-data'
      }
    });
    xlsx.utils.sheet_to_json.mockReturnValue([]);

    // Test that it throws an error
    expect(() => parseXLSX('empty.xlsx')).toThrow('No data found in the XLSX file');
  });

  it('should throw error when file has no sheets', () => {
    // Mock file with no sheets
    xlsx.readFile.mockReturnValue({
      SheetNames: [],
      Sheets: {}
    });

    // Test that it throws an error
    expect(() => parseXLSX('no-sheets.xlsx')).toThrow('No sheets found in the XLSX file');
  });
}); 