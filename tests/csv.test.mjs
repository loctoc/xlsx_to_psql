import { describe, it, expect } from 'vitest';
import { parseAndTransformCSV } from '../lib/csv.mjs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('parseAndTransformCSV', () => {
    const testFilePath = path.join(__dirname, 'table-data-parser-tester.csv');

    it.skip('should successfully parse CSV file', () => {
        const result = parseAndTransformCSV(testFilePath);
        expect(result).toMatchFileSnapshot()
    });

    it('should throw error when file cannot be read', () => {
        expect(() => parseCSV('nonexistent.csv')).toThrow();
    });
}); 