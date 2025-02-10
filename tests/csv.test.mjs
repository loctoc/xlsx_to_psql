import { describe, it, expect } from 'vitest';
import { parseCSV } from '../lib/csv.mjs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('parseCSV', () => {
    const testFilePath = path.join(__dirname, 'table-data-parser-tester.csv');

    it('should successfully parse CSV file', () => {
        const result = parseCSV(testFilePath);

        // Verify the structure
        expect(result).toHaveProperty('columns');
        expect(result).toHaveProperty('rows');

        // Verify columns match the test file with sanitized names
        const expectedColumns = [
            'form_name',
            'submission_number',
            'sent_at',
            'sender_name',
            'sender_identifier',
            'sender_department',
            'sender_designation',
            'sender_location',
            'sender_division',
            'sender_sub_division',
            'current_status',
            'remarks',
            'name',
            'stage_1_date',
            'stage_1_name',
            'stage_1_action',
            'stage_1_location',
            'location',
            'please_select_the_zone',
            'ambient_temperature_recorded',
            'evidence',
            'chiller_temperature_recorded',
            'evidence_2',
            'old_meat_chiller_temperature_recorded',
            'evidence_3',
            'freezer_temperature_recorded',
            'evidence_4',
            'milk_chiller_temperature',
            'evidence_5',
            'new_meat_chiller_temperature_recorded',
            'evidence_6'
        ];
        expect(result.columns).toEqual(expectedColumns);

        // Verify we have the correct number of rows
        expect(result.rows).toHaveLength(2);

        // Create expected date objects
        const sentAt1 = new Date('2025-02-05T19:20:00');
        const sentAt2 = new Date('2025-02-05T19:21:00');

        // Verify specific data points from the first row
        expect(result.rows[0]).toMatchObject({
            'form_name': 'QA Temperature Check at DH',
            'submission_number': 'QATC-2672',
            'sent_at': sentAt1,
            'sender_name': 'Anwar .',
            'sender_identifier': '8851773847',
            'location': 'DEL_MLVNGR_P01R1CC',
            'please_select_the_zone': 'Ambient',
            'ambient_temperature_recorded': 'Below 14',
            'evidence': 'Click to view picture',
            'chiller_temperature_recorded': null,
            'evidence_2': null
        });

        // Verify specific data points from the second row
        expect(result.rows[1]).toMatchObject({
            'form_name': 'QA Temperature Check at DH',
            'submission_number': 'QATC-2673',
            'sent_at': sentAt2,
            'please_select_the_zone': 'Chiller',
            'ambient_temperature_recorded': null,
            'evidence': null,
            'chiller_temperature_recorded': '6',
            'evidence_2': 'Click to view picture'
        });
    });

    it('should throw error when file cannot be read', () => {
        expect(() => parseCSV('nonexistent.csv')).toThrow();
    });
}); 