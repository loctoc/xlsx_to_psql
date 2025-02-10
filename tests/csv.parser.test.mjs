import fs from 'fs';
import { parse } from 'csv-parse';
// Example CSV file path
const csvFilePath = '/Users/krenangi/Downloads/xlsx-parser-tester-3.csv'

// Configuring csv-parse
const parser = parse({
  columns: false,
  bom: true,
  trim: true,
});
let rowCount = 0;
// Read CSV file and parse
fs.createReadStream(csvFilePath)
  .pipe(parser)
  .on('data', (row) => {
    console.log(row[0])
  })
  .on('end', () => {
    console.log('CSV parsing complete.');
  })
  .on('error', (err) => {
    console.error('Error parsing CSV:', err.message);
  });