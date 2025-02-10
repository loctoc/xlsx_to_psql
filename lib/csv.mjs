import fs from 'fs';
import { parse } from 'csv-parse';
import moment from 'moment-timezone';

// Helper function to format elapsed time
function formatElapsed(startTime) {
  const elapsed = Date.now() - startTime;
  if (elapsed < 1000) return `${elapsed}ms`;
  return `${(elapsed / 1000).toFixed(2)}s`;
}

function sanitizeColumnName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_') // Replace spaces and special chars with underscore
    .replace(/^_+|_+$/g, '')     // Remove leading/trailing underscores
    .replace(/_+/g, '_');        // Replace multiple underscores with single
}

function transformValue(value, config, timezone) {
  if (value === '' || value === '-') return null;

  try {
    switch (config.fieldType?.toLowerCase()) {
      case 'timestamp':
        const m = moment.tz(value, 'YYYY-MM-DD HH:mm', timezone);
        if (!m.isValid()) {
          console.warn(`⚠️ Invalid date format: ${value}`);
          return null;
        }
        return m.toISOString();

      case 'number':
        const num = Number(value);
        return isNaN(num) ? null : num;

      case 'string':
      default:
        return value;
    }
  } catch (error) {
    console.warn(`⚠️ Error transforming value: ${value}`, error.message);
    return null;
  }
}

export async function parseAndTransformCSV(filePath, tableConfig, timezone, onProgress) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    const columns = [];
    const transformedData = [];
    let isFirstRow = true;
    let totalRows = 0;
    let skippedRows = 0;
    let emptyRows = 0;
    let headerIndexMap = new Map();

    // Count total lines first (excluding header)
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const roughLineCount = fileContent.split('\n').length - 1;
    console.log(`\n📊 [${new Date().toISOString()}] Approximate lines in CSV: ${roughLineCount}`);

    let processedRows = 0;
    let lastLogTime = Date.now();
    const logInterval = 5000;

    fs.createReadStream(filePath)
      .pipe(parse({
        columns: false,
        skip_empty_lines: true,
        bom: true,
        trim: true,
        skipRecordsWithError: true,
        quote: '"',           // Use double quotes for field enclosure
        escape: '"',          // Use double quotes for escaping
        relax_quotes: true,   // Allow quotes in unquoted field
        relax_column_count: true, // Allow rows to have different column counts
        relax: true,         // Be more lenient with field parsing
        ltrim: true,         // Trim left spaces
        rtrim: true,         // Trim right spaces
        skip_empty_lines: true, // Skip empty lines
        skip_records_with_empty_values: false // Don't skip records with empty values
      }))
      .on('data', (row) => {
        totalRows++;
        
        if (isFirstRow) {
          isFirstRow = false;
          // Create mapping of header names to their indices
          row.forEach((header, index) => {
            // Clean up header - replace newlines and extra spaces
            const cleanHeader = header.replace(/\s+/g, ' ').trim();
            headerIndexMap.set(cleanHeader, index);
          });

          // Process column headers using config
          tableConfig.forEach(config => {
            if (headerIndexMap.has(config.header)) {
              columns.push(config.sqlColumn || sanitizeColumnName(config.header));
            } else {
              console.log(`⚠️ [${new Date().toISOString()}] Skipping missing column: ${config.header}`);
            }
          });

          console.log(`📋 [${new Date().toISOString()}] Columns found: ${columns.length}`);
          return;
        }

        // Check if row is empty
        if (row.every(val => !val || String(val).trim() === '')) {
          emptyRows++;
          return;
        }

        // Transform row values using config and header indices
        const transformedRow = columns.map(colName => {
          const config = tableConfig.find(c =>
            (c.sqlColumn || sanitizeColumnName(c.header)) === colName
          );
          if (!config) return null;

          const index = headerIndexMap.get(config.header);
          let value = row[index];
          
          // Clean up value - replace multiple newlines/spaces with single space
          if (typeof value === 'string') {
            value = value.replace(/\s+/g, ' ').trim();
          }
          
          return transformValue(value, config, timezone);
        });

        // Skip row if all values are null
        if (transformedRow.every(val => val === null)) {
          skippedRows++;
          return;
        }

        transformedData.push(transformedRow);

        // Update progress
        processedRows++;
        const progress = (processedRows / roughLineCount) * 100;
        onProgress(progress);

        // Log progress periodically
        const now = Date.now();
        if (now - lastLogTime > logInterval) {
          const rowsPerSecond = Math.round((processedRows / (now - startTime)) * 1000);
          console.log(`📈 [${new Date().toISOString()}] Processing speed: ${rowsPerSecond} rows/sec`);
          lastLogTime = now;
        }
      })
      .on('error', (error) => {
        console.error(`⚠️ [${new Date().toISOString()}] Error processing row: ${error.message}`);
        skippedRows++;
      })
      .on('skip', (error) => {
        console.warn(`⚠️ [${new Date().toISOString()}] Skipped row: ${error.message}`);
        skippedRows++;
      })
      .on('end', () => {
        if (transformedData.length === 0) {
          reject(new Error('No valid data found in the CSV file'));
        } else {
          const duration = ((Date.now() - startTime) / 1000).toFixed(2);
          console.log(`
📊 CSV Processing Summary:
   Total lines in file: ${roughLineCount}
   Total rows processed: ${totalRows}
   Empty rows skipped: ${emptyRows}
   Invalid rows skipped: ${skippedRows}
   Valid rows transformed: ${transformedData.length}
   Processing time: ${duration}s
`);

          // Log a sample row
          if (transformedData.length > 0) {
            console.log('\n📝 Sample transformed row:');
            console.log(columns.reduce((obj, col, i) => {
              obj[col] = transformedData[0][i];
              return obj;
            }, {}));
          }

          resolve({ columns, transformedData });
        }
      })
      .on('error', reject);
  });
} 