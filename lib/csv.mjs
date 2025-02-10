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
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

function getHyperlinkUrl(value, config) {
  // If isHyperlink is explicitly set to false, return text value
  if (config.isHyperlink === false) {
    return value || null;
  }
  
  // For CSV, we don't have hyperlink metadata, so just return the value
  return value || null;
}

function transformValue(value, config, timezone) {
  if (value === '' || value === '-' || value === undefined || value === null) return null;

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
        return String(value).replace(/\s+/g, ' ').trim();
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
    let processedRows = 0;
    let skippedRows = 0;
    let emptyRows = 0;
    let isFirstRow = true;

    console.log(`\n📊 [${new Date().toISOString()}] Reading CSV file...`);

    // Map columns based on config array order
    tableConfig.forEach((config, index) => {
      if (!config.skip) {
        columns.push(config.sqlColumn || sanitizeColumnName(config.header));
      }
    });

    console.log(`📋 [${new Date().toISOString()}] Columns found: ${columns.length}`);

    const parser = parse({
      bom: true,
      trim: true,
      skip_empty_lines: true
    });

    const stream = fs.createReadStream(filePath)
      .pipe(parser)
      .on('data', (row) => {
        if (isFirstRow) {
          isFirstRow = false;
          return;
        }

        const rowData = [];
        let hasData = false;

        tableConfig.forEach((config, colIndex) => {
          if (config.skip) {
            return; // Skip this column
          }

          let value = getHyperlinkUrl(row[colIndex], config);
          
          if (value !== null) {
            hasData = true;
          }

          value = transformValue(value, config, timezone);
          rowData.push(value);
        });

        if (hasData) {
          transformedData.push(rowData);
          processedRows++;
        } else {
          emptyRows++;
        }

        // Update progress every 1000 rows
        if (processedRows % 1000 === 0) {
          onProgress({
            processed: processedRows,
            skipped: skippedRows + emptyRows
          });
        }
      })
      .on('end', () => {
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`
📊 CSV Processing Summary:
   Total rows processed: ${processedRows + emptyRows + skippedRows}
   Empty rows skipped: ${emptyRows}
   Invalid rows skipped: ${skippedRows}
   Valid rows transformed: ${transformedData.length}
   Processing time: ${duration}s
`);

        if (transformedData.length === 0) {
          reject(new Error('No valid data found in the CSV file'));
          return;
        }

        // Log a sample row
        console.log('\n�� Sample transformed row:');
        console.log(columns.reduce((obj, col, i) => {
          obj[col] = transformedData[0][i];
          return obj;
        }, {}));

        resolve({
          columns: tableConfig
            .filter(c => !c.skip)
            .map(c => c.sqlColumn || sanitizeColumnName(c.header)),
          transformedData,
          summary: {
            totalRows: processedRows + emptyRows + skippedRows,
            processedRows,
            skippedRows,
            emptyRows,
            elapsed: formatElapsed(startTime)
          }
        });
      })
      .on('error', (error) => {
        console.error(`⚠️ [${new Date().toISOString()}] Error processing CSV: ${error.message}`);
        reject(new Error(`Failed to process CSV: ${error.message}`));
      });
  });
} 