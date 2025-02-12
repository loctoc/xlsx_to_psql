import XLSX from 'xlsx';
import moment from 'moment-timezone';

function sanitizeColumnName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

function getHyperlinkUrl(cell, config) {
  // If isHyperlink is explicitly set to false, return text value
  if (config.isHyperlink === false) {
    return cell.v || null;
  }
  
  // Otherwise, prefer hyperlink if available, fall back to text
  if (cell && cell.l && cell.l.Target) {
    return cell.l.Target;
  }
  return cell.v || null;
}

function transformValue(value, config, timezone) {
  if (value === '' || value === '-' || value === undefined || value === null) return null;

  try {
    switch (config.fieldType?.toLowerCase()) {
      case 'timestamp':
        // Handle Excel date numbers
        if (typeof value === 'number') {
          const excelDate = XLSX.SSF.parse_date_code(value);
          const dateStr = `${excelDate.y}-${String(excelDate.m).padStart(2, '0')}-${String(excelDate.d).padStart(2, '0')} ${String(excelDate.H).padStart(2, '0')}:${String(excelDate.M).padStart(2, '0')}`;
          const m = moment.tz(dateStr, 'YYYY-MM-DD HH:mm', timezone);
          if (!m.isValid()) {
            console.warn(`⚠️ Invalid date format: ${value}`);
            return null;
          }
          return m.toISOString();
        }
        // Handle string dates
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

export async function parseAndTransformXLSX(filePath, tableConfig, timezone, onProgress, sheetName = null) {
  return new Promise((resolve, reject) => {
    try {
      const startTime = Date.now();
      const columns = [];
      const transformedData = [];
      let processedRows = 0;
      let skippedRows = 0;
      let emptyRows = 0;

      console.log(`\n📊 [${new Date().toISOString()}] Reading XLSX file...`);
      
      // Read the workbook with hyperlink parsing enabled
      const workbook = XLSX.readFile(filePath, {
        type: 'file',
        cellDates: true,
        cellNF: false,
        cellText: false,
        cellHTML: true // Enable HTML/hyperlink parsing
      });

      // List available sheets
      console.log('\n📑 Available sheets:', workbook.SheetNames);

      // Get sheet - uses first sheet if sheetName is null
      const targetSheet = sheetName || workbook.SheetNames[0];
      if (!workbook.SheetNames.includes(targetSheet)) {
        throw new Error(`Sheet "${targetSheet}" not found. Available sheets: ${workbook.SheetNames.join(', ')}`);
      }

      console.log(`📋 [${new Date().toISOString()}] Using sheet: ${targetSheet}`);
      const worksheet = workbook.Sheets[targetSheet];

      // Get the range of cells in the worksheet
      const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');
      
      // Map columns based on config array order
      tableConfig.forEach((config, index) => {
        if (!config.skip) {
          columns.push(config.sqlColumn || sanitizeColumnName(config.header));
        }
      });

      console.log(`📋 [${new Date().toISOString()}] Columns found: ${columns.length}`);

      // Process data rows
      for (let row = range.s.r + 1; row <= range.e.r; row++) {
        const rowData = [];
        let hasData = false;

        tableConfig.forEach((config, colIndex) => {
          if (config.skip) {
            return; // Skip this column
          }

          const cellAddress = XLSX.utils.encode_cell({ r: row, c: colIndex });
          const cell = worksheet[cellAddress];
          
          let value = cell ? getHyperlinkUrl(cell, config) : null;
          
          if (value !== null) {
            hasData = true;
          }

          // Transform the value based on field type
          if (config.fieldType === 'timestamp' && value) {
            try {
              const date = moment.tz(value, timezone);
              value = date.isValid() ? date.toDate() : null;
            } catch (error) {
              console.warn(`Invalid date value: ${value}`);
              value = null;
            }
          } else if (config.fieldType === 'number' && value !== null) {
            value = Number(value);
            if (isNaN(value)) {
              value = null;
            }
          }

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
            total: range.e.r - range.s.r,
            processed: processedRows,
            skipped: skippedRows + emptyRows
          });
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`
📊 XLSX Processing Summary:
   Sheet name: ${targetSheet}
   Total rows in file: ${range.e.r - range.s.r}
   Empty rows skipped: ${emptyRows}
   Invalid rows skipped: ${skippedRows}
   Valid rows transformed: ${transformedData.length}
   Processing time: ${duration}s
`);

      if (transformedData.length === 0) {
        console.warn('⚠️ No data rows found in the XLSX file. Creating empty table.');
      } else {
        // Only log sample row if we have data
        console.log('\n📝 Sample transformed row:');
        console.log(columns.reduce((obj, col, i) => {
          obj[col] = transformedData[0][i];
          return obj;
        }, {}));
      }

      resolve({
        columns: tableConfig
          .filter(c => !c.skip)
          .map(c => c.sqlColumn || sanitizeColumnName(c.header)),
        transformedData,
        summary: {
          totalRows: range.e.r - range.s.r,
          processedRows,
          skippedRows,
          emptyRows,
          elapsed: formatElapsed(startTime)
        }
      });

    } catch (error) {
      console.error(`⚠️ [${new Date().toISOString()}] Error processing XLSX:`, {
        message: error.message,
        stack: error.stack,
        file: filePath
      });
      reject(new Error(`Failed to process XLSX: ${error.message}\n${error.stack}`));
    }
  });
}

// Helper function to format elapsed time
function formatElapsed(startTime) {
  const elapsed = Date.now() - startTime;
  if (elapsed < 1000) return `${elapsed}ms`;
  return `${(elapsed / 1000).toFixed(2)}s`;
}