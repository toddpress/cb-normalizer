import { createReadStream, createWriteStream } from 'fs';

import csv from 'csv';
import glob from 'glob';
import { promisify } from 'util';

const globAsync = promisify(glob);

// Data Maps
const UNIFIED_KEYS_BY_COLUMN = {
  'created at': 'Date(UTC)',
  product: 'Market',
  side: 'Type',
  price: 'Price',
  size: 'Amount',
  total: 'Total',
  fee: 'Fee',
  'price/fee/total unit': 'Fee Coin',
};

const VALUE_TRANSFORMERS_BY_ORIG_COLUMN = {
  'created at': (val) => val.replace(/(T|Z)+/g, ' ').trim(),
  product: (val) => val.replace(/\-/g, ''),
  total: (val) => val.replace(/\-/g, ''),
  side: (val) => val.toUpperCase(),
};

export const getFilesFromPattern = async (pattern) => {
  return await globAsync(pattern);
};

const getBinanceColumnsFromMap = (map) => Object.values(map);

const getColumnFromMap = (map) => (column) => map[column];

const getBinanceColumnName = (column) =>
  getColumnFromMap(UNIFIED_KEYS_BY_COLUMN)(column);

const getColumnTransformFromMap = (map) => (column) =>
  typeof map[column] === 'function' ? map[column] : (val) => val;

const getTransformerForColumn = (column) =>
  getColumnTransformFromMap(VALUE_TRANSFORMERS_BY_ORIG_COLUMN)(column);

const transformCoinbaseEntry = ([k, v], i) => {
  const unifiedColumn = getBinanceColumnName(k);
  const transformedValue = getTransformerForColumn(k)(v);
  return [unifiedColumn, transformedValue];
};

const transformResults = (obj, mappingFn) =>
  Object.fromEntries(
    Object.entries(obj)
      .map(mappingFn)
      .filter(([k, v]) => k !== undefined)
  );

const processFile = async (url) => {
  let output = [];
  const parser = createReadStream(url).pipe(
    csv.parse({
      columns: true,
      skip_empty_lines: true,
      parallel: true,
    })
  );

  for await (const row of parser) {
    const transformed = transformResults(row, transformCoinbaseEntry);
    output.push(transformed);
  }

  return output;
};

const processCoinbaseRecords = async (glob) => {
  const files = await getFilesFromPattern(glob);
  // get promise array that resolves to obj arr from csv
  const results = await Promise.all(files.map(processFile));
  // flatten results into a single array
  return [].concat(...results);
};

const getStringCSVFromObjArray = (array) => {
  return new Promise((resolve, reject) =>
    csv.stringify(
      array,
      {
        header: true,
        columns: getBinanceColumnsFromMap(UNIFIED_KEYS_BY_COLUMN),
      },
      (err, data) => {
        if (err) reject(err);
        resolve(data);
      }
    )
  );
};

(async () => {
  const glob = process.argv[2];
  const target = process.argv[3] || 'transactions.csv';
  //1.1. get all CSVs for CB exports
  //1.2. iterate through files
  //1.3  transform coinbase CSVs into Binance obj arrays.
  const objs = await processCoinbaseRecords(glob);
  const output = await getStringCSVFromObjArray(objs);
  const stream = createWriteStream(target, { flags: 'a' });
  stream.write(output, function () {
    console.log('written to file! %s', target);
  });
  stream.end();
})();
