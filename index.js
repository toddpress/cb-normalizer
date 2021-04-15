/**
 * Export Normalizer
 *
 * This tool is used to normalize csv exports from Coinbase into the Binance format for the purposes of tracking your crypto portfolio
 * in a spreadsheet application. Perhaps this tool will become more sophisticated in the future by querying exchange APIs for transaction data, then normalizing that.
 * But for now, we just need something to reduce the manual legwork needed to maintain an up-to-date portfolio.
 */

import { createReadStream, createWriteStream } from 'fs';

import csv from 'csv';
import glob from 'glob';
import { promisify } from 'util';

const globAsync = promisify(glob);

// Data Maps
const BN_KEYS_BY_CB_COLUMN = {
  'created at': 'Date(UTC)',
  product: 'Market',
  side: 'Type',
  price: 'Price',
  size: 'Amount',
  total: 'Total',
  fee: 'Fee',
  'price/fee/total unit': 'Fee Coin',
};

const VALUE_TRANSFORMERS_BY_CB_COLUMN = {
  'created at': (val) => val.replace(/(T|Z)+/g, ' ').trim(),
  product: (val) => val.replace(/\-/g, ''),
  total: (val) => val.replace(/\-/g, ''),
  side: (val) => val.toUpperCase(),
};

const getColumnFromMap = (map) => (column) => map[column];

const getBinanceColumnName = (column) =>
  getColumnFromMap(BN_KEYS_BY_CB_COLUMN)(column);

const getColumnTransformFromMap = (map) => (column) =>
  typeof map[column] === 'function' ? map[column] : (val) => val;

const getTransformerForColumn = (column) =>
  getColumnTransformFromMap(VALUE_TRANSFORMERS_BY_CB_COLUMN)(column);

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

const getFilesFromPattern = async (pattern) => {
  return await globAsync(pattern);
};

const processCoinbaseRecords = async (glob) => {
  const files = await getFilesFromPattern(glob);
  const results = await Promise.all(files.map(processFile));
  return [].concat(...results);
};

const getBinanceColumnsFromMap = (map) => Object.values(map);

const getStringCSVFromObjArray = (array) => {
  return new Promise((resolve, reject) =>
    csv.stringify(
      array,
      {
        header: true,
        columns: getBinanceColumnsFromMap(BN_KEYS_BY_CB_COLUMN),
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
  const objs = await processCoinbaseRecords(glob);
  const output = await getStringCSVFromObjArray(objs);
  const stream = createWriteStream(target, { flags: 'a' });
  stream.write(output, function () {
    console.log('\nwritten to file! %s\n', target);
    target.includes('.csv') || console.log('NOTE: To generate *.csv files, add csv to the end of the second arg.\n');
  });
  stream.end();
})();
