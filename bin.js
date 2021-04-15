const cbNormalizer = require('./index.js');

const glob = process.argv[2];
const target = process.argv[3] || 'transactions.csv';


cbNormalizer(glob, target);