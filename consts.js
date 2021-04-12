export const BN_TO_CB_COLUMNS = {
    '_portfolio':'portfolio',
    '_trade id':'trade id',
    '_size unit':'size unit',
    'Market':'product',
    'Type':'side',
    'Date(UTC)':'created at',
    'Amount':'size',
    'Price':'price',
    'Fee':'fee',
    'Total':'total',
    'Fee Coin':'price/fee/total unit',
};

const CB_TO_BN_COLUMNS = {
  'price/fee/total unit': 'Fee Coin',
  'created at': 'Date(UTC)',
  'product': 'Market',
  'size': 'Amount',
  'price': 'Price',
  'total': 'Total',
  'side': 'Type',
  'fee': 'Fee',
};
