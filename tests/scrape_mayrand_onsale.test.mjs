import assert from 'node:assert/strict';
import test from 'node:test';

import { parseUnitPriceText } from '../scripts/scrape_mayrand_onsale.mjs';

test('parseUnitPriceText extracts unit price from unit-price-ref format', () => {
  const result = parseUnitPriceText('1,33$/100g');
  assert.equal(result.unitPrice, 1.33);
  assert.equal(result.unitLabel, '100g');
});
