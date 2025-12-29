import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const BASE_URL = 'https://mayrand.ca/fr/page-recherche?search=onsale';
const OUTPUT_DIR = path.join('public', 'mayrand', 'onsale');
const DEBUG_DIR = path.join('outputs', 'debug');
const QUERY = 'onsale';
const SOURCE = 'mayrand';
const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const PAGE_TIMEOUT_MS = 45000;
const PAGE_RETRY_COUNT = 2;
const PAGE_MAX_LIMIT = 100;
const RESULTS_WAIT_TIMEOUT_MS = 20000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeWhitespace = (value) =>
  value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? null;

const parseNumber = (value) => {
  if (!value) return null;
  const cleaned = value
    .replace(/\s+/g, '')
    .replace(/\$/g, '')
    .replace(/,/g, '.')
    .replace(/[^0-9.]/g, '');
  if (!cleaned) return null;
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parsePriceCandidates = (values) => {
  const prices = values
    .map((entry) => parseNumber(entry))
    .filter((entry) => entry !== null);
  if (prices.length === 0) return { sale: null, regular: null };
  const sorted = [...prices].sort((a, b) => a - b);
  const sale = sorted[0] ?? null;
  const regular = sorted.length > 1 ? sorted[sorted.length - 1] : null;
  return { sale, regular };
};

const resolveUrl = (maybeUrl) => {
  if (!maybeUrl) return null;
  try {
    return new URL(maybeUrl, BASE_URL).toString();
  } catch {
    return null;
  }
};

const buildFallbackUrl = (sku, name) => {
  const anchor = sku || name;
  if (!anchor) return BASE_URL;
  return `${BASE_URL}#${encodeURIComponent(anchor.replace(/\s+/g, '-').slice(0, 80))}`;
};

const uniqueKeyForItem = (item) =>
  item.sku || item.url || `${item.name ?? ''}-${item.unit_label ?? ''}`.trim();

const ensureDirs = async () => {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.mkdir(DEBUG_DIR, { recursive: true });
};

const writeJson = async (targetPath, data) => {
  await fs.writeFile(targetPath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
};

const writeCsv = async (targetPath, items) => {
  const headers = [
    'source',
    'query',
    'name',
    'brand',
    'sku',
    'price_sale',
    'price_regular',
    'unit_label',
    'unit_price',
    'url',
    'image',
    'category',
    'scraped_at',
  ];
  const lines = [headers.join(',')];
  const sanitize = (value) => {
    if (value === null || value === undefined) return '';
    const stringValue = String(value);
    if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
  };

  for (const item of items) {
    const row = headers.map((header) => sanitize(item[header] ?? ''));
    lines.push(row.join(','));
  }
  await fs.writeFile(targetPath, `${lines.join('\n')}\n`, 'utf8');
};

const acceptCookies = async (page) => {
  const consentSelectors = [
    '#onetrust-accept-btn-handler',
    'button#onetrust-accept-btn-handler',
    'button[aria-label*="Accept"]',
    'button[aria-label*="Accepter"]',
    'button:has-text("Tout accepter")',
    'button:has-text("Accepter")',
    'button:has-text("Accept")',
    'button:has-text("Agree")',
  ];

  for (const selector of consentSelectors) {
    const locator = page.locator(selector).first();
    try {
      if (await locator.isVisible({ timeout: 2000 })) {
        await locator.click({ timeout: 2000 });
        await page.waitForTimeout(500);
        break;
      }
    } catch {
      continue;
    }
  }
};

const waitForLoaders = async (page) => {
  const loaderSelectors = [
    '.loading',
    '.loader',
    '.spinner',
    '.is-loading',
    '[aria-busy="true"]',
    '[data-loading="true"]',
  ];
  for (const selector of loaderSelectors) {
    try {
      await page.locator(selector).first().waitFor({ state: 'hidden', timeout: 5000 });
    } catch {
      continue;
    }
  }
};

const waitForResults = async (page) => {
  await page.waitForFunction(
    () => {
      const containerSelectors = [
        '.product-list',
        '.product-listing',
        '.search-results',
        '.products',
        '.product-grid',
        '.listing',
      ];
      const cardSelectors = [
        '[data-product-id]',
        '[data-sku]',
        '.product-tile',
        '.product-item',
        '.product',
        '.product-container',
        '.productBox',
      ];
      const container = containerSelectors
        .map((selector) => document.querySelector(selector))
        .find(Boolean);
      const scope = container ?? document;
      const cards = scope.querySelectorAll(cardSelectors.join(','));
      return Boolean(container) || cards.length > 0;
    },
    { timeout: RESULTS_WAIT_TIMEOUT_MS }
  );
};

const scrapePage = async (page) => {
  return page.evaluate(() => {
    const normalizeWhitespace = (value) =>
      value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? null;

    const isVisible = (element) => {
      if (!element) return false;
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return (
        rect.width > 0 &&
        rect.height > 0 &&
        style.visibility !== 'hidden' &&
        style.display !== 'none' &&
        Number.parseFloat(style.opacity || '1') > 0
      );
    };

    const cardSelectors = [
      '[data-product-id]',
      '[data-sku]',
      '.product-tile',
      '.product-item',
      '.product',
      '.product-container',
      '.productBox',
    ];
    const cardSet = new Set();
    cardSelectors.forEach((selector) => {
      document.querySelectorAll(selector).forEach((element) => cardSet.add(element));
    });
    const cards = Array.from(cardSet).filter((element) => isVisible(element));

    const containerSelectors = [
      '.product-list',
      '.product-listing',
      '.search-results',
      '.products',
      '.product-grid',
      '.listing',
    ];
    const resultsContainer = containerSelectors
      .map((selector) => document.querySelector(selector))
      .find(Boolean);

    const breadcrumb = normalizeWhitespace(
      Array.from(document.querySelectorAll('nav.breadcrumb, .breadcrumb, .breadcrumbs'))
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const results = cards.map((card) => {
      const text = normalizeWhitespace(card.innerText || card.textContent || '');
      const name = normalizeWhitespace(
        card.querySelector('.product-title, .product-name, .title, h2, h3, a[title]')
          ?.textContent
      );

      const brand = normalizeWhitespace(
        card.querySelector('.product-brand, .brand, .manufacturer')?.textContent
      );

      const skuMatch = text?.match(/(?:code|sku|produit|item|article)\s*:?\s*([0-9]{3,})/i);
      const skuFallback = text?.match(/\b([0-9]{3,})\b/);
      const sku = skuMatch?.[1] || skuFallback?.[1] || null;

      const priceNodes = card.querySelectorAll(
        '.price--sale, .price-sale, .special-price, .sale-price, .product-price, .price, .pricing, .value'
      );
      const priceTexts = Array.from(priceNodes)
        .map((node) => node.textContent)
        .filter(Boolean);

      const regularNodes = card.querySelectorAll('del, s, .price--regular, .regular-price');
      const regularTexts = Array.from(regularNodes)
        .map((node) => node.textContent)
        .filter(Boolean);

      const unitLabel = normalizeWhitespace(
        card.querySelector('.unit, .unit-label, .product-unit, .unitLabel')?.textContent
      );
      const unitPrice = normalizeWhitespace(
        card.querySelector('.unit-price, .price-per-unit, .unitPrice')?.textContent
      );

      const link = card.querySelector('a[href]')?.getAttribute('href') || null;
      const image =
        card.querySelector('img')?.getAttribute('src') ||
        card.querySelector('img')?.getAttribute('data-src') ||
        card.querySelector('img')?.getAttribute('data-lazy') ||
        null;

      const category =
        normalizeWhitespace(card.getAttribute('data-category')) ||
        normalizeWhitespace(card.querySelector('.category, .product-category')?.textContent) ||
        null;

      return {
        name,
        brand,
        sku,
        priceTexts,
        regularTexts,
        unitLabel,
        unitPrice,
        link,
        image,
        category,
        breadcrumb,
      };
    });

    const paginationLinks = Array.from(
      document.querySelectorAll(
        '.pagination a, nav.pagination a, .pager a, .pagination-link, .pagination__link'
      )
    ).map((link) => link.getAttribute('href'));

    const nextHref =
      document.querySelector('a[rel="next"], .pagination-next a, .pager-next a')?.getAttribute(
        'href'
      ) || null;

    const emptyStateText = normalizeWhitespace(
      Array.from(
        document.querySelectorAll(
          '.no-results, .empty, .results-empty, .search-empty, [data-testid="no-results"]'
        )
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const resultsCountText = normalizeWhitespace(
      Array.from(
        document.querySelectorAll('.results-count, .search-result-count, .product-count, .count')
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    return {
      results,
      paginationLinks,
      nextHref,
      breadcrumb,
      emptyStateText,
      resultsCountText,
      containerSelector: resultsContainer ? resultsContainer.className || resultsContainer.id : null,
      visibleCardCount: cards.length,
    };
  });
};

const getPaginationInfo = (paginationLinks) => {
  let maxPage = null;
  paginationLinks.forEach((href) => {
    if (!href) return;
    try {
      const url = new URL(href, BASE_URL);
      const pageParam = url.searchParams.get('page');
      const pageNumber = pageParam ? Number.parseInt(pageParam, 10) : null;
      if (Number.isFinite(pageNumber)) {
        maxPage = maxPage ? Math.max(maxPage, pageNumber) : pageNumber;
      }
    } catch {
      return;
    }
  });
  return maxPage;
};

const absoluteUrl = (href) => resolveUrl(href);

const main = async () => {
  await ensureDirs();

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: USER_AGENT,
    locale: 'fr-CA',
    viewport: { width: 1365, height: 768 },
  });
  const page = await context.newPage();
  page.setDefaultTimeout(PAGE_TIMEOUT_MS);

  const allItems = [];
  let currentPage = 1;
  let maxPage = null;
  let nextHref = null;

  while (currentPage <= PAGE_MAX_LIMIT) {
    const pageUrl = (() => {
      if (currentPage === 1) return BASE_URL;
      if (nextHref) return absoluteUrl(nextHref) || BASE_URL;
      const url = new URL(BASE_URL);
      url.searchParams.set('page', String(currentPage));
      return url.toString();
    })();

    let extracted = null;
    let lastError = null;

    for (let attempt = 0; attempt <= PAGE_RETRY_COUNT; attempt += 1) {
      try {
        await page.goto(pageUrl, { waitUntil: 'networkidle' });
        await acceptCookies(page);
        await waitForLoaders(page);
        await waitForResults(page);
        extracted = await scrapePage(page);
        if (extracted.results.length > 0 || attempt === PAGE_RETRY_COUNT) {
          break;
        }
        await sleep(1000);
      } catch (error) {
        lastError = error;
        await sleep(1000);
      }
    }

    if (!extracted) {
      const errorMessage = lastError ? lastError.message : 'Unknown error';
      const debugStamp = new Date().toISOString().replace(/[:.]/g, '-');
      await fs.writeFile(
        path.join(DEBUG_DIR, `mayrand-onsale-page-${currentPage}-${debugStamp}.error.txt`),
        `Failed to scrape page ${currentPage}: ${errorMessage}\n`,
        'utf8'
      );
      break;
    }

    if (extracted.results.length === 0) {
      const debugStamp = new Date().toISOString().replace(/[:.]/g, '-');
      const htmlPath = path.join(DEBUG_DIR, `mayrand-onsale-page-${currentPage}-${debugStamp}.html`);
      await fs.writeFile(htmlPath, await page.content(), 'utf8');
      await page.screenshot({
        path: path.join(DEBUG_DIR, `mayrand-onsale-page-${currentPage}-${debugStamp}.png`),
        fullPage: true,
      });
      const emptyDebug = {
        page: currentPage,
        url: pageUrl,
        timestamp: new Date().toISOString(),
        empty_state_text: extracted.emptyStateText,
        results_count_text: extracted.resultsCountText,
        container_selector: extracted.containerSelector,
        visible_card_count: extracted.visibleCardCount,
      };
      await fs.writeFile(
        path.join(DEBUG_DIR, `mayrand-onsale-page-${currentPage}-${debugStamp}.empty.json`),
        `${JSON.stringify(emptyDebug, null, 2)}\n`,
        'utf8'
      );
      console.log(`Mayrand onsale page ${currentPage}: 0 items`, emptyDebug);
    }

    const pageCategory = extracted.breadcrumb || null;

    const pageItems = extracted.results.map((item) => {
      const { sale, regular } = parsePriceCandidates([
        ...item.priceTexts,
        ...item.regularTexts,
      ]);
      const unitPrice = parseNumber(item.unitPrice);
      const resolvedLink = resolveUrl(item.link);
      const resolvedImage = resolveUrl(item.image);
      return {
        source: SOURCE,
        query: QUERY,
        name: item.name,
        brand: item.brand,
        sku: item.sku,
        price_sale: sale,
        price_regular: item.regularTexts.length > 0 ? regular : null,
        unit_label: item.unitLabel,
        unit_price: unitPrice,
        url: resolvedLink || buildFallbackUrl(item.sku, item.name),
        image: resolvedImage,
        category: item.category || pageCategory,
        scraped_at: new Date().toISOString(),
      };
    });

    allItems.push(...pageItems);

    if (maxPage === null) {
      maxPage = getPaginationInfo(extracted.paginationLinks);
    }

    nextHref = extracted.nextHref;

    console.log(
      `Mayrand onsale page ${currentPage}: ${pageItems.length} items (total ${allItems.length})`
    );

    if (maxPage !== null) {
      if (currentPage >= maxPage) break;
    } else if (!nextHref) {
      break;
    }

    currentPage += 1;
    const jitter = 500 + Math.floor(Math.random() * 700);
    await sleep(jitter);
  }

  await browser.close();

  const uniqueItems = new Map();
  allItems.forEach((item) => {
    const key = uniqueKeyForItem(item);
    if (!key) return;
    if (!uniqueItems.has(key)) {
      uniqueItems.set(key, item);
    }
  });

  const finalItems = Array.from(uniqueItems.values());

  await writeJson(path.join(OUTPUT_DIR, 'data.json'), finalItems);
  await writeCsv(path.join(OUTPUT_DIR, 'data.csv'), finalItems);

  await writeJson(path.join(OUTPUT_DIR, 'metadata.json'), {
    timestamp: new Date().toISOString(),
    product_count: finalItems.length,
    page_count: currentPage,
  });
};

main().catch(async (error) => {
  await ensureDirs();
  const debugStamp = new Date().toISOString().replace(/[:.]/g, '-');
  await fs.writeFile(
    path.join(DEBUG_DIR, `mayrand-onsale-fatal-${debugStamp}.error.txt`),
    error.stack || error.message,
    'utf8'
  );
  console.error(error);
  process.exitCode = 1;
});
