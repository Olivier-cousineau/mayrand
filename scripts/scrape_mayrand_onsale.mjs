import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const BASE_URL = 'https://mayrand.ca/fr/page-recherche';
const OUTPUT_DIR = path.join('public', 'mayrand', 'onsale');
const DEBUG_DIR = path.join('outputs', 'debug');
const SOURCE = 'mayrand';
const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const PAGE_TIMEOUT_MS = 45000;
const PAGE_RETRY_COUNT = 2;
const PAGE_MAX_LIMIT = 100;
const RESULTS_WAIT_TIMEOUT_MS = 20000;
const RESULT_WAIT_ATTEMPTS = 10;
const RESULT_WAIT_INITIAL_DELAY_MS = 500;
const RESULT_WAIT_MAX_DELAY_MS = 1500;
const DETAIL_CONCURRENCY_LIMIT = 4;
const DETAIL_BASE_DELAY_MS = 350;
const DETAIL_JITTER_MS = 450;
const CONTAINER_SELECTOR = '#product-container';
const CARDS_SELECTOR = 'div.product-card-wrapper';
const FALLBACK_CARD_SELECTORS = [
  '#product-container a[href*="/fr/nos-produits/"]',
  'a[href*="/fr/nos-produits/"]',
];
const CAPTCHA_KEYWORDS = ['captcha', 'verify', 'access denied', 'robot', 'cloudflare'];

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

const parseUnitPriceText = (value) => {
  if (!value) return { unitPrice: null, unitLabel: null };
  const normalized = value.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim();
  const match = normalized.match(
    /(?:\$|€)?\s*([0-9]+(?:[.,][0-9]+)?)\s*(?:\$|€)?\s*(?:\/|par)\s*([^\n]+)/i
  );
  if (!match) return { unitPrice: null, unitLabel: null };
  return {
    unitPrice: parseNumber(match[1]),
    unitLabel: normalizeWhitespace(match[2]),
  };
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

const normalizePricePair = ({ sale, regular }) => {
  if (regular === null && sale !== null) {
    return { sale: null, regular: sale };
  }
  if (sale !== null && regular !== null && sale === regular) {
    return { sale: null, regular };
  }
  return { sale, regular };
};

const resolveUrl = (maybeUrl, baseUrl = BASE_URL) => {
  if (!maybeUrl) return null;
  try {
    return new URL(maybeUrl, baseUrl).toString();
  } catch {
    return null;
  }
};

const buildFallbackUrl = (sku, name, baseUrl) => {
  const anchor = sku || name;
  if (!anchor) return baseUrl;
  return `${baseUrl}#${encodeURIComponent(anchor.replace(/\s+/g, '-').slice(0, 80))}`;
};

const buildFallbackKey = (item) => {
  if (!item?.name) return null;
  return `${item.name}__${item.price_sale}__${item.unit_label || ''}`;
};

const uniqueKeyForItem = (item) => item?.sku || item?.url || buildFallbackKey(item);

const extractSkuFromUrl = (url) => {
  if (!url) return null;
  const match = url.match(/-(\d{3,})(?:\D|$)/);
  return match?.[1] || null;
};

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

const getResultsDomState = async (page) =>
  page.evaluate(({ containerSelector, cardsSelector }) => {
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

    const cardSelectors = [cardsSelector];
    const loaderSelectors = [
      '.loading',
      '.loader',
      '.spinner',
      '.is-loading',
      '[aria-busy="true"]',
      '[data-loading="true"]',
      '.skeleton',
      '.skeleton-loader',
    ];

    const container = document.querySelector(containerSelector);
    const scope = container ?? document;
    const cards = scope.querySelectorAll(cardSelectors.join(','));
    const loaderVisible = loaderSelectors.some((selector) => {
      const node = document.querySelector(selector);
      return node && isVisible(node);
    });

    const emptyStateTextFromSelectors = normalizeWhitespace(
      Array.from(
        document.querySelectorAll(
          '.no-results, .empty, .results-empty, .search-empty, [data-testid="no-results"]'
        )
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const bodyText = normalizeWhitespace(document.body?.innerText || '');
    const emptyStateMatch =
      bodyText?.match(
        /(Aucun r\u00e9sultat[^.]*|0\s*r\u00e9sultat[^.]*|Aucun produit[^.]*|No results[^.]*|0\s*results[^.]*)/i
      ) ?? null;
    const emptyStateText = emptyStateTextFromSelectors || emptyStateMatch?.[1] || null;

    const resultsCountText = normalizeWhitespace(
      Array.from(
        document.querySelectorAll('.results-count, .search-result-count, .product-count, .count')
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    return {
      cardsCount: cards.length,
      loaderVisible,
      resultsCountText,
      emptyStateText,
      containerSelector: container ? container.className || container.id : null,
    };
  }, { containerSelector: CONTAINER_SELECTOR, cardsSelector: CARDS_SELECTOR });

const waitForResultsWithRetry = async (page, contextLabel) => {
  const step =
    RESULT_WAIT_ATTEMPTS > 1
      ? (RESULT_WAIT_MAX_DELAY_MS - RESULT_WAIT_INITIAL_DELAY_MS) / (RESULT_WAIT_ATTEMPTS - 1)
      : 0;

  for (let attempt = 1; attempt <= RESULT_WAIT_ATTEMPTS; attempt += 1) {
    const state = await getResultsDomState(page);
    console.log(`Mayrand onsale wait attempt ${attempt} (${contextLabel})`, state);

    if (state.resultsCountText || state.cardsCount > 0 || !state.loaderVisible) {
      await page.waitForTimeout(200);
      return state;
    }

    const delay = Math.round(
      RESULT_WAIT_INITIAL_DELAY_MS + step * (attempt - 1)
    );
    await page.waitForTimeout(delay);
  }

  await page.waitForTimeout(500);
  return getResultsDomState(page);
};

const scrapeProductPage = async (page, productUrl) => {
  await page.goto(productUrl, { waitUntil: 'domcontentloaded' });
  await acceptCookies(page);
  await page.waitForTimeout(500);
  return page.evaluate(() => {
    const normalizeWhitespace = (value) =>
      value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? null;

    const readJsonLd = () => {
      const scripts = Array.from(
        document.querySelectorAll('script[type="application/ld+json"]')
      )
        .map((node) => node.textContent)
        .filter(Boolean);
      const parsed = [];
      for (const raw of scripts) {
        try {
          parsed.push(JSON.parse(raw));
        } catch {
          continue;
        }
      }
      return parsed;
    };

    const flattenJsonLd = (data) => {
      if (!data) return [];
      if (Array.isArray(data)) return data.flatMap(flattenJsonLd);
      if (typeof data === 'object') {
        if (data['@graph']) return flattenJsonLd(data['@graph']);
        return [data];
      }
      return [];
    };

    const jsonLdEntries = readJsonLd().flatMap(flattenJsonLd);
    const productEntry = jsonLdEntries.find((entry) => {
      const type = entry?.['@type'];
      if (!type) return false;
      if (Array.isArray(type)) {
        return type.some((value) => String(value).toLowerCase() === 'product');
      }
      return String(type).toLowerCase() === 'product';
    });

    const productName = normalizeWhitespace(productEntry?.name);
    const productBrand = normalizeWhitespace(
      productEntry?.brand?.name || productEntry?.brand
    );
    const productSku = normalizeWhitespace(productEntry?.sku);

    const offers = productEntry?.offers;
    const offersList = Array.isArray(offers) ? offers : offers ? [offers] : [];
    const offerPrices = offersList
      .map((offer) => {
        if (!offer) return null;
        if (offer.price) return String(offer.price);
        if (offer.lowPrice) return String(offer.lowPrice);
        if (offer.priceSpecification?.price) return String(offer.priceSpecification.price);
        return null;
      })
      .filter(Boolean);

    const h1Text = normalizeWhitespace(document.querySelector('h1')?.textContent);
    const ogTitle = normalizeWhitespace(
      document.querySelector('meta[property="og:title"], meta[name="og:title"]')?.getAttribute(
        'content'
      )
    );

    const breadcrumb = normalizeWhitespace(
      Array.from(document.querySelectorAll('nav.breadcrumb, .breadcrumb, .breadcrumbs'))
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const unitPriceSaleText = normalizeWhitespace(
      document.querySelector('.unit_price span.me-2')?.textContent
    );
    const unitPriceRegularText = normalizeWhitespace(
      document.querySelector('.unit_price del.price-discount')?.textContent
    );
    const priceSaleText =
      unitPriceSaleText ||
      normalizeWhitespace(
        document.querySelector(
          '.price--sale, .price-sale, .sale-price, .special-price, .price-promo, .promo-price'
        )?.textContent
      );
    const priceRegularText =
      unitPriceRegularText ||
      normalizeWhitespace(
        document.querySelector('del, s, .price--regular, .regular-price, .old-price')
          ?.textContent
      );

    const priceCandidates = Array.from(
      document.querySelectorAll(
        '.product-price, .price, .price-value, .value, .pricing, .product-card-price'
      )
    )
      .map((node) => node.textContent)
      .filter(Boolean);

    const unitLabel = normalizeWhitespace(
      document.querySelector(
        '.unit_quantity, .unit, .unit-label, .unit-text, .product-unit, .unitLabel, [data-testid="unit"]'
      )?.textContent
    );
    const unitPriceText = normalizeWhitespace(
      document.querySelector(
        '.unit-price-ref, .unit-price, .price-unit, .price-per, .unit-price-value, [data-testid="unit-price"]'
      )?.textContent
    );

    return {
      productName,
      productBrand,
      productSku,
      offerPrices,
      h1Text,
      ogTitle,
      breadcrumb,
      priceSaleText,
      priceRegularText,
      priceCandidates,
      unitLabel,
      unitPriceText,
    };
  });
};

const enrichItemsWithDetails = async (context, items) => {
  const results = new Array(items.length);
  let index = 0;

  const worker = async () => {
    while (index < items.length) {
      const current = index;
      index += 1;
      const item = items[current];
      if (!item?.url) {
        results[current] = item;
        continue;
      }
      await sleep(DETAIL_BASE_DELAY_MS + Math.random() * DETAIL_JITTER_MS);
      const page = await context.newPage();
      page.setDefaultTimeout(PAGE_TIMEOUT_MS);
      try {
        const details = await scrapeProductPage(page, item.url);
        const parsedSale = parseNumber(details.priceSaleText);
        const parsedRegular = parseNumber(details.priceRegularText);
        const offerPrices = details.offerPrices.map((entry) => parseNumber(entry)).filter(Boolean);
        const offerSale = offerPrices.length > 0 ? Math.min(...offerPrices) : null;
        const offerRegular = offerPrices.length > 1 ? Math.max(...offerPrices) : null;
        const priceCandidates =
          parsedSale !== null || parsedRegular !== null
            ? { sale: parsedSale, regular: parsedRegular }
            : parsePriceCandidates(details.priceCandidates);
        const combinedPriceSale = parsedSale ?? offerSale ?? priceCandidates.sale ?? null;
        const combinedPriceRegular =
          parsedRegular ?? offerRegular ?? priceCandidates.regular ?? null;
        const normalizedPrices = normalizePricePair({
          sale: combinedPriceSale,
          regular: combinedPriceRegular,
        });

        const unitPriceParsed = parseUnitPriceText(details.unitPriceText);
        const nameFallback =
          details.h1Text ||
          details.ogTitle ||
          details.productName ||
          item.name ||
          item.sku ||
          item.url ||
          'Produit Mayrand';

        results[current] = {
          ...item,
          name: nameFallback,
          brand: details.productBrand || item.brand || null,
          sku:
            details.productSku ||
            item.sku ||
            extractSkuFromUrl(item.url) ||
            null,
          price_sale: normalizedPrices.sale ?? item.price_sale ?? null,
          price_regular: normalizedPrices.regular ?? item.price_regular ?? null,
          unit_label:
            details.unitLabel ||
            unitPriceParsed.unitLabel ||
            item.unit_label ||
            null,
          unit_price: unitPriceParsed.unitPrice ?? item.unit_price ?? null,
          category: details.breadcrumb || item.category || null,
        };
      } catch (error) {
        results[current] = {
          ...item,
          name: item.name || item.sku || item.url || 'Produit Mayrand',
        };
        console.log('Mayrand onsale product scrape failed', {
          url: item.url,
          error: error?.message,
        });
      } finally {
        await page.close();
      }
    }
  };

  const workers = Array.from({ length: DETAIL_CONCURRENCY_LIMIT }, () => worker());
  await Promise.all(workers);
  return results;
};

const scrapePage = async (page) => {
  return page.evaluate(({ containerSelector, cardsSelector, fallbackSelectors }) => {
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

    const cardSelectors = [cardsSelector, ...fallbackSelectors];
    const cardSet = new Set();
    cardSelectors.forEach((selector) => {
      document.querySelectorAll(selector).forEach((element) => cardSet.add(element));
    });
    const cards = Array.from(cardSet).filter((element) => isVisible(element));

    const resultsContainer = document.querySelector(containerSelector);

    const breadcrumb = normalizeWhitespace(
      Array.from(document.querySelectorAll('nav.breadcrumb, .breadcrumb, .breadcrumbs'))
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const results = cards.map((card) => {
      try {
        const cardRoot =
          card.closest?.(cardsSelector) ||
          card.closest?.('[data-product-id]') ||
          card.closest?.('[class*="product"]') ||
          card;
        const text = normalizeWhitespace(
          cardRoot?.innerText || cardRoot?.textContent || card.innerText || card.textContent || ''
        );
        const linkElement = card.matches('a[href]') ? card : cardRoot?.querySelector('a[href]');
        const name = normalizeWhitespace(linkElement?.textContent);
        const link = linkElement?.getAttribute('href') || cardRoot?.getAttribute('href') || null;

        const brand = normalizeWhitespace(
          cardRoot?.querySelector('.product-brand, .brand, .manufacturer')?.textContent
        );

        const skuAttribute =
          normalizeWhitespace(cardRoot?.getAttribute('data-sku')) ||
          normalizeWhitespace(cardRoot?.getAttribute('data-product-id')) ||
          normalizeWhitespace(card.getAttribute('data-sku')) ||
          normalizeWhitespace(card.getAttribute('data-product-id'));
        const skuNode = normalizeWhitespace(
          cardRoot?.querySelector('.sku, .product-sku, .code, .product-code')?.textContent
        );
        const skuMatch = text?.match(/(?:code|sku|produit|item|article)\s*:?\s*([0-9]{3,})/i);
        const skuFallback = text?.match(/\b([0-9]{3,})\b/);
        const sku = skuAttribute || skuNode || skuMatch?.[1] || skuFallback?.[1] || null;

        const unitPriceSaleText = normalizeWhitespace(
          cardRoot?.querySelector('.unit_price span.me-2')?.textContent
        );
        const unitPriceRegularText = normalizeWhitespace(
          cardRoot?.querySelector('.unit_price del.price-discount')?.textContent
        );
        const priceSaleText =
          unitPriceSaleText ||
          normalizeWhitespace(
            cardRoot?.querySelector(
              '.price--sale, .price-sale, .sale-price, .special-price, .price-promo, .promo-price'
            )?.textContent
          );
        const priceRegularText =
          unitPriceRegularText ||
          normalizeWhitespace(
            cardRoot?.querySelector('del, s, .price--regular, .regular-price, .old-price')
              ?.textContent
          );

        const priceCandidates = Array.from(
          cardRoot?.querySelectorAll(
            '.product-price, .price, .price-value, .value, .pricing, .product-card-price'
          ) || []
        )
          .map((node) => node.textContent)
          .filter(Boolean);

        const unitLabel = normalizeWhitespace(
          cardRoot?.querySelector(
            '.unit_quantity, .unit, .unit-label, .unit-text, .product-unit, .unitLabel, [data-testid="unit"]'
          )?.textContent
        );

        const image =
          cardRoot?.querySelector('img')?.getAttribute('src') ||
          cardRoot?.querySelector('img')?.getAttribute('data-src') ||
          cardRoot?.querySelector('img')?.getAttribute('data-lazy') ||
          null;

        const category =
          normalizeWhitespace(cardRoot?.getAttribute('data-category')) ||
          normalizeWhitespace(cardRoot?.querySelector('.category, .product-category')?.textContent) ||
          null;

        return {
          name,
          brand,
          sku,
          priceSaleText,
          priceRegularText,
          priceCandidates,
          unitLabel,
          link,
          image,
          category,
          breadcrumb,
        };
      } catch (error) {
        return {
          name: null,
          brand: null,
          sku: null,
          priceSaleText: null,
          priceRegularText: null,
          priceCandidates: [],
          unitLabel: null,
          link: null,
          image: null,
          category: null,
          breadcrumb,
          error: error?.message || String(error),
        };
      }
    });

    const paginationLinks = Array.from(
      document.querySelectorAll(
        '.pagination a, nav.pagination a, .pager a, .pagination-link, .pagination__link'
      )
    ).map((link) => link.getAttribute('href'));

    const paginationButtons = Array.from(
      document.querySelectorAll('button.pagination-btn[data-page]')
    )
      .map((button) => {
        const pageValue = button.getAttribute('data-page');
        const pageNumber = pageValue ? Number.parseInt(pageValue, 10) : null;
        return Number.isFinite(pageNumber) ? pageNumber : null;
      })
      .filter((pageNumber) => pageNumber !== null);

    const emptyStateTextFromSelectors = normalizeWhitespace(
      Array.from(
        document.querySelectorAll(
          '.no-results, .empty, .results-empty, .search-empty, [data-testid="no-results"]'
        )
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const bodyText = normalizeWhitespace(document.body?.innerText || '');
    const emptyStateMatch =
      bodyText?.match(
        /(Aucun r\u00e9sultat[^.]*|0\s*r\u00e9sultat[^.]*|Aucun produit[^.]*|No results[^.]*|0\s*results[^.]*)/i
      ) ?? null;
    const emptyStateText = emptyStateTextFromSelectors || emptyStateMatch?.[1] || null;

    const resultsCountText = normalizeWhitespace(
      Array.from(
        document.querySelectorAll('.results-count, .search-result-count, .product-count, .count')
      )
        .map((node) => node.textContent)
        .find(Boolean)
    );

    const nextCandidates = Array.from(document.querySelectorAll('a, button')).filter((node) => {
      const text = normalizeWhitespace(node.textContent || '');
      const aria = normalizeWhitespace(node.getAttribute('aria-label') || '');
      const rel = normalizeWhitespace(node.getAttribute('rel') || '');
      const haystack = [text, aria, rel].filter(Boolean).join(' ').toLowerCase();
      return (
        haystack.includes('suivant') ||
        haystack.includes('next') ||
        haystack.includes('prochain') ||
        haystack.includes('suivante') ||
        rel.toLowerCase() === 'next'
      );
    });

    const nextPageNode = nextCandidates.find((node) => isVisible(node)) || null;
    const nextPageDisabled = nextPageNode
      ? nextPageNode.hasAttribute('disabled') ||
        nextPageNode.getAttribute('aria-disabled') === 'true' ||
        nextPageNode.classList.contains('disabled') ||
        nextPageNode.classList.contains('is-disabled')
      : null;
    const nextPageHref = nextPageNode?.getAttribute('href') || null;
    const nextPageNumber = nextPageNode?.getAttribute('data-page');
    const nextPageLabel = normalizeWhitespace(nextPageNode?.textContent || '');

    return {
      results,
      paginationLinks,
      paginationButtons,
      nextPage: {
        href: nextPageHref,
        pageNumber: nextPageNumber ? Number.parseInt(nextPageNumber, 10) : null,
        label: nextPageLabel,
        disabled: nextPageDisabled,
      },
      breadcrumb,
      emptyStateText,
      resultsCountText,
      containerSelector: resultsContainer ? resultsContainer.className || resultsContainer.id : null,
      visibleCardCount: cards.length,
    };
  }, {
    containerSelector: CONTAINER_SELECTOR,
    cardsSelector: CARDS_SELECTOR,
    fallbackSelectors: FALLBACK_CARD_SELECTORS,
  });
};

const getPaginationInfo = (paginationLinks, baseUrl) => {
  let maxPage = null;
  paginationLinks.forEach((href) => {
    if (!href) return;
    try {
      const url = new URL(href, baseUrl);
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

const getMaxPageFromButtons = (paginationButtons) => {
  if (!paginationButtons || paginationButtons.length === 0) return null;
  return paginationButtons.reduce((max, value) => (max === null ? value : Math.max(max, value)), null);
};

const waitForCardsStable = async (page) => {
  let lastCount = 0;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const count = await page.locator(CARDS_SELECTOR).count();
    if (count > 0 && count === lastCount) {
      return count;
    }
    lastCount = count;
    await page.waitForTimeout(250);
  }
  return lastCount;
};

const getListingCardCount = async (page) =>
  page.evaluate(
    ({ cardsSelector, fallbackSelectors }) => {
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
      const cardSet = new Set();
      [cardsSelector, ...fallbackSelectors].forEach((selector) => {
        document.querySelectorAll(selector).forEach((element) => cardSet.add(element));
      });
      return Array.from(cardSet).filter((element) => isVisible(element)).length;
    },
    { cardsSelector: CARDS_SELECTOR, fallbackSelectors: FALLBACK_CARD_SELECTORS }
  );

const scrollForLazyLoad = async (page) => {
  const initialCount = await getListingCardCount(page);
  await page.evaluate(async () => {
    await new Promise((resolve) => {
      let total = 0;
      const distance = Math.max(window.innerHeight * 0.8, 300);
      const timer = window.setInterval(() => {
        window.scrollBy(0, distance);
        total += distance;
        if (total >= document.body.scrollHeight) {
          window.clearInterval(timer);
          resolve(null);
        }
      }, 200);
    });
  });
  await page.waitForTimeout(500);
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }));
  await page.waitForTimeout(400);
  const finalCount = await getListingCardCount(page);
  return { initialCount, finalCount };
};

const getCaptchaStatus = async (page, html = null) => {
  const content = html || (await page.content());
  const lower = content.toLowerCase();
  const hits = CAPTCHA_KEYWORDS.filter((keyword) => lower.includes(keyword));
  return {
    detected: hits.length > 0,
    keywords: hits,
  };
};

const saveZeroItemsDebug = async (page, query, currentPage, extracted) => {
  const debugStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const debugBase = `mayrand_zero_items-${slugify(query)}-page-${currentPage}-${debugStamp}`;
  const html = await page.content();
  const pageUrl = page.url();
  const pageTitle = await page.title();
  const captchaStatus = await getCaptchaStatus(page, html);

  await fs.writeFile(path.join(DEBUG_DIR, `${debugBase}.html`), html, 'utf8');
  await page.screenshot({
    path: path.join(DEBUG_DIR, `${debugBase}.png`),
    fullPage: true,
  });
  await fs.writeFile(
    path.join(DEBUG_DIR, `${debugBase}.json`),
    `${JSON.stringify(
      {
        query,
        page: currentPage,
        url: pageUrl,
        title: pageTitle,
        captcha: captchaStatus,
        empty_state_text: extracted?.emptyStateText ?? null,
        results_count_text: extracted?.resultsCountText ?? null,
        container_selector: extracted?.containerSelector ?? null,
        visible_card_count: extracted?.visibleCardCount ?? null,
        cards_selector: CARDS_SELECTOR,
        fallback_selectors: FALLBACK_CARD_SELECTORS,
      },
      null,
      2
    )}\n`,
    'utf8'
  );
  console.log('Mayrand onsale 0 items debug', {
    query,
    page: currentPage,
    url: pageUrl,
    title: pageTitle,
    captcha: captchaStatus,
  });
};

const killOverlays = async (page) => {
  await page.evaluate(() => {
    const selectors = [
      '#hs-interactives-modal-overlay',
      '#hs-web-interactives-top-anchor',
      '[id*="hs-interactives"]',
      '[class*="modal-overlay"]',
      '[class*="overlay"]',
      '[role="dialog"]',
    ];
    for (const sel of selectors) {
      document.querySelectorAll(sel).forEach((el) => el.remove());
    }
    document.querySelectorAll('body *').forEach((el) => {
      const id = (el.id || '').toLowerCase();
      const cls = (el.className || '').toString().toLowerCase();
      if (id.includes('overlay') || cls.includes('overlay')) {
        el.style.pointerEvents = 'none';
      }
    });
  });
};

const getActivePage = async (page) =>
  page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button.pagination-btn'));
    const active =
      btns.find((b) => b.classList.contains('active')) ||
      btns.find((b) => b.getAttribute('aria-current') === 'page');
    const raw = active?.getAttribute('data-page') || active?.textContent?.trim() || null;
    const n = raw ? Number(String(raw).replace(/\D/g, '')) : null;
    return Number.isFinite(n) ? n : null;
  });

const getCardsSignature = async (page) =>
  page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll('.product-container *'))
      .filter(
        (el) => el.tagName === 'A' || (el.className || '').toString().toLowerCase().includes('product')
      )
      .slice(0, 15);
    const sig = cards.map((el) => (el.getAttribute('href') || el.textContent || '').trim().slice(0, 80));
    return sig.join('|');
  });

const getPagerText = async (page) =>
  page.evaluate(() => {
    const normalizeWhitespace = (value) =>
      value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? '';
    const pager = document.querySelector(
      '.pagination, nav.pagination, .pager, .pagination__list, [aria-label*="pagination" i]'
    );
    return normalizeWhitespace(pager?.textContent || '');
  });

const countCards = async (page) => page.locator(CARDS_SELECTOR).count();

const pagerShowsTarget = (pagerText, targetPage) => {
  if (!pagerText) return false;
  const normalized = pagerText.replace(/\s+/g, ' ').toLowerCase();
  const target = String(targetPage);
  return normalized.includes(` ${target} `) || normalized.endsWith(` ${target}`) || normalized.startsWith(`${target} `);
};

const goToPage = async (page, baseUrl, targetPage) => {
  const buttonSelector = `button.pagination-btn[data-page="${targetPage}"]`;
  const button = page.locator(buttonSelector).first();
  const initialActive = await getActivePage(page);
  if ((await button.count()) > 0) {
    const isDisabled =
      (await button.getAttribute('disabled')) !== null ||
      (await button.getAttribute('aria-disabled')) === 'true';
    if (!isDisabled) {
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        await killOverlays(page);

        const beforeActive = await getActivePage(page);
        const beforeCount = await countCards(page);
        const beforeSig = await getCardsSignature(page);
        const beforePagerText = await getPagerText(page);

        await button.scrollIntoViewIfNeeded();

        try {
          await button.click({ timeout: 15000 });
        } catch {
          await killOverlays(page);
          await button.click({ timeout: 15000, force: true });
        }

        try {
          await page.waitForLoadState('networkidle', { timeout: 20000 });
          await waitForResultsWithRetry(page, `pagination-${targetPage}-attempt-${attempt}`);
          await waitForCardsStable(page);
          const afterActive = await getActivePage(page);
          const afterCount = await countCards(page);
          const afterSig = await getCardsSignature(page);
          const afterPagerText = await getPagerText(page);
          const sigChanged = beforeActive !== afterActive || beforeCount !== afterCount;

          console.log('Pagination status', {
            attempt,
            target: targetPage,
            beforeActive,
            afterActive,
            beforePagerText,
            afterPagerText,
            sigChanged,
            signatureDelta: beforeSig !== afterSig,
          });

          if (afterActive === targetPage || pagerShowsTarget(afterPagerText, targetPage)) {
            return {
              beforeActive,
              afterActive,
              sigChanged,
            };
          }

          await page.waitForTimeout(500);
        } catch (error) {
          const afterActive = await getActivePage(page);
          console.log('Pagination attempt failed', {
            attempt,
            target: targetPage,
            beforeActive,
            afterActive,
            error: error?.message,
          });
          await page.waitForTimeout(800);
        }
      }

      await page.screenshot({
        path: path.join(DEBUG_DIR, `mayrand-pagination-fail-${targetPage}.png`),
        fullPage: true,
      });
      const html = await page.content();
      await fs.writeFile(path.join(DEBUG_DIR, `mayrand-pagination-fail-${targetPage}.html`), html, 'utf8');
      return null;
    }
  }

  const url = new URL(baseUrl);
  url.searchParams.set('page', String(targetPage));
  const beforeActive = initialActive ?? (await getActivePage(page));
  await page.goto(url.toString(), { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(750);
  await page.waitForSelector(CARDS_SELECTOR, { timeout: 15000 });
  await waitForCardsStable(page);
  const afterActive = await getActivePage(page);
  return {
    beforeActive,
    afterActive,
    sigChanged: beforeActive !== afterActive,
  };
};

const goToNextPage = async (page, baseUrl, nextPage) => {
  if (!nextPage || nextPage.disabled) return null;
  if (Number.isFinite(nextPage.pageNumber)) {
    const moved = await goToPage(page, baseUrl, nextPage.pageNumber);
    return moved;
  }
  if (nextPage.href) {
    const resolved = resolveUrl(nextPage.href, baseUrl);
    if (!resolved) return null;
    await page.goto(resolved, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(750);
    await page.waitForSelector(CARDS_SELECTOR, { timeout: 15000 });
    await waitForCardsStable(page);
    const afterActive = await getActivePage(page);
    return {
      beforeActive: null,
      afterActive,
      sigChanged: false,
    };
  }
  return null;
};

const readHistoricalCount = async () => {
  try {
    const metadata = JSON.parse(
      await fs.readFile(path.join(OUTPUT_DIR, 'metadata.json'), 'utf8')
    );
    if (Number.isFinite(metadata?.totalItems)) {
      return metadata.totalItems;
    }
    if (Number.isFinite(metadata?.total_items)) {
      return metadata.total_items;
    }
    if (Number.isFinite(metadata?.product_count)) {
      return metadata.product_count;
    }
  } catch {
    // ignore
  }
  try {
    const data = JSON.parse(await fs.readFile(path.join(OUTPUT_DIR, 'data.json'), 'utf8'));
    return Array.isArray(data) ? data.length : 0;
  } catch {
    return 0;
  }
};

const slugify = (value) =>
  (value || 'query')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40) || 'query';

const scrapeListing = async (page, query) => {
  const baseUrl = new URL(BASE_URL);
  if (query) {
    baseUrl.searchParams.set('search', query);
  }
  const baseUrlString = baseUrl.toString();
  const allItems = [];
  const uniqueItems = new Map();
  let currentPage = 1;
  let maxPage = null;
  let emptyPageStreak = 0;
  let pageCount = 0;
  let stoppedReason = null;
  while (currentPage <= PAGE_MAX_LIMIT) {
    const pageUrl = currentPage === 1 ? baseUrlString : page.url() || baseUrlString;

    let extracted = null;
    let lastError = null;

    for (let attempt = 0; attempt <= PAGE_RETRY_COUNT; attempt += 1) {
      try {
        if (currentPage === 1) {
          await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });
        }
        await acceptCookies(page);
        await page.waitForTimeout(750);
        try {
          await page.waitForSelector(CARDS_SELECTOR, { timeout: RESULTS_WAIT_TIMEOUT_MS });
        } catch {
          // continue to fallback wait logic
        }
        await waitForCardsStable(page);
        await waitForResultsWithRetry(page, `${query}-page-${currentPage}`);
        await scrollForLazyLoad(page);
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
        path.join(DEBUG_DIR, `mayrand-onsale-${slugify(query)}-page-${currentPage}-${debugStamp}.error.txt`),
        `Failed to scrape page ${currentPage}: ${errorMessage}\n`,
        'utf8'
      );
      break;
    }

    if (extracted.results.length === 0) {
      await saveZeroItemsDebug(page, query, currentPage, extracted);
    }

    if (extracted.visibleCardCount === 0) {
      const debugBase = `mayrand-onsale-${slugify(query)}-page-${currentPage}`;
      const htmlPath = path.join(DEBUG_DIR, `${debugBase}.html`);
      await fs.writeFile(htmlPath, await page.content(), 'utf8');
      await page.screenshot({
        path: path.join(DEBUG_DIR, `${debugBase}.png`),
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
        cards_selector: CARDS_SELECTOR,
      };
      await fs.writeFile(
        path.join(DEBUG_DIR, `${debugBase}.empty.json`),
        `${JSON.stringify(emptyDebug, null, 2)}\n`,
        'utf8'
      );
      console.log(`Mayrand onsale ${query} page ${currentPage}: 0 items`, emptyDebug);
      emptyPageStreak += 1;
    } else {
      emptyPageStreak = 0;
    }

    const pageCategory = extracted.breadcrumb || null;

    const pageItems = extracted.results.map((item) => {
      const parsedSale = parseNumber(item.priceSaleText);
      const parsedRegular = parseNumber(item.priceRegularText);
      const { sale, regular } =
        parsedSale !== null || parsedRegular !== null
          ? { sale: parsedSale, regular: parsedRegular }
          : parsePriceCandidates(item.priceCandidates);
      const normalizedPrices = normalizePricePair({ sale, regular });
      const resolvedLink = resolveUrl(item.link, baseUrlString);
      const resolvedImage = resolveUrl(item.image, baseUrlString);
      return {
        source: SOURCE,
        query,
        name: item.name,
        brand: item.brand,
        sku: item.sku,
        price_sale: normalizedPrices.sale,
        price_regular: normalizedPrices.regular,
        unit_label: item.unitLabel,
        unit_price: null,
        url: resolvedLink || buildFallbackUrl(item.sku, item.name, baseUrlString),
        image: resolvedImage,
        category: item.category || pageCategory,
        scraped_at: new Date().toISOString(),
      };
    });

    const allCountBefore = allItems.length;
    const keyStats = { withSku: 0, withUrl: 0, withFallbackKey: 0 };
    pageItems.forEach((item) => {
      let key = null;
      if (item.sku) {
        keyStats.withSku += 1;
        key = item.sku;
      } else if (item.url) {
        keyStats.withUrl += 1;
        key = item.url;
      } else if (item.name) {
        keyStats.withFallbackKey += 1;
        key = buildFallbackKey(item);
      }

      if (key) {
        if (!uniqueItems.has(key)) {
          uniqueItems.set(key, item);
          allItems.push(item);
        }
        return;
      }
      allItems.push(item);
    });
    const allCountAfter = allItems.length;

    pageCount += 1;

    const derivedMaxPage =
      getMaxPageFromButtons(extracted.paginationButtons) ??
      getPaginationInfo(extracted.paginationLinks, baseUrl);
    if (Number.isFinite(derivedMaxPage)) {
      maxPage = maxPage === null ? derivedMaxPage : Math.max(maxPage, derivedMaxPage);
    }

    console.log(
      `Mayrand onsale ${query} page ${currentPage}: extracted ${pageItems.length}, unique total ${allItems.length}`,
      {
        extractedCount: pageItems.length,
        allCountBefore,
        allCountAfter,
        withSku: keyStats.withSku,
        withUrl: keyStats.withUrl,
        withFallbackKey: keyStats.withFallbackKey,
      }
    );

    if (pageItems.length === 0) {
      const debugStamp = new Date().toISOString().replace(/[:.]/g, '-');
      const debugBase = `mayrand-onsale-${slugify(query)}-page-${currentPage}-extraction-0-${debugStamp}`;
      await fs.writeFile(path.join(DEBUG_DIR, `${debugBase}.html`), await page.content(), 'utf8');
      await page.screenshot({
        path: path.join(DEBUG_DIR, `${debugBase}.png`),
        fullPage: true,
      });
    }

    if (emptyPageStreak >= 2) {
      console.log(
        `Mayrand onsale ${query} stopping after ${emptyPageStreak} empty pages in a row.`
      );
      stoppedReason = 'empty-pages-streak';
      break;
    }

    if (maxPage !== null && currentPage >= maxPage) {
      stoppedReason = 'max-page-reached';
      break;
    }

    const targetPage = currentPage + 1;
    let paginationStatus = null;
    if (extracted.nextPage && !extracted.nextPage.disabled) {
      paginationStatus = await goToNextPage(page, baseUrlString, extracted.nextPage);
    }
    if (!paginationStatus && maxPage !== null && currentPage < maxPage) {
      paginationStatus = await goToPage(page, baseUrlString, targetPage);
    }

    if (!paginationStatus) {
      stoppedReason = extracted.nextPage?.disabled ? 'next-disabled' : 'no-next-page';
      break;
    }

    if (paginationStatus.afterActive !== targetPage) {
      console.log('No more pages, stopping at page', currentPage);
      stoppedReason = 'no-next-page';
      break;
    }

    currentPage = targetPage;
    const jitter = 500 + Math.floor(Math.random() * 700);
    await sleep(jitter);
  }

  if (!stoppedReason) {
    stoppedReason = currentPage > PAGE_MAX_LIMIT ? 'page-limit-reached' : 'completed';
  }

  return {
    baseUrl: baseUrlString,
    items: allItems,
    pageCount,
    stoppedReason,
  };
};

const main = async () => {
  await ensureDirs();

  const browser = await chromium.launch({ headless: true });
  const browserContext = await browser.newContext({
    userAgent: USER_AGENT,
    locale: 'fr-CA',
    viewport: { width: 1365, height: 768 },
  });
  const page = await browserContext.newPage();
  page.setDefaultTimeout(PAGE_TIMEOUT_MS);

  const queries = ['onsale', 'promo', 'solde'];
  let result = null;
  let queryUsed = null;
  for (const query of queries) {
    const runResult = await scrapeListing(page, query);
    if (runResult.items.length > 0) {
      result = runResult;
      queryUsed = query;
      break;
    }
    if (!result) {
      result = runResult;
      queryUsed = query;
    }
  }

  const uniqueItems = new Map();
  const finalItems = [];
  const candidateItems = result?.items ?? [];
  candidateItems.forEach((item) => {
    const key = uniqueKeyForItem(item);
    if (key) {
      if (uniqueItems.has(key)) return;
      uniqueItems.set(key, item);
    }
    finalItems.push(item);
  });

  const enrichedItems = await enrichItemsWithDetails(browserContext, finalItems);
  await browser.close();

  const historicalCount = await readHistoricalCount();
  if (enrichedItems.length === 0) {
    if (historicalCount > 0) {
      throw new Error(
        `Mayrand onsale scrape returned 0 items; historical count ${historicalCount}. Aborting publish.`
      );
    }
    await writeJson(path.join(OUTPUT_DIR, 'metadata.json'), {
      pagesScraped: result?.pageCount ?? 0,
      totalItems: enrichedItems.length,
    });
    return;
  }

  await writeJson(path.join(OUTPUT_DIR, 'data.json'), enrichedItems);
  await writeCsv(path.join(OUTPUT_DIR, 'data.csv'), enrichedItems);

  await writeJson(path.join(OUTPUT_DIR, 'metadata.json'), {
    pagesScraped: result?.pageCount ?? 0,
    totalItems: enrichedItems.length,
  });
};

export { normalizeWhitespace, parseNumber, parseUnitPriceText };

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
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
}
