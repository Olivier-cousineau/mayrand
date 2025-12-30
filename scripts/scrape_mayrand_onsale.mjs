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
const CONTAINER_SELECTOR = '#product-container';
const CARDS_SELECTOR = 'div.product-card-wrapper';

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

const scrapePage = async (page) => {
  return page.evaluate(({ containerSelector, cardsSelector }) => {
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
        const text = normalizeWhitespace(card.innerText || card.textContent || '');
        const linkElement = card.querySelector('a[href]');
        const name = normalizeWhitespace(linkElement?.textContent);
        const link = linkElement?.getAttribute('href') || null;

        const brand = normalizeWhitespace(
          card.querySelector('.product-brand, .brand, .manufacturer')?.textContent
        );

        const skuAttribute =
          normalizeWhitespace(card.getAttribute('data-sku')) ||
          normalizeWhitespace(card.getAttribute('data-product-id'));
        const skuNode = normalizeWhitespace(
          card.querySelector('.sku, .product-sku, .code, .product-code')?.textContent
        );
        const skuMatch = text?.match(/(?:code|sku|produit|item|article)\s*:?\s*([0-9]{3,})/i);
        const skuFallback = text?.match(/\b([0-9]{3,})\b/);
        const sku = skuAttribute || skuNode || skuMatch?.[1] || skuFallback?.[1] || null;

        const priceSaleText = normalizeWhitespace(
          card.querySelector(
            '.price--sale, .price-sale, .sale-price, .special-price, .price-promo, .promo-price'
          )?.textContent
        );
        const priceRegularText = normalizeWhitespace(
          card.querySelector('del, s, .price--regular, .regular-price, .old-price')
            ?.textContent
        );

        const priceCandidates = Array.from(
          card.querySelectorAll(
            '.product-price, .price, .price-value, .value, .pricing, .product-card-price'
          )
        )
          .map((node) => node.textContent)
          .filter(Boolean);

        const unitLabel = normalizeWhitespace(
          card.querySelector(
            '.unit, .unit-label, .unit-text, .product-unit, .unitLabel, [data-testid="unit"]'
          )?.textContent
        );

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
  }, { containerSelector: CONTAINER_SELECTOR, cardsSelector: CARDS_SELECTOR });
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

const dismissHubspotOverlay = async (page) => {
  const overlaySelector = '#hs-interactives-modal-overlay';
  const closeAttempted = await page.evaluate((selector) => {
    const overlay = document.querySelector(selector);
    if (!overlay) return false;
    const closeButton =
      overlay.querySelector(
        '[aria-label="Close"], [aria-label*="Fermer" i], button[title*="close" i], button[title*="fermer" i], .close, .modal-close, [data-dismiss]'
      ) || overlay.querySelector('button, [role="button"]');
    if (closeButton) {
      closeButton.click();
      return true;
    }
    return false;
  }, overlaySelector);

  if (closeAttempted) {
    await page.waitForTimeout(300);
  }

  const overlayStillPresent = await page.evaluate((selector) => {
    const overlay = document.querySelector(selector);
    if (!overlay) return false;
    const style = window.getComputedStyle(overlay);
    return style.display !== 'none' && style.visibility !== 'hidden';
  }, overlaySelector);

  if (overlayStillPresent) {
    await page.addStyleTag({
      content: `${overlaySelector}{display:none !important;pointer-events:none !important;}`,
    });
  }

  const overlayStillBlocking = await page.evaluate((selector) => {
    const overlay = document.querySelector(selector);
    return Boolean(overlay);
  }, overlaySelector);

  if (overlayStillBlocking) {
    await page.evaluate((selector) => {
      const overlay = document.querySelector(selector);
      if (overlay) overlay.remove();
    }, overlaySelector);
  }
};

const goToPage = async (page, baseUrl, targetPage) => {
  const buttonSelector = `button.pagination-btn[data-page="${targetPage}"]`;
  const button = page.locator(buttonSelector).first();
  if ((await button.count()) > 0) {
    const isDisabled =
      (await button.getAttribute('disabled')) !== null ||
      (await button.getAttribute('aria-disabled')) === 'true';
    if (!isDisabled) {
      await dismissHubspotOverlay(page);
      const previousState = await page.evaluate((cardsSelector) => {
        const normalizeWhitespace = (value) =>
          value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? null;
        const cards = Array.from(document.querySelectorAll(cardsSelector)).slice(0, 3);
        const cardsSignature = cards
          .map((card) => normalizeWhitespace(card?.innerText || card?.textContent || ''))
          .filter(Boolean)
          .join(' | ');
        const activeButton =
          document.querySelector(
            'button.pagination-btn[aria-current="page"], button.pagination-btn.active, button.pagination-btn.is-active'
          ) || null;
        const activePage = activeButton?.getAttribute('data-page') || null;
        return { cardsSignature, activePage };
      }, CARDS_SELECTOR);
      await button.click({ timeout: 15000 });
      await page.waitForFunction(
        ({ targetPageValue }) => {
          const activeButton =
            document.querySelector(
              'button.pagination-btn[aria-current="page"], button.pagination-btn.active, button.pagination-btn.is-active'
            ) || null;
          const activePage = activeButton?.getAttribute('data-page') || null;
          return Boolean(activePage && String(activePage) === String(targetPageValue));
        },
        {
          timeout: 15000,
        },
        {
          targetPageValue: String(targetPage),
        }
      );
      try {
        await page.waitForFunction(
          ({ cardsSelector, beforeSignature }) => {
            if (!beforeSignature) return true;
            const normalizeWhitespace = (value) =>
              value?.replace(/\s+/g, ' ').replace(/\u00a0/g, ' ').trim() ?? null;
            const cards = Array.from(document.querySelectorAll(cardsSelector)).slice(0, 3);
            const cardsSignature = cards
              .map((card) => normalizeWhitespace(card?.innerText || card?.textContent || ''))
              .filter(Boolean)
              .join(' | ');
            return Boolean(cardsSignature && cardsSignature !== beforeSignature);
          },
          {
            timeout: 5000,
          },
          {
            cardsSelector: CARDS_SELECTOR,
            beforeSignature: previousState.cardsSignature,
          }
        );
      } catch (error) {
        if (error?.name !== 'TimeoutError') {
          throw error;
        }
      }
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(750);
      await page.waitForSelector(CARDS_SELECTOR, { timeout: 15000 });
      await waitForCardsStable(page);
      return page.url();
    }
  }

  const url = new URL(baseUrl);
  url.searchParams.set('page', String(targetPage));
  await page.goto(url.toString(), { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(750);
  await page.waitForSelector(CARDS_SELECTOR, { timeout: 15000 });
  await waitForCardsStable(page);
  return url.toString();
};

const goToNextPage = async (page, baseUrl, nextPage) => {
  if (!nextPage || nextPage.disabled) return false;
  if (Number.isFinite(nextPage.pageNumber)) {
    await goToPage(page, baseUrl, nextPage.pageNumber);
    return true;
  }
  if (nextPage.href) {
    const resolved = resolveUrl(nextPage.href, baseUrl);
    if (!resolved) return false;
    if (resolved === page.url()) return false;
    await page.goto(resolved, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(750);
    await page.waitForSelector(CARDS_SELECTOR, { timeout: 15000 });
    await waitForCardsStable(page);
    return true;
  }
  return false;
};

const readHistoricalCount = async () => {
  try {
    const metadata = JSON.parse(
      await fs.readFile(path.join(OUTPUT_DIR, 'metadata.json'), 'utf8')
    );
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
  let currentPage = 1;
  let maxPage = null;
  let emptyPageStreak = 0;
  let pageCount = 0;
  let stoppedReason = null;
  let lastPageUrl = null;

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
      const resolvedLink = resolveUrl(item.link, baseUrlString);
      const resolvedImage = resolveUrl(item.image, baseUrlString);
      return {
        source: SOURCE,
        query,
        name: item.name,
        brand: item.brand,
        sku: item.sku,
        price_sale: sale,
        price_regular: parsedRegular !== null ? parsedRegular : regular,
        unit_label: item.unitLabel,
        unit_price: null,
        url: resolvedLink || buildFallbackUrl(item.sku, item.name, baseUrlString),
        image: resolvedImage,
        category: item.category || pageCategory,
        scraped_at: new Date().toISOString(),
      };
    });

    allItems.push(...pageItems);

    pageCount += 1;

    if (maxPage === null) {
      maxPage =
        getMaxPageFromButtons(extracted.paginationButtons) ??
        getPaginationInfo(extracted.paginationLinks, baseUrl);
    }

    console.log(
      `Mayrand onsale ${query} page ${currentPage}: ${pageItems.length} items (total ${allItems.length})`
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

    const currentUrl = page.url();
    let movedToNext = false;
    if (extracted.nextPage && !extracted.nextPage.disabled) {
      movedToNext = await goToNextPage(page, baseUrlString, extracted.nextPage);
    }
    if (!movedToNext && maxPage !== null && currentPage < maxPage) {
      await goToPage(page, baseUrlString, currentPage + 1);
      movedToNext = true;
    }

    if (!movedToNext) {
      stoppedReason = extracted.nextPage?.disabled ? 'next-disabled' : 'no-next-page';
      break;
    }

    const nextUrl = page.url();
    if (nextUrl === currentUrl || nextUrl === lastPageUrl) {
      stoppedReason = 'pagination-stalled';
      break;
    }
    lastPageUrl = nextUrl;

    currentPage += 1;
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

  await browser.close();

  const uniqueItems = new Map();
  const candidateItems = result?.items ?? [];
  candidateItems.forEach((item) => {
    const key = uniqueKeyForItem(item);
    if (!key) return;
    if (!uniqueItems.has(key)) {
      uniqueItems.set(key, item);
    }
  });

  const finalItems = Array.from(uniqueItems.values());

  const historicalCount = await readHistoricalCount();
  if (finalItems.length === 0) {
    if (historicalCount > 0) {
      throw new Error(
        `Mayrand onsale scrape returned 0 items; historical count ${historicalCount}. Aborting publish.`
      );
    }
    await writeJson(path.join(OUTPUT_DIR, 'metadata.json'), {
      timestamp: new Date().toISOString(),
      total_items: finalItems.length,
      query_used: queryUsed,
      pages_scraped: result?.pageCount ?? 0,
      stopped_reason: result?.stoppedReason ?? null,
    });
    return;
  }

  await writeJson(path.join(OUTPUT_DIR, 'data.json'), finalItems);
  await writeCsv(path.join(OUTPUT_DIR, 'data.csv'), finalItems);

  await writeJson(path.join(OUTPUT_DIR, 'metadata.json'), {
    timestamp: new Date().toISOString(),
    total_items: finalItems.length,
    query_used: queryUsed,
    pages_scraped: result?.pageCount ?? 0,
    stopped_reason: result?.stoppedReason ?? null,
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
