const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs").promises;
const zlib = require("zlib");
const xmlParser = require("fast-xml-parser");
const { URL } = require("url");

class ProductCrawler {
  constructor() {
    // Domains to crawl
    this.allowedDomains = [
      "virgio.com",
      "tatacliq.com",
      // "intl.nykaafashion.com",
      "westside.com",
      // "myntra.com",
      "nykaafashion.com",
    ];

    // Base URLs
    this.startUrls = [
      "https://www.virgio.com/",
      "https://www.tatacliq.com/",
      // "https://intl.nykaafashion.com/",
      "https://www.westside.com/",
      "https://www.nykaafashion.com/",
    ];

    // Robots.txt URLs
    this.robotsUrls = [
      "https://www.virgio.com/robots.txt",
      "https://www.tatacliq.com/robots.txt",
      // "https://intl.nykaafashion.com/robots.txt",
      "https://www.westside.com/robots.txt",
      "https://www.nykaafashion.com/robots.txt",
    ];

    // Sitemap URLs
    this.sitemapUrls = [
      "https://www.virgio.com/sitemap.xml",
      "https://www.tatacliq.com/sitemap.xml",
      "https://www.westside.com/sitemap.xml",
      // "https://intl.nykaafashion.com/sitemap.xml",
      "https://www.nykaafashion.com/sitemap-v2/sitemap-index.xml",
    ];

    // Domain-specific product patterns
    this.domainConfig = {
      "myntra.com": {
        productPatterns: [
          /\/p\/[a-zA-Z0-9\-_]+/,
          /\/product\/[a-zA-Z0-9\-_]+/,
          /\/[a-zA-Z0-9\-_]+\/buy$/,
        ],
        excludedPatterns: [/wishlist|cart|account|login/],
      },
      "virgio.com": {
        productPatterns: [/\/products\/[a-zA-Z0-9\-_]+/],
        excludedPatterns: [/collections|category/],
      },
      "tatacliq.com": {
        productPatterns: [/\/p-mp[a-zA-Z0-9]+/],
        excludedPatterns: [/wishlist|cart|account/],
      },
      "nykaafashion.com": {
        productPatterns: [
          /\/p\/[a-zA-Z0-9\-_]+/,
          /\/product-details\/[a-zA-Z0-9\-_]+/,
        ],
        excludedPatterns: [/search|collections/],
        categoryPatterns: [
          /\/clothing\/|\/footwear\/|\/accessories\/|\/jewellery\//,
        ],
        altDomain: "intl.nykaafashion.com",
      },
      "intl.nykaafashion.com": {
        productPatterns: [
          /\/p\/[a-zA-Z0-9\-_]+/,
          /\/product-details\/[a-zA-Z0-9\-_]+/,
        ],
        categoryPatterns: [
          /\/clothing\/|\/footwear\/|\/accessories\/|\/jewellery\//,
        ],
      },
      "westside.com": {
        productPatterns: [/\/products\/[a-zA-Z0-9\-_]+/],
        excludedPatterns: [/collections|pages/],
      },
    };

    // Generic product patterns
    this.genericProductPatterns = [
      /\/products\/[a-zA-Z0-9\-_]+/,
      /\/apparel\/[a-zA-Z0-9\-_]+/,
      /\/p-[a-zA-Z0-9]+/,
      /\/buy[a-zA-Z0-9]+/,
      /\/p-mp[a-zA-Z0-9]+/,
      /\/p\/[a-zA-Z0-9\-_]+/,
      /\/item\/[a-zA-Z0-9\-_]+/,
      /\/items\/[a-zA-Z0-9\-_]+/,
      /\/pdp\/[a-zA-Z0-9\-_]+/,
      /\/product\/[a-zA-Z0-9\-_]+/,
      /\/product-details\/[a-zA-Z0-9\-_]+/,
      /\/women's-clothing\/[a-zA-Z0-9\-_]+/,
      /\/men's-clothing\/[a-zA-Z0-9\-_]+/,
      /\/[a-zA-Z0-9\-_]+\/buy$/,
    ];

    // User agents to rotate
    this.userAgents = [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    ];

    // File types to skip
    this.skipExtensions = [
      ".png",
      ".jpeg",
      ".jpg",
      ".gif",
      ".pdf",
      ".svg",
      ".webp",
      ".css",
      ".js",
      ".mp3",
      ".mp4",
      ".mov",
      ".zip",
      ".rar",
    ];

    // Results storage
    this.productUrls = {};
    this.visitedUrls = new Set();

    // Max crawl depth
    this.maxDepth = 10;
  }

  // Simple storeProductUrl without checkpoint logic
  storeProductUrl(urlStr, domain) {
    if (!this.productUrls[domain]) {
      this.productUrls[domain] = new Set();
    }
    this.productUrls[domain].add(urlStr);
    console.log(`Found product: ${urlStr}`);
  }

  // Get total count
  getTotalUrlCount() {
    let count = 0;
    for (const urls of Object.values(this.productUrls)) {
      count += urls.size;
    }
    return count;
  }

  // Get random user agent
  getRandomUserAgent() {
    return this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
  }

  // Get headers for requests
  getHeaders() {
    return {
      "User-Agent": this.getRandomUserAgent(),
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      Referer: "https://www.google.com/",
    };
  }

  // Parse domain from URL
  getDomain(urlStr) {
    const parsedUrl = new URL(urlStr);
    return parsedUrl.hostname.replace("www.", "");
  }

  // Check if URL is a product
  isProductUrl(urlStr, domain) {
    // Skip if already processed
    if (this.productUrls[domain] && this.productUrls[domain].has(urlStr)) {
      return false;
    }

    // Skip image and document files
    try {
      const urlObj = new URL(urlStr);
      const path = urlObj.pathname.toLowerCase();

      // Check if the path ends with any of the skip extensions
      if (this.skipExtensions.some((ext) => path.endsWith(ext))) {
        return false;
      }
    } catch (e) {
      // If URL parsing fails, use the simple includes method as fallback
      if (
        this.skipExtensions.some((ext) => urlStr.toLowerCase().endsWith(ext))
      ) {
        return false;
      }
    }

    // Domain-specific checks
    if (this.domainConfig[domain]) {
      const config = this.domainConfig[domain];

      // Check exclusions
      if (
        config.excludedPatterns &&
        config.excludedPatterns.some((pattern) => pattern.test(urlStr))
      ) {
        return false;
      }

      // Check product patterns
      if (
        config.productPatterns &&
        config.productPatterns.some((pattern) => pattern.test(urlStr))
      ) {
        return true;
      }
    }

    // Generic patterns
    return this.genericProductPatterns.some((pattern) => pattern.test(urlStr));
  }

  // Parse robots.txt
  async parseRobots(robotsUrl) {
    try {
      const domain = this.getDomain(robotsUrl);
      console.log(`Parsing robots.txt for ${domain}`);

      const response = await axios.get(robotsUrl, {
        headers: this.getHeaders(),
        timeout: 10000,
      });

      const robotsTxt = response.data;

      // Look for sitemaps
      const sitemapMatches = robotsTxt.match(/Sitemap:\s*(.*)/gi);
      if (sitemapMatches) {
        for (const match of sitemapMatches) {
          const sitemapUrl = match.split("Sitemap:")[1].trim();
          console.log(`Found sitemap in robots.txt: ${sitemapUrl}`);
          await this.parseSitemap(sitemapUrl);
        }
      }

      // For Nykaa, look for category paths
      if (domain.includes("nykaafashion.com")) {
        const allowMatches = robotsTxt.match(/Allow:\s*(.*)/gi);
        if (allowMatches) {
          for (const match of allowMatches) {
            const path = match.split("Allow:")[1].trim();
            const config = this.domainConfig[domain];

            if (
              config &&
              config.categoryPatterns &&
              config.categoryPatterns.some((pattern) => pattern.test(path))
            ) {
              const fullUrl = `https://${domain}${path}`;
              console.log(`Found category path in robots.txt: ${fullUrl}`);
              await this.crawlPage(fullUrl, 0);
            }
          }
        }
      }
    } catch (error) {
      console.error(`Error parsing robots.txt ${robotsUrl}: ${error.message}`);
    }
  }

  // Parse sitemap method
  async parseSitemap(sitemapUrl) {
    try {
      console.log(`Parsing sitemap: ${sitemapUrl}`);
      const domain = this.getDomain(sitemapUrl);

      // Configure request
      const options = {
        headers: this.getHeaders(),
        timeout: 15000,
        responseType: "arraybuffer", // Handle binary data like gzip
      };

      const response = await axios.get(sitemapUrl, options);

      // Check if the content is gzipped
      let xmlData;
      if (sitemapUrl.endsWith(".gz")) {
        // Decompress gzipped content
        xmlData = zlib.gunzipSync(response.data).toString();
        console.log(`Decompressed gzipped sitemap: ${sitemapUrl}`);
      } else {
        // Regular XML content
        xmlData = response.data.toString();
      }

      // Parse XML
      const parser = new xmlParser.XMLParser();
      const result = parser.parse(xmlData);

      // Handle sitemap index (collection of sitemaps)
      if (result.sitemapindex) {
        console.log(
          `Found sitemap index with ${
            result.sitemapindex.sitemap?.length || 0
          } sitemaps`
        );
        const sitemaps = Array.isArray(result.sitemapindex.sitemap)
          ? result.sitemapindex.sitemap
          : [result.sitemapindex.sitemap];

        for (const sitemap of sitemaps) {
          if (sitemap && sitemap.loc) {
            await this.parseSitemap(sitemap.loc);
          }
        }
      }
      // Handle URL set (actual URLs)
      else if (result.urlset) {
        const urls = Array.isArray(result.urlset.url)
          ? result.urlset.url
          : [result.urlset.url];

        console.log(`Found ${urls.length} URLs in sitemap`);

        for (const item of urls) {
          if (item && item.loc) {
            if (this.isProductUrl(item.loc, domain)) {
              this.storeProductUrl(item.loc, domain);
            }
          }
        }
      }
    } catch (error) {
      console.error(`Error parsing sitemap ${sitemapUrl}: ${error.message}`);
    }
  }

  // Crawl a single page
  async crawlPage(pageUrl, depth) {
    // Skip if already visited or exceeds max depth
    if (
      this.visitedUrls.has(pageUrl) ||
      depth > this.maxDepth ||
      this.skipExtensions.some((ext) => {
        try {
          const urlObj = new URL(pageUrl);
          const path = urlObj.pathname.toLowerCase();
          return path.endsWith(ext);
        } catch (e) {
          return pageUrl.toLowerCase().endsWith(ext);
        }
      })
    ) {
      return;
    }

    this.visitedUrls.add(pageUrl);
    const domain = this.getDomain(pageUrl);

    try {
      console.log(`Crawling: ${pageUrl} (depth: ${depth})`);

      // Try to handle 403 errors for Nykaa
      const config = {
        headers: this.getHeaders(),
        timeout: 15000,
        validateStatus: (status) =>
          (status >= 200 && status < 300) || status === 403,
      };

      const response = await axios.get(pageUrl, config);

      // Handle Nykaa blocking
      if (response.status === 403 && pageUrl.includes("nykaafashion.com")) {
        const config = this.domainConfig["nykaafashion.com"];
        if (config && config.altDomain) {
          const altUrl = pageUrl.replace("nykaafashion.com", config.altDomain);
          console.log(`Trying alternative domain for blocked Nykaa: ${altUrl}`);
          await this.crawlPage(altUrl, depth);
          return;
        }
      }

      // Safe HTML parsing
      let $;
      try {
        // Handle different response data formats
        let html = "";
        if (typeof response.data === "string") {
          html = response.data;
        } else if (response.data instanceof Buffer) {
          html = response.data.toString();
        } else if (response.data && typeof response.data === "object") {
          // For JSON responses that might occur
          html = JSON.stringify(response.data);
        }

        // Load HTML with proper error handling
        $ = cheerio.load(html);

        // Extract all links
        const links = $("a")
          .map((i, el) => $(el).attr("href"))
          .get()
          .filter((href) => href);

        console.log(`Found ${links.length} links on ${pageUrl}`);

        // Rest of your link processing code...
      } catch (cheerioError) {
        console.error(
          `Error parsing HTML from ${pageUrl}: ${cheerioError.message}`
        );
        return; // Skip this page on error
      }

      // Process links
      for (const link of links) {
        try {
          const absoluteUrl = new URL(link, pageUrl).href;
          const linkDomain = this.getDomain(absoluteUrl);

          if (!this.allowedDomains.includes(linkDomain)) {
            continue;
          }

          if (this.isProductUrl(absoluteUrl, linkDomain)) {
            this.storeProductUrl(absoluteUrl, linkDomain);
          } else if (depth < this.maxDepth) {
            // Follow link
            await this.crawlPage(absoluteUrl, depth + 1);
          }
        } catch (urlError) {
          // Invalid URL, just skip it
        }
      }

      // Look for embedded JSON
      await this.extractJsonData($, pageUrl, domain);
    } catch (error) {
      console.error(`Error crawling ${pageUrl}: ${error.message}`);
    }
  }

  // Extract JSON data from script tags
  async extractJsonData($, pageUrl, domain) {
    try {
      const scriptTags = $(
        'script[type="application/json"], script:contains("__PRELOADED_STATE__"), script:contains("__INITIAL_STATE__")'
      );

      scriptTags.each((i, el) => {
        try {
          const script = $(el).html();
          let jsonData;

          if (script.includes("__PRELOADED_STATE__")) {
            const jsonStr = script
              .split("__PRELOADED_STATE__ = ")[1]
              .split(";</script>")[0];
            jsonData = JSON.parse(jsonStr);
          } else if (script.includes("__INITIAL_STATE__")) {
            const jsonStr = script
              .split("__INITIAL_STATE__ = ")[1]
              .split(";</script>")[0];
            jsonData = JSON.parse(jsonStr);
          } else {
            jsonData = JSON.parse(script);
          }

          this.searchJsonForUrls(jsonData, pageUrl, domain);
        } catch (e) {
          // Ignore JSON parsing errors
        }
      });
    } catch (error) {
      console.error(
        `Error extracting JSON data from ${pageUrl}: ${error.message}`
      );
    }
  }

  // Search JSON for product URLs
  searchJsonForUrls(data, pageUrl, domain) {
    const searchObject = (obj, path = []) => {
      if (!obj || typeof obj !== "object") return;

      if (Array.isArray(obj)) {
        obj.forEach((item) => searchObject(item, path));
        return;
      }

      Object.entries(obj).forEach(([key, value]) => {
        if (
          ["productUrl", "product_url", "url", "href", "link"].includes(key) &&
          typeof value === "string"
        ) {
          try {
            const absoluteUrl = new URL(value, pageUrl).href;
            if (this.isProductUrl(absoluteUrl, domain)) {
              this.storeProductUrl(absoluteUrl, domain);
            }
          } catch (e) {
            // Invalid URL, skip
          }
        }
        if (value && typeof value === "object") {
          searchObject(value, [...path, key]);
        }
      });
    };

    searchObject(data);
  }

  // Save results to file
  async saveResults() {
    try {
      console.log("Saving results...");

      // Create all_products.json
      const allProducts = [];
      let totalCount = 0;

      for (const [domain, urls] of Object.entries(this.productUrls)) {
        const domainUrls = [...urls];
        totalCount += domainUrls.length;

        // Add to all products
        domainUrls.forEach((url) => {
          allProducts.push({
            domain,
            product_url: url,
          });
        });

        // Create domain-specific file
        const filename = `${domain.replace(/\./g, "_")}_products.json`;
        const domainData = domainUrls.map((url) => ({ product_url: url }));

        await fs.writeFile(filename, JSON.stringify(domainData, null, 2));

        console.log(
          `Saved ${domainUrls.length} URLs for ${domain} to ${filename}`
        );
      }

      // Save all products
      await fs.writeFile(
        "all_products.json",
        JSON.stringify(allProducts, null, 2)
      );

      console.log(
        `Total of ${totalCount} product URLs saved to all_products.json`
      );
    } catch (error) {
      console.error(`Error saving results: ${error.message}`);
    }
  }

  async crawl() {
    console.log("Starting crawler...");

    try {
      // Initialize storage
      for (const domain of this.allowedDomains) {
        this.productUrls[domain] = new Set();
      }

      // Step 1: Check robots.txt files
      for (const robotsUrl of this.robotsUrls) {
        await this.parseRobots(robotsUrl);
      }

      // Step 2: Parse known sitemaps
      for (const sitemapUrl of this.sitemapUrls) {
        await this.parseSitemap(sitemapUrl);
      }

      // Step 3: Regular crawling
      for (const startUrl of this.startUrls) {
        await this.crawlPage(startUrl, 0);
      }

      // Save results
      await this.saveResults();
    } catch (error) {
      console.error(`Crawler error: ${error.message}`);
    }

    console.log("Crawling completed.");
  }
}
const crawler = new ProductCrawler();
crawler.crawl().then(() => {
  console.log("Crawling finished.");
});
module.exports = { ProductCrawler };
