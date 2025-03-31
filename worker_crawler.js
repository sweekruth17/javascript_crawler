const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");
const os = require("os");
const path = require("path");
const fs = require("fs").promises;
const http = require("http");
const https = require("https");
const axios = require("axios");
const cheerio = require("cheerio");
const { URL } = require("url");

// Number of workers (adjust based on your system)
const NUM_WORKERS = Math.max(1, os.cpus().length - 1);

if (isMainThread) {
  // Main thread code
  class CrawlerCoordinator {
    constructor() {
      this.workers = [];
      this.results = {};
      this.completedWorkers = 0;
      this.startTime = Date.now();
    }

    async startCrawling() {
      console.log(`Starting crawler with ${NUM_WORKERS} worker threads...`);

      // Define work to distribute
      const domains = [
        "virgio.com",
        "tatacliq.com",
        // "intl.nykaafashion.com",
        "westside.com",
        // "myntra.com",
        "nykaafashion.com"
      ];

      // Distribute domains among workers
      const domainsPerWorker = Math.ceil(domains.length / NUM_WORKERS);

      for (let i = 0; i < NUM_WORKERS; i++) {
        const workerDomains = domains.slice(
          i * domainsPerWorker,
          Math.min((i + 1) * domainsPerWorker, domains.length)
        );

        if (workerDomains.length === 0) continue;

        const worker = new Worker(__filename, {
          workerData: {
            workerId: i,
            domains: workerDomains,
          },
        });

        worker.on("message", (message) => this.handleWorkerMessage(message, i));
        worker.on("error", (error) =>
          console.error(`Worker ${i} error:`, error)
        );
        worker.on("exit", (code) => this.handleWorkerExit(i, code));

        this.workers.push(worker);
      }
    }

    handleWorkerMessage(message, workerId) {
      if (message.type === "result") {
        // Handle final results
        this.results[workerId] = message.data;
        console.log(`Received final results from worker ${workerId}`);
      } else if (message.type === "log") {
        console.log(`[Worker ${workerId}] ${message.data}`);
      }
    }

    async handleWorkerExit(workerId, exitCode) {
      console.log(`Worker ${workerId} exited with code ${exitCode}`);
      this.completedWorkers++;

      if (this.completedWorkers === this.workers.length) {
        await this.mergeResults();
      }
    }

    async mergeResults() {
      console.log("All workers completed, merging results...");

      const allProducts = [];
      const mergedProductUrls = {};
      let totalCount = 0;

      // Merge results from all workers
      for (const workerResults of Object.values(this.results)) {
        for (const [domain, urls] of Object.entries(workerResults)) {
          if (!mergedProductUrls[domain]) {
            mergedProductUrls[domain] = new Set();
          }

          for (const url of urls) {
            mergedProductUrls[domain].add(url);
          }
        }
      }

      // Process merged results
      for (const [domain, urls] of Object.entries(mergedProductUrls)) {
        const domainUrls = [...urls];
        totalCount += domainUrls.length;

        // Add to all products
        domainUrls.forEach((url) => {
          allProducts.push({
            domain,
            product_url: url,
          });
        });

        // Save domain-specific results
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

      const duration = (Date.now() - this.startTime) / 1000;
      console.log(`Crawling completed in ${duration.toFixed(2)} seconds.`);
      console.log(
        `Total of ${totalCount} product URLs saved to all_products.json`
      );
    }
  }

  // Start the crawler coordinator
  const coordinator = new CrawlerCoordinator();
  coordinator.startCrawling().catch(console.error);
} else {
  // Worker thread code
  const { ProductCrawler } = require("./script");

  class WorkerCrawler extends ProductCrawler {
    constructor(workerId, domains) {
      super();
      this.workerId = workerId;

      // Filter to only assigned domains
      this.allowedDomains = domains;
      this.startUrls = this.startUrls.filter((url) => {
        const domain = this.getDomain(url);
        return domains.includes(domain);
      });
      this.robotsUrls = this.robotsUrls.filter((url) => {
        const domain = this.getDomain(url);
        return domains.includes(domain);
      });
      this.sitemapUrls = this.sitemapUrls.filter((url) => {
        const domain = this.getDomain(url);
        return domains.includes(domain);
      });

      // Performance enhancements
      this.maxConcurrentRequests = 25;
      this.activeRequests = 0;
      this.requestQueue = [];
      this.requestDelay = 100; // ms between requests to same domain
      this.lastRequestTime = {};

      // Connection pooling for faster HTTP requests
      this.httpAgent = new http.Agent({
        keepAlive: true,
        maxSockets: 50,
        maxFreeSockets: 10,
        timeout: 60000,
      });

      this.httpsAgent = new https.Agent({
        keepAlive: true,
        maxSockets: 50,
        maxFreeSockets: 10,
        timeout: 60000,
      });

      // Results tracking
      this.productUrls = {};
      for (const domain of this.allowedDomains) {
        this.productUrls[domain] = new Set();
      }

      // Override console log to send to main thread
      this.log = (message) => {
        parentPort.postMessage({
          type: "log",
          data: message,
        });
      };
    }

    // Override to use connection pooling and concurrency control
    getRequestOptions(url) {
      const isHttps = url.startsWith("https://");
      return {
        headers: this.getHeaders(),
        agent: isHttps ? this.httpsAgent : this.httpAgent,
        timeout: 15000,
        responseType: "arraybuffer",
        decompress: true,
      };
    }

    // Add this method to manage request concurrency
    async scheduleRequest(fn) {
      if (this.activeRequests >= this.maxConcurrentRequests) {
        // Queue the request for later execution
        return new Promise((resolve) => {
          this.requestQueue.push(async () => {
            const result = await fn();
            resolve(result);
          });
        });
      }

      // Execute the request now
      this.activeRequests++;
      try {
        return await fn();
      } finally {
        this.activeRequests--;

        // Process next queued request if any
        if (this.requestQueue.length > 0) {
          const nextRequest = this.requestQueue.shift();
          nextRequest().catch((e) =>
            this.log(`Queued request error: ${e.message}`)
          );
        }
      }
    }

    // Add domain-specific rate limiting
    async throttleRequest(domain, fn) {
      const now = Date.now();
      const lastRequest = this.lastRequestTime[domain] || 0;
      const elapsed = now - lastRequest;

      if (elapsed < this.requestDelay) {
        await new Promise((resolve) =>
          setTimeout(resolve, this.requestDelay - elapsed)
        );
      }

      this.lastRequestTime[domain] = Date.now();
      return fn();
    }

    // Override storeProductUrl to use worker logging
    storeProductUrl(urlStr, domain) {
      if (!this.productUrls[domain]) {
        this.productUrls[domain] = new Set();
      }
      this.productUrls[domain].add(urlStr);
      this.log(`Found product: ${urlStr}`);
    }

    // Override crawlPage to use concurrency control
    async crawlPage(pageUrl, depth) {
      const domain = this.getDomain(pageUrl);

      return this.scheduleRequest(() =>
        this.throttleRequest(domain, async () => {
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
                return pageUrl.toLowerCase().includes(ext);
              }
            })
          ) {
            return;
          }

          this.visitedUrls.add(pageUrl);

          try {
            // Get and process the page
            const options = this.getRequestOptions(pageUrl);
            const response = await axios.get(pageUrl, options);

            // Check if this is a product URL
            if (this.isProductUrl(pageUrl, domain)) {
              this.storeProductUrl(pageUrl, domain);
            }

            // Only continue crawling if we haven't hit max depth
            if (depth < this.maxDepth) {
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

              // Process each link
              for (const link of links) {
                try {
                  // Normalize URL
                  const absoluteUrl = new URL(link, pageUrl).href;
                  const linkDomain = this.getDomain(absoluteUrl);

                  // Only follow links within allowed domains
                  if (this.allowedDomains.includes(linkDomain)) {
                    // Skip excluded patterns
                    if (
                      this.domainConfig[linkDomain] &&
                      this.domainConfig[linkDomain].excludedPatterns
                    ) {
                      const excluded = this.domainConfig[
                        linkDomain
                      ].excludedPatterns.some((pattern) =>
                        pattern.test(absoluteUrl)
                      );
                      if (excluded) continue;
                    }

                    // Crawl the linked page
                    await this.crawlPage(absoluteUrl, depth + 1);
                  }
                } catch (urlError) {
                  // Invalid URL, just skip it
                }
              }
            }

            // Look for embedded JSON
            await this.extractJsonData($, pageUrl, domain);
          } catch (error) {
            // More detailed error handling
            if (error.response) {
              this.log(
                `Error crawling ${pageUrl}: HTTP ${error.response.status}`
              );
            } else if (error.request) {
              this.log(`Network error for ${pageUrl}: ${error.message}`);
            } else {
              this.log(`Error with ${pageUrl}: ${error.message}`);
            }
          }
        })
      );
    }

    async crawl() {
      this.log(
        `Worker ${
          this.workerId
        } starting crawler for domains: ${this.allowedDomains.join(", ")}`
      );

      try {
        // Step 1-3 with parallel execution
        await Promise.all(
          this.robotsUrls.map((robotsUrl) => this.parseRobots(robotsUrl))
        );

        await Promise.all(
          this.sitemapUrls.map((sitemapUrl) => this.parseSitemap(sitemapUrl))
        );

        await Promise.all(
          this.startUrls.map((startUrl) => this.crawlPage(startUrl, 0))
        );

        // Return results to main thread
        const serializableResults = {};
        for (const [domain, urlSet] of Object.entries(this.productUrls)) {
          serializableResults[domain] = [...urlSet];
        }

        parentPort.postMessage({
          type: "result",
          data: serializableResults,
        });
      } catch (error) {
        this.log(`Worker crawler error: ${error.message}`);
      } finally {
        // Clean up resources
        this.httpAgent.destroy();
        this.httpsAgent.destroy();
      }
    }
  }

  const { workerId, domains } = workerData;
  const crawler = new WorkerCrawler(workerId, domains);
  crawler.crawl().catch((error) => {
    console.error(`Worker ${workerId} failed:`, error);
  });
}
