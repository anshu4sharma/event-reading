import { ethers } from "ethers";
import Redis from "ioredis";
import abi from "./factory";
import PQueue from "p-queue";

const redis = new Redis({
  host: "localhost",
  port: 6379,
  retryStrategy: (times) => Math.min(times * 50, 2000),
  maxRetriesPerRequest: 3,
});

const providerUrls = [
  "https://data-seed-prebsc-1-s1.binance.org:8545/",
  "https://data-seed-prebsc-2-s1.binance.org:8545/",
  "https://data-seed-prebsc-1-s2.binance.org:8545/",
];

let currentProviderIndex = 0;
let provider = new ethers.JsonRpcProvider(providerUrls[currentProviderIndex]);

const CONTRACT_ADDRESS = "0xE7E4A7F78cd780De96617d53066f1764FA4bC26C";
const contract = new ethers.Contract(CONTRACT_ADDRESS, abi, provider);

const START_BLOCK = 48383381;
const MAX_RETRIES = 3;
const MAX_CONCURRENT_REQUESTS = 20;
const PARALLEL_BLOCKS = 10;

const TRACKED_EVENTS = ["Create", "Transfer", "Buy"];
const queue = new PQueue({ concurrency: PARALLEL_BLOCKS });

async function getLastProcessedBlock(): Promise<number> {
  try {
    const lastBlock = await redis.get("lastProcessedBlock");
    console.log(lastBlock, "lastBlock");

    return lastBlock ? parseInt(lastBlock) : START_BLOCK;
  } catch (error) {
    console.error("Redis error getting last block:", error);
    return START_BLOCK;
  }
}

async function saveLastProcessedBlock(blockNumber: number) {
  try {
    await redis.set("lastProcessedBlock", blockNumber);
  } catch (error) {
    console.error("Redis error saving last block:", error);
  }
}

async function switchProvider() {
  currentProviderIndex = (currentProviderIndex + 1) % providerUrls.length;
  provider = new ethers.JsonRpcProvider(providerUrls[currentProviderIndex]);
  contract.connect(provider);
  console.log(`🔄 Switched to provider ${providerUrls[currentProviderIndex]}`);
}

async function processBlock(blockNumber: number): Promise<void> {
  let retries = 0;
  while (retries < MAX_RETRIES) {
    try {
      console.log(`Processing block ${blockNumber}...`);
      const block = await provider.getBlock(blockNumber, true);
      if (!block) {
        console.warn(`Block ${blockNumber} not found, skipping...`);
        return;
      }

      const relevantTxs = await Promise.all(
        block.transactions.map(async (txHash: string) => {
          const tx = await provider.getTransaction(txHash);
          return tx && tx.to?.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()
            ? tx
            : null;
        })
      );
      if (relevantTxs.length > 0) {
        console.log(
          `Found ${relevantTxs.length} relevant transactions in block ${blockNumber}`
        );
      }

      const chunkSize = MAX_CONCURRENT_REQUESTS;
      for (let i = 0; i < relevantTxs.length; i += chunkSize) {
        const chunk = relevantTxs.slice(i, i + chunkSize);
        await Promise.all(
          chunk.filter(tx => tx !== null).map(async (tx: any) => {
            const txHash = tx.hash;
            try {
              const receipt = await provider.getTransactionReceipt(txHash);
              if (!receipt?.logs.length) return;

              const contractLogs = receipt.logs.filter(
                (log) =>
                  log.address.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()
              );

              for (const log of contractLogs) {
                try {
                  const parsedLog = contract.interface.parseLog(log);
                  console.log(parsedLog, "parsedLog");

                  if (parsedLog && TRACKED_EVENTS.includes(parsedLog.name)) {
                    console.log(
                      `📢 Event: ${
                        parsedLog.name
                      } | Block: ${blockNumber} | TX: ${txHash.slice(0, 10)}...`
                    );
                    console.log("Event data:", parsedLog.args);
                  }
                } catch (err) {
                  console.warn(`Skipping unknown event in tx ${txHash}`);
                }
              }
            } catch (error) {
              console.warn(`Error processing tx ${txHash}:`, error);
            }
          })
        );
      }

      await saveLastProcessedBlock(blockNumber);
      console.log(`✅ Completed block ${blockNumber}`);
      return;
    } catch (error) {
      retries++;
      console.error(
        `Block ${blockNumber} failed (attempt ${retries}/${MAX_RETRIES}):`,
        error
      );
      if (retries >= MAX_RETRIES) {
        console.error(`⚠️ Switching provider due to repeated failures.`);
        await switchProvider();
      }
      await new Promise((resolve) => setTimeout(resolve, retries * 1000));
    }
  }
}

async function processBlocks() {
  let lastProcessedBlock = await getLastProcessedBlock();
  const latestBlock = await provider.getBlockNumber();

  console.log(
    `Starting block sync from ${lastProcessedBlock} to ${latestBlock}...`
  );

  const blockNumbers = Array.from(
    { length: latestBlock - lastProcessedBlock + 1 },
    (_, i) => lastProcessedBlock + i
  );

  await queue.addAll(blockNumbers.map((block) => () => processBlock(block)));
  console.log("✅ Event processing up to date!");
}

process.on("SIGINT", async () => {
  console.log("Gracefully shutting down...");
  await redis.quit();
  process.exit(0);
});

processBlocks().catch(async (error) => {
  console.error("Fatal error:", error);
  await redis.quit();
  process.exit(1);
});
