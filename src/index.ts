// Start of Selection
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
  "https://bsc-testnet-rpc.publicnode.com",
  "https://data-seed-prebsc-1-s3.bnbchain.org:8545",
  "https://bsc-testnet.drpc.org",
  "https://bsc-testnet.public.blastapi.io",
  "https://bsc-testnet.public.block-engine.org",
  "https://api.zan.top/bsc-testnet"
];

let currentProviderIndex = 0;
let provider = new ethers.JsonRpcProvider(providerUrls[currentProviderIndex]);

const CONTRACT_ADDRESS = "0xE7E4A7F78cd780De96617d53066f1764FA4bC26C";
const contract = new ethers.Contract(CONTRACT_ADDRESS, abi, provider);

const START_BLOCK = 48395475;
const MAX_RETRIES = 3;
const MAX_CONCURRENT_REQUESTS = 20;
const PARALLEL_BLOCKS = 10;

const TRACKED_EVENTS = ["Create", "Transfer", "Buy"];
const queue = new PQueue({ concurrency: PARALLEL_BLOCKS });

const DEBUG = false;
let rpcCallCount = 0;

function trackRPCCall(method: string) {
  rpcCallCount++;
  if (DEBUG) {
    console.log(`🔄 RPC Call #${rpcCallCount}: ${method}`);
  }
}

async function getLastProcessedBlock(): Promise<number> {
  try {
    const lastBlock = await redis.get("lastProcessedBlock");

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
      if (DEBUG) {
        console.log(`\n🔍 Processing block ${blockNumber}...`);
      }
      trackRPCCall("getBlock");
      const block = await provider.getBlock(blockNumber, true);

      if (!block) {
        console.warn(`⚠️ Block ${blockNumber} not found, skipping...`);
        return;
      }

      if (DEBUG) {
        console.log(
          `📦 Block ${blockNumber} has ${block.transactions.length} total transactions`
        );
      }
      // Fetch full transactions
      const transactions = await Promise.all(
        block.transactions.map((txHash: string) => provider.getTransaction(txHash))
      );

      const relevantTxs = transactions.filter(
        (tx: any) => tx?.to?.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()
      );
      if (DEBUG) {
        console.log(
          `${blockNumber}: ${relevantTxs.length > 0 ? "✅" : "⏭️"} ${relevantTxs.length
          } relevant transactions`
        );
      }

      const chunkSize = MAX_CONCURRENT_REQUESTS;
      for (let i = 0; i < relevantTxs.length; i += chunkSize) {
        const chunk = relevantTxs.slice(i, i + chunkSize);
        await Promise.all(
          chunk.map(async (tx: any) => {
            const txHash = tx.hash;
            try {
              trackRPCCall("getTransactionReceipt");
              const receipt = await provider.getTransactionReceipt(txHash);
              if (!receipt) {
                if (DEBUG)
                  console.log(`⚠️ No receipt for tx ${txHash.slice(0, 10)}...`);
                return;
              }

              const contractLogs = receipt.logs.filter(
                (log) =>
                  log.address.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()
              );

              if (DEBUG) {
                console.log(
                  `📜 TX ${txHash.slice(0, 10)}... has ${contractLogs.length
                  } contract logs`
                );
              }

              for (const log of contractLogs) {
                try {
                  const parsedLog = contract.interface.parseLog(log);
                  console.log(parsedLog, "parsedLog");

                  if (parsedLog && TRACKED_EVENTS.includes(parsedLog.name)) {
                    console.log(
                      `📢 Event: ${parsedLog.name
                      } | Block: ${blockNumber} | TX: ${txHash.slice(0, 10)}...`
                    );
                    console.log("Event data:", parsedLog.args);
                  }
                } catch (err) {
                  console.warn(`Skipping unknown event in tx ${txHash}`);
                }
              }
            } catch (error) {
              console.warn(`❌ Error processing tx ${txHash}:`, error);
            }
          })
        );
      }

      await saveLastProcessedBlock(blockNumber);
      console.log(`✅ Successfully processed block ${blockNumber}.`);
      return;
    } catch (error) {
      retries++;
      console.error(
        `❌ Block ${blockNumber} failed (attempt ${retries}/${MAX_RETRIES}):`,
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
  let lastStatsTime = Date.now();

  while (true) {
    try {
      trackRPCCall("getBlockNumber");
      const latestBlock = await provider.getBlockNumber();

      if (Date.now() - lastStatsTime > 60000) {
        console.log(`\n📊 Stats:`);
        console.log(`Total RPC calls: ${rpcCallCount}`);
        console.log(`Current block: ${lastProcessedBlock}`);
        console.log(`Latest network block: ${latestBlock}`);
        console.log(`Blocks behind: ${latestBlock - lastProcessedBlock}`);
        lastStatsTime = Date.now();
      }

      if (lastProcessedBlock < latestBlock) {
        console.log(
          `Processing blocks from ${lastProcessedBlock + 1
          } to ${latestBlock}...`
        );

        const blockNumbers = Array.from(
          { length: latestBlock - lastProcessedBlock },
          (_, i) => lastProcessedBlock + 1 + i
        );

        await queue.addAll(
          blockNumbers.map((block) => () => processBlock(block))
        );
        lastProcessedBlock = latestBlock;
      } else {
        if (DEBUG) {
          console.log(
            `⏳ Waiting for new blocks... Current: ${lastProcessedBlock}`
          );
        }
      }

      await new Promise((resolve) => setTimeout(resolve, 5000));
    } catch (error) {
      console.error("❌ Error in block processing loop:", error);
      await switchProvider();
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

process.on("SIGINT", async () => {
  console.log("Gracefully shutting down...");
  await queue.clear();
  await queue.onIdle();
  await redis.quit();
  process.exit(0);
});

processBlocks().catch(async (error) => {
  console.error("Fatal error:", error);
  await redis.quit();
  process.exit(1);
});
