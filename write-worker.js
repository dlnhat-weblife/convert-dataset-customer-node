const { parentPort, workerData } = require("worker_threads");
const fs = require("fs");

const { data, startPosition } = workerData;

console.log({
  startPosition,
  first: data[0],
});

const writer = fs.createWriteStream("./output.csv", {
  flags: "a",
  start: startPosition,
});

data.forEach((row) => {
  writer.write(row.join(",") + "\n");
});

writer.end();
parentPort.postMessage({ done: true });
/**
 * city: 県 | 都 | 府 | 道
 * province: 市 | 区
 */
