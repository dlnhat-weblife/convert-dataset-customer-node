const { Worker } = require("worker_threads");
const fs = require("fs");

let length = 2;

const writer = fs.createWriteStream("./output.csv", {
  flags: "a",
  start: 1,
});

writer.write(
  [
    "Metafield: account.company_code [number_integer]",
    "Command",
    "Send Welcome Email",
    "Password",
    "Metafield: account.pwd [single_line_text_field]",
    "Metafield: account.lid [single_line_text_field]",
    "Metafield: account.sp-id [single_line_text_field]",
    "Metafield: account.barcode [single_line_text_field]",
    "Metafield: account.sp-barcode [single_line_text_field]",
    "Metafield: account.line-id [single_line_text_field]",
    "Email",
    "Metafield: account.name [single_line_text_field]",
    "Last Name",
    "First Name",
    "Metafield: account.kana [single_line_text_field]",
    "Metafield: account.kana-last [single_line_text_field]",
    "Metafield: account.kana-first [single_line_text_field]",
    "Metafield: account.gender [single_line_text_field]",
    "Metafield: account.birthday [single_line_text_field]",
    "Address Phone",
    "Address Country Code",
    "Address Zip",
    "Address Province",
    "Address City",
    "Address Line 1",
    "Metafield: account.address [single_line_text_field]",
    "Metafield: account.register-line1 [single_line_text_field]",
    "Metafield: account.register-ec [single_line_text_field]",
    "Metafield: account.delete-flg [single_line_text_field]",
    "Metafield: account.delete-date [single_line_text_field]",
    "Metafield: account.newsletter-flg [single_line_text_field]",
    "Metafield: account.newsletter-edit-date [single_line_text_field]",
    "Metafield: account.line-flg [single_line_text_field]",
    "Metafield: account.line-edit-date [single_line_text_field]",
    "Metafield: account.dm-flg [single_line_text_field]",
    "Metafield: account.dm-edit-date [single_line_text_field]",
    "Metafield: account.dcall-flg [single_line_text_field]",
    "Metafield: account.dcall-edit-date [single_line_text_field]",
    "Metafield: account.first-order-date [single_line_text_field]",
    "Metafield: account.stage [single_line_text_field]",
    "Metafield: account.mile [single_line_text_field]",
    "Metafield: account.mile-expiry-date [single_line_text_field]",
    "Metafield: account.mile-reaching-expiration [single_line_text_field]",
    "Metafield: account.rank [single_line_text_field]",
    "Tags",
    "Metafield: account.note1 [multi_line_text_field]",
    "Metafield: account.note2 [multi_line_text_field]",
    "Metafield: account.register-line2 [single_line_text_field]",
    "Metafield: account.edit-line [single_line_text_field]",
    "Metafield: account.last-update [single_line_text_field]",
  ].join(",") + "\n"
);
writer.end();

const readCSV = () => {
  const worker = new Worker("./read-worker.js");
  worker.on("message", (data) => {
    writeToFile(data.rs);
    length += data.rs.length;
  });
  worker.on("error", (err) => console.error(err));
  worker.on("exit", (code) => {
    console.log("read worker exit", code);
    if (code !== 0) console.error(new Error(`stopped with ${code} exit code`));
  });
};

const writeToFile = (rs) => {
  const worker = new Worker("./write-worker.js", {
    workerData: {
      data: rs,
      startPosition: length,
    },
  });

  worker.on("message", (data) => {
    console.log("write done", data);
  });
  worker.on("error", (err) => {
    console.error(err);
  });
  worker.on("exit", (code) => {
    console.log("write worker exit", code);
    if (code !== 0) console.error(new Error(`stopped with ${code} exit code`));
  });
};

readCSV();
