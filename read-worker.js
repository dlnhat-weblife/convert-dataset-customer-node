const { parentPort } = require("worker_threads");
const csv = require("csv-parser");
const fs = require("fs");

const path = "./cust.csv";
const rs = [];

fs.createReadStream(path)
  .pipe(csv())
  .on("data", async (row) => {
    if (rs.length === 1000000) {
      console.log(row);
      parentPort.postMessage({ rs });
      rs.length = 0;
    }
    let randomPassword = Math.random().toString(36).substring(5);
    while (randomPassword.length < 8) {
      randomPassword += Math.random().toString(36).substring(3, 4);
    }
    let phoneNumber = String(row.TEL_NO)
      .replace(/[-ー ]/g, "")
      .replace("+81", "0");
    if (phoneNumber.startsWith("81")) {
      phoneNumber = phoneNumber.replace("81", "0");
    }
    const zipCode = String(row.ADDRESS_ZIP).replace(/[-ー ]/g, "");
    const addressFull = row.ADDRESS_FULL;
    let city, province, address, addressWithoutCity;

    /**
     * city: 郡 | 市 | 区 | 町 | 村
     */
    if (addressFull.includes("郡")) {
      city = addressFull.split("郡")[0] + "郡";
    } else if (addressFull.includes("市")) {
      city = addressFull.split("市")[0] + "市";
    } else if (addressFull.includes("区")) {
      city = addressFull.split("区")[0] + "区";
    } else if (addressFull.includes("町")) {
      city = addressFull.split("町")[0] + "町";
    } else if (addressFull.includes("村")) {
      city = addressFull.split("村")[0] + "村";
    }

    rs.push([
      5, //row["COMPANY_CD"]
      "MERGE",
      "FALSE",
      randomPassword,
      randomPassword,
      row.CUST_ID_CRM, // expect to be changed in the future
      row.CUST_ID_CRM, // expect to be changed in the future
      row.CUST_BARCD,
      row.CUST_BARCD_SP,
      row.LINE_ADDRESS,
      row.EMAIL_ADDRESS,
      row.CUSTNAME_FULL,
      row.CUSTNAME_LAST,
      row.CUSTNAME_FIRST,
      row.CUSTNAME_FULLKANA,
      row.CUSTNAME_LASTKANA,
      row.CUSTNAME_FIRSTKANA,
      row.GENDER,
      row.BIRTHDAY,
      phoneNumber,
      "JP",
      zipCode,
      "", // province
      "", // city
      "", // address
      row.ADDRESS_FULL,
      row.REGIST_DATE_LINE,
      row.REGIST_DATE_EC,
      row.DELETE_FLG,
      row.DELETE_DATE,
      row.DMAIL_FLG4EC,
      row.DMAIL_FLG4EC_SWITCH_DATE,
      row.DMAIL_FLG4LINE,
      row.DMAIL_FLG4LINE_SWITCH_DATE,
      row.DMAIL_FLG4PAPER,
      row.DMAIL_FLG4PAPER_SWITCH_DATE,
      row.DCALL_FLG,
      row.DCALL_FLG4EC_SWITCH_DATE,
      row.PURCHASE_DATE_1ST,
      row.POINT_RANK,
      row.POINT_BALANCE,
      row.POINT_EXPIRY_DATE,
      row.POINT_REACHING_EXPIRATION,
      row.CUST_RANK,
      row.CUSTATTRIB_TAGS,
      "", // note1
      "", // note2,
      row.LINE_REGIST_DATE,
      row.LINE_UPDATE_DATE,
      row.CUSTATTRIB_UPDATE_DATE,
    ]);
  })
  .on("end", () => {
    console.log("CSV file successfully processed");
    parentPort.postMessage({ rs });
  });
