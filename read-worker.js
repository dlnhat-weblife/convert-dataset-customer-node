const { parentPort } = require("worker_threads");
const csv = require("csv-parser");
const fs = require("fs");

const path = "./cust.csv";
const rs = [];

const specialProvinces = ["北海道", "東京都", "京都府", "大阪府"];

const specialCities = [
  "大和郡山市",
  "郡山市",
  "市川市",
  "市原市",
  "郡上市",
  "蒲郡市",
  "四日市市",
  "廿日市市",
  "小郡市",
  "野々市市",
  "高市郡",
  "余市郡",
];

const number = new RegExp(/\d/);
const jpNumber = new RegExp(/[０-９]/);

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
    const addressFull = String(row.ADDRESS_FULL);
    let province,
      city,
      address,
      addressWithoutProvince = addressFull;

    if (
      String(addressFull).length &&
      (number.test(addressFull) || jpNumber.test(addressFull))
    ) {
      if (
        // specialProvinces.includes(addressFull)
        specialProvinces.some((p) => addressFull.includes(p))
      ) {
        // const index = specialProvinces.indexOf((p) => addressFull.includes(p));
        const index = specialProvinces.findIndex((p) => addressFull.includes(p));
        province = specialProvinces[index];
        addressWithoutProvince = String(addressFull.replace(province, ""));
      } else {
        const [p, rest] = addressFull.split("県");
        province = p + "県";
        addressWithoutProvince = String(rest);
      }

      if (
        specialCities.some((c) => addressWithoutProvince.includes(c))
      ) {
        const index = specialCities.findIndex((c) => addressWithoutProvince.includes(c));
        city = specialCities[index];
        address = addressWithoutProvince.replace(city, "");
      } else {
        if (addressWithoutProvince.includes("郡")) {
          const [c, rest] = addressWithoutProvince.split("郡");
          city = c + "郡";
          address = rest;
        } else if (addressWithoutProvince.includes("市")) {
          const [c, rest] = addressWithoutProvince.split("市");
          city = c + "市";
          address = rest;
        } else if (addressWithoutProvince.includes("区")) {
          const [c, rest] = addressWithoutProvince.split("区");
          city = c + "区";
          address = rest;
        } else if (addressWithoutProvince.includes("町")) {
          const [c, rest] = addressWithoutProvince.split("町");
          city = c + "町";
          address = rest;
        } else if (addressWithoutProvince.includes("村")) {
          const [c, rest] = addressWithoutProvince.split("村");
          city = c + "村";
          address = rest;
        }
      }
    }

    rs.push([
      5, //row["COMPANY_CD"]
      "MERGE",
      "FALSE",
      randomPassword,
      randomPassword,
      row.CUST_ID_LINE,
      row.CUST_ID_CRM,
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
      province, // province
      city, // city
      address, // address
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
      row.NOTE_BYHQ,
      row.NOTE_BYSTORE,
      row.LINE_REGIST_DATE,
      row.LINE_UPDATE_DATE,
      row.CUSTATTRIB_UPDATE_DATE,
    ]);
  })
  .on("end", () => {
    console.log("CSV file successfully processed");
    parentPort.postMessage({ rs });
  });
