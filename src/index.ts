import Knex from "knex";
import mongoose, { Schema } from "mongoose";
import { readJson, remove, move, writeFile, ensureDir } from "fs-extra";
import { snakeCase, camelCase } from "change-case";
import path from "path";

import Profile from "../mongo-models/profile";
import Etl from "../mongo-models/etl";
import Payment from "../mongo-models/payment";
import Event from "../mongo-models/event";
import Budget from "../mongo-models/budget";

import { ProfileRecord, PaymentRecord, YearRecord, EventRecord, AccountingRecord } from "./postgre-schema";

mongoose.Promise = global.Promise;

const dataPath = path.join(__dirname, "../../cityvizor-data");

const dry = process.env["DRY"] ? !!JSON.parse(process.env["DRY"]) : false;

function convertRow2CamelCase(row: any): any {

  if (!row) return row;

  return Object.entries(row).reduce((acc, cur) => {
    acc[camelCase(cur[0])] = cur[1];
    return acc;
  }, {} as any);
};

(async function () {

  const knexConfig = await readJson("../cityvizor-keys/db.json");
  knexConfig.connection.host = "127.0.0.1";

  const pg = Knex({
    ...knexConfig,
    wrapIdentifier: (value, origImpl, queryContext) => origImpl(snakeCase(value)),

    // convert snake_case names to camelCase
    postProcessResponse: (result, queryContext) => {
      if (Array.isArray(result)) {
        return result.map(row => convertRow2CamelCase(row));
      } else {
        return convertRow2CamelCase(result);
      }
    }
  });

  await mongoose.connect("mongodb://127.0.0.1:27017/cityvizor", { useNewUrlParser: true, useUnifiedTopology: true, bufferCommands: false })




  console.log("\r\n=== PROFILES ===");
  const profileIds: { [key: string]: number } = {};

  if (!dry) await pg.raw("DELETE FROM app.profiles");
  if (!dry) await pg.raw("ALTER SEQUENCE app.profiles_id_seq RESTART WITH 1");

  if (!dry) await remove(path.join(dataPath, "avatars"));
  if (!dry) await ensureDir(path.join(dataPath, "avatars"));

  const profiles = await Profile.find({}).lean();

  const profileStatuses = {
    "active": "visible",
    "pending": "pending",
    "hidden": "hidden"
  }

  for (let profile of profiles) {


    const extname = profile.avatar ? path.extname(profile.avatar.name) : null


    if (!dry) {
      var id = (await pg<ProfileRecord>("app.profiles").insert({
        name: profile.name,
        status: profileStatuses[profile.status],
        url: profile.url,
        email: profile.email,
        ico: profile.ico,
        edesky: profile.edesky,
        mapasamospravy: profile.mapasamospravy < 1000 ? profile.mapasamospravy : null,
        gpsX: profile.gps[0],
        gpsY: profile.gps[1],
        avatarType: extname
      }, ["id"]))[0].id;
    }

    if (profile.avatar) {

      const profileAvatar = await Profile.findOne({ _id: profile._id }).select("avatar")

      const avatarPath = path.join(dataPath, "avatars", `avatar_${id}${extname}`);
      if (!dry) await writeFile(avatarPath, profileAvatar.avatar.data)
    }

    profileIds[profile._id] = id;

    process.stdout.write(".");
  }


  console.log("\r\n=== ETLS ===");

  if (!dry) await pg.raw("DELETE FROM app.years");

  const etls = await Etl.find({}).lean();

  for (let etl of etls) {
    if (!dry) await pg<YearRecord>("app.years").insert({
      profileId: profileIds[etl.profile],
      year: etl.year,
      validity: etl.validity,
      hidden: !etl.visible
    })
    process.stdout.write(".");
  }

  console.log("\r\n=== EVENTS ===");

  if (!dry) await pg.raw("DELETE FROM data.events");

  const events = await Event.find({}).lean();

  const eventIds: { [key: string]: number } = {};

  for (let event of events) {

    if(!Number(event.srcId)) continue;

    if (!dry) await pg<EventRecord>("data.events").insert({
      profileId: profileIds[event.profile],
      year: event.year,
      id: Number(event.srcId),
      name: event.name
    })

    eventIds[event._id] = Number(event.srcId) || null;

    process.stdout.write(".");
  }






  console.log("\r\n=== PAYMENTS ===");

  if (!dry) await pg.raw("DELETE FROM data.payments");

  const payments = await Payment.find({}).lean();

  for (let payment of payments) {

    if (!dry) await pg<PaymentRecord>("data.payments").insert({
      profileId: profileIds[payment.profile],
      year: payment.year,
      paragraph: Number(payment.paragraph),
      item: Number(payment.item),
      unit: null,
      event: eventIds[payment.event],
      amount: payment.amount,
      date: payment.date,
      counterpartyId: payment.counterpartyId,
      counterpartyName: payment.counterpartyName,
      description: payment.description
    })
    process.stdout.write(".");
  }  

  console.log("\r\n=== BUDGETS ===");

  if (!dry) await pg.raw("DELETE FROM data.accounting");

  const budgets = await Budget.find({}).lean();

  for (let budget of budgets) {

    if (budget.paragraphs) {
      for (let paragraph of budget.paragraphs) {

        for (let event of paragraph.events) {

          if (!eventIds[event.event]) continue;

          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "UCT",
            paragraph: Number(paragraph.id),
            item: null,
            unit: null,
            event: eventIds[event.event],
            amount: event.expenditureAmount
          })

          paragraph.expenditureAmount = paragraph.expenditureAmount - event.expenditureAmount;

          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "ROZ",
            paragraph: Number(paragraph.id),
            item: null,
            unit: null,
            event: eventIds[event.event],
            amount: event.budgetExpenditureAmount
          })

          paragraph.budgetExpenditureAmount = paragraph.budgetExpenditureAmount - event.budgetExpenditureAmount;
        }

        if (paragraph.expenditureAmount) {
          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "UCT",
            paragraph: Number(paragraph.id),
            item: null,
            unit: null,
            event: null,
            amount: paragraph.expenditureAmount
          })
        }

        if (paragraph.budgetExpenditureAmount) {
          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "ROZ",
            paragraph: Number(paragraph.id),
            item: null,
            unit: null,
            event: null,
            amount: paragraph.budgetExpenditureAmount
          })
        }
      }
    }
    if (budget.items) {
      for (let item of budget.items) {

        for (let event of item.events) {

          if (!eventIds[event.event]) continue;

          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "UCT",
            item: Number(item.id),
            paragraph: null,
            unit: null,
            event: eventIds[event.event],
            amount: Math.max(event.incomeAmount, event.expenditureAmount)
          })

          item.incomeAmount = item.incomeAmount - event.incomeAmount;
          item.expenditureAmount = item.expenditureAmount - event.expenditureAmount;

          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "ROZ",
            item: Number(item.id),
            paragraph: null,
            unit: null,
            event: eventIds[event.event],
            amount: Math.max(event.budgetIncomeAmount, event.budgetExpenditureAmount)
          })

          item.budgetIncomeAmount = item.budgetIncomeAmount - event.budgetIncomeAmount;
          item.budgetExpenditureAmount = item.budgetExpenditureAmount - event.budgetExpenditureAmount;
        }

        if (item.incomeAmount || item.expenditureAmount) {
          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "UCT",
            item: Number(item.id),
            paragraph: null,
            unit: null,
            event: null,
            amount: Math.max(item.incomeAmount, item.expenditureAmount)
          })
        }

        if (item.budgetIncomeAmount || item.budgetExpenditureAmount) {
          if (!dry) await pg<AccountingRecord>("data.accounting").insert({
            profileId: profileIds[budget.profile],
            year: budget.year,

            type: "ROZ",
            item: Number(item.id),
            paragraph: null,
            unit: null,
            event: null,
            amount: Math.max(item.budgetIncomeAmount, item.budgetExpenditureAmount)
          })
        }
      }
    }



    process.stdout.write(".");
  }

  console.log("\r\n=== FINISHED ===");

  await mongoose.disconnect();

  await pg.destroy();

})();