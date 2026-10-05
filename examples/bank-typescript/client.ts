import { actors } from "./generated/index.js"

const account = actors.BankAccount.get("demo")
console.log("Balance before deposit:", await account.getBalance(), "cents")
console.log("Balance after depositing $25:", await account.deposit(2500), "cents")
