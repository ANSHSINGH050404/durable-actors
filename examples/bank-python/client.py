from generated import actors

account = actors.BankAccount.get("demo")
print("Balance before deposit:", account.get_balance(), "cents")
print("Balance after depositing $25:", account.deposit(2500), "cents")
