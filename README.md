# SIMPLE MARKET MAKING BOT

# !!!!!NOT FINANCIAL ADVICE, USE AT YOUR OWN RISK, I AM NOT LIABLE FOR YOUR LOSSES IN ANY WAY, THIS BOT COULD (LIKELY) HAVE BUGS!!!!!

- The bot has this logic:
1. Submits a limit buy 1 tick over the best bid
2. If the buy is executed in a 15 sec span a sell order is submitted at price = best_ask - 1 tick
3. Data is saved to mySQL and then plotted on Grafana (optional)
4. THIS BOT WAS USED DURING THE BINANCE 0% FEES CAMPAINS, TRADE AT YOUR OWN RISKS


# BEFORE USE:

- Install requirements.txt
- Insert yout private keys
- Choose the desired spread gain for each trade


Consider supporting this repo if you are interested or register with my link to have a voucher on binance:
Binance: https://www.binance.com/en/activity/referral-entry/CPA?fromActivityPage=true&ref=CPA_00Q0PV68WP
Bybit: https://www.bybit.com/en-US/invite?ref=R1BKR
