"""
Investment Advisor Chatbot - Web App Version
=============================================
FastAPI + WebSockets backend for AWS Bedrock Nova-2-Sonic
Integrated with TalkingHead frontend.
"""

import asyncio
import base64
import csv
import datetime
import json
import logging
import os
import uuid
import warnings

# APT Testing Logs
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


import uvicorn
from aws_sdk_bedrock_runtime.client import (
    BedrockRuntimeClient,
    InvokeModelWithBidirectionalStreamOperationInput,
)
from aws_sdk_bedrock_runtime.config import Config
from aws_sdk_bedrock_runtime.models import (
    BidirectionalInputPayloadPart,
    InvokeModelWithBidirectionalStreamInputChunk,
)
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from smithy_aws_core.identity.environment import EnvironmentCredentialsResolver

warnings.filterwarnings("ignore")

DEBUG = False
CSV_FILE = "customer_details.csv"

# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION & PROMPTS
# ═══════════════════════════════════════════════════════════════

CSV_COLUMNS = [
    "session_id",
    "timestamp",
    "full_name",
    "region_stated",
    "canonical_country",
    "currency_code",
    "currency_symbol",
    "monthly_inflow",
    "monthly_outflow",
    "monthly_surplus",
    "total_debt",
    "debt_to_income_months",
    "risk_appetite",
    "investment_amount",
    "investment_period_years",
    "investment_goals",
    "avoid_asset_classes",
    "alloc_cash_pct",
    "alloc_bonds_pct",
    "alloc_equities_pct",
    "alloc_reits_pct",
    "alloc_commodities_pct",
    "alloc_crypto_pct",
    "monthly_cash",
    "monthly_bonds",
    "monthly_equities",
    "monthly_reits",
    "monthly_commodities",
    "monthly_crypto",
    "top_pick_cash",
    "top_pick_bonds",
    "top_pick_equities",
    "top_pick_reits",
    "top_pick_commodities",
    "top_pick_crypto",
    "rationale",
]

SYSTEM_PROMPT = """
You are Aria, a professional investment advisor assistant. You ONLY discuss investment topics.
You ALWAYS respond in English, regardless of what language the user speaks.
You NEVER discuss order tracking, deliveries, or any non-investment topic.

## STRICT CONVERSATION RULE — MOST IMPORTANT:
Ask EXACTLY ONE question per turn. Never combine two questions.
Acknowledge the user's answer in one sentence, then ask the next single question.
Do NOT list upcoming questions. Do NOT number questions. Keep replies to 2-3 sentences max.

## Questions to ask in order (one at a time):
1. Full name
2. Country or region of residence
   - From their answer, YOU must silently determine: canonical country name, ISO currency code,
     and currency symbol. Do NOT ask the user about currency. Examples:
     "New Delhi" or "India" → India, INR, ₹
     "London" or "UK"       → United Kingdom, GBP, £
     "New York" or "USA"    → United States, USD, $
     "Dubai"                → United Arab Emirates, AED, AED
     Use your knowledge to map any city, region, or country to the correct currency.
3. Monthly income — ask in their local currency using the symbol you determined
4. Monthly expenses — same currency
5. Total outstanding debt (loans, credit cards, mortgages)
6. Risk appetite — ask: "If your portfolio dropped 20% temporarily, would you sell everything,
   hold on, or invest more?" Map to: Low / Moderate / High
7. How much they want to invest each month
8. Investment horizon: less than 1 year / 1-3 years / 3-5 years / more than 5 years
9. Main investment goal (e.g. car, house, retirement, education, wealth growth)
10. Any asset classes to avoid (crypto, stocks, etc.) — if none, that is fine

## After collecting all answers:
- Summarise the profile in 2-3 sentences and ask the user to confirm.
- Once confirmed, call calculateAssetAllocation with ALL fields including:
  canonical_country, currency_code, currency_symbol (which YOU determined from their region).
- When the tool responds:
  a) State each allocation percentage, one sentence per asset class.
  b) For each class, name 2-3 specific instruments from the tool's portfolio_recommendations,
     noting they are suited to the user's country and currency.
  c) Explain the rationale in 2 plain-English sentences.
  d) End with the disclaimer.
- Offer to answer follow-up questions about the investment plan.

## Tone: Warm, concise, jargon-free. Always English only.
"""

PORTFOLIO_OPTIONS = {
    "Cash & Money Market": {
        "GBP": [
            {
                "name": "Marcus by Goldman Sachs Easy-Access Savings",
                "type": "Savings Account",
                "detail": "Competitive UK rate, FSCS protected up to £85k",
            },
            {
                "name": "Vanguard Sterling Short-Term Money Market Fund",
                "type": "Money Market Fund",
                "detail": "Low cost, same-day liquidity",
            },
            {
                "name": "NS&I Premium Bonds",
                "type": "Government Savings",
                "detail": "Tax-free, government-backed, up to £50k",
            },
        ],
        "USD": [
            {
                "name": "Fidelity Government Money Market Fund (SPAXX)",
                "type": "Money Market Fund",
                "detail": "~5% 7-day yield, highly liquid",
            },
            {
                "name": "Marcus by Goldman Sachs High-Yield Savings",
                "type": "Savings Account",
                "detail": "FDIC insured, no fees",
            },
            {
                "name": "US Treasury Bills (3-month)",
                "type": "T-Bills",
                "detail": "Risk-free government instrument",
            },
        ],
        "INR": [
            {
                "name": "HDFC Liquid Fund",
                "type": "Liquid Mutual Fund",
                "detail": "Instant redemption, ~7% returns",
            },
            {
                "name": "SBI Savings Account",
                "type": "Savings Account",
                "detail": "DICGC insured up to Rs 5 lakh",
            },
            {
                "name": "Paytm Money Liquid ETF",
                "type": "Liquid ETF",
                "detail": "Real-time NAV, T+0 settlement",
            },
        ],
        "EUR": [
            {
                "name": "DWS ESG Liquidity Fund",
                "type": "Money Market Fund",
                "detail": "Euro denominated, daily liquidity",
            },
            {
                "name": "Amundi Euro Liquidity SRI",
                "type": "Money Market Fund",
                "detail": "ESG-screened, low risk",
            },
            {
                "name": "Deutsche Bank Tagesgeld",
                "type": "Savings Account",
                "detail": "Instant access, deposit protected",
            },
        ],
        "AED": [
            {
                "name": "Emirates NBD eSaver Account",
                "type": "Savings Account",
                "detail": "Competitive AED rate, instant access",
            },
            {
                "name": "ENBD Money Market Fund",
                "type": "Money Market Fund",
                "detail": "AED-denominated, daily liquidity",
            },
            {
                "name": "UAE Government Treasury Bills",
                "type": "T-Bills",
                "detail": "Risk-free short-term government paper",
            },
        ],
        "DEFAULT": [
            {
                "name": "Local High-Yield Savings Account",
                "type": "Savings Account",
                "detail": "Check rates at your primary bank",
            },
            {
                "name": "Short-Term Treasury Bills",
                "type": "Government Bills",
                "detail": "3-6 month government instrument",
            },
            {
                "name": "Money Market Mutual Fund",
                "type": "Money Market Fund",
                "detail": "Low risk, daily liquidity",
            },
        ],
    },
    "Bonds / Fixed Income": {
        "GBP": [
            {
                "name": "iShares Core UK Gilts ETF (IGLT)",
                "type": "UK Gilt ETF",
                "detail": "UK government bonds, low cost",
            },
            {
                "name": "Vanguard UK Investment Grade Bond Index",
                "type": "Corporate Bond Fund",
                "detail": "Diversified UK corporate bonds, IG only",
            },
            {
                "name": "NS&I Fixed Rate Savings Bonds",
                "type": "Government Bond",
                "detail": "Guaranteed returns, government-backed",
            },
        ],
        "USD": [
            {
                "name": "iShares Core US Aggregate Bond ETF (AGG)",
                "type": "Broad Bond ETF",
                "detail": "Tracks US investment-grade bond market",
            },
            {
                "name": "Vanguard Total Bond Market ETF (BND)",
                "type": "Broad Bond ETF",
                "detail": "Diversified, ultra-low 0.03% expense ratio",
            },
            {
                "name": "US I-Bonds (TreasuryDirect)",
                "type": "Inflation Bond",
                "detail": "Inflation-linked, up to $10k/year",
            },
        ],
        "INR": [
            {
                "name": "SBI Magnum Gilt Fund",
                "type": "Gilt Fund",
                "detail": "Government securities only",
            },
            {
                "name": "HDFC Corporate Bond Fund",
                "type": "Corporate Bond Fund",
                "detail": "AAA-rated bonds, 3-5 year duration",
            },
            {
                "name": "RBI Floating Rate Savings Bonds",
                "type": "Government Bond",
                "detail": "Inflation-linked rate, 7-year lock-in",
            },
        ],
        "EUR": [
            {
                "name": "iShares Core Euro Govt Bond ETF (IEGA)",
                "type": "Govt Bond ETF",
                "detail": "Eurozone sovereign bonds",
            },
            {
                "name": "Vanguard EUR Corporate Bond ETF",
                "type": "Corporate Bond ETF",
                "detail": "Investment grade Euro corporates",
            },
            {
                "name": "German Bundesanleihe (Bunds)",
                "type": "Govt Bond",
                "detail": "Benchmark safe-haven Euro bond",
            },
        ],
        "AED": [
            {
                "name": "UAE Government Sukuk",
                "type": "Islamic Bond",
                "detail": "Sharia-compliant sovereign instrument",
            },
            {
                "name": "Emirates NBD Bond Fund",
                "type": "Corporate Bond Fund",
                "detail": "AED/USD bonds, diversified issuers",
            },
            {
                "name": "Franklin Gulf Bond Fund",
                "type": "GCC Bond Fund",
                "detail": "Regional GCC fixed income exposure",
            },
        ],
        "DEFAULT": [
            {
                "name": "iShares Global Govt Bond ETF",
                "type": "Govt Bond ETF",
                "detail": "Diversified sovereign bonds globally",
            },
            {
                "name": "Local Government Savings Bond",
                "type": "Government Bond",
                "detail": "Check your national treasury website",
            },
            {
                "name": "Investment Grade Corporate Bond Fund",
                "type": "Corporate Bond Fund",
                "detail": "AAA-BBB rated, 3-7 year duration",
            },
        ],
    },
    "Equities": {
        "GBP": [
            {
                "name": "Vanguard FTSE All-World ETF (VWRL)",
                "type": "Global Equity ETF",
                "detail": "3500+ stocks, 0.22% OCF, London-listed",
            },
            {
                "name": "iShares Core MSCI World ETF (IWDA)",
                "type": "Global Equity ETF",
                "detail": "Developed markets, 0.20% OCF",
            },
            {
                "name": "Fundsmith Equity Fund",
                "type": "Active Global Fund",
                "detail": "Quality-focused, long-term compounder",
            },
        ],
        "USD": [
            {
                "name": "Vanguard S&P 500 ETF (VOO)",
                "type": "US Equity ETF",
                "detail": "Tracks S&P 500, 0.03% expense ratio",
            },
            {
                "name": "iShares MSCI ACWI ETF (ACWI)",
                "type": "Global Equity ETF",
                "detail": "All-world developed + emerging markets",
            },
            {
                "name": "Fidelity ZERO Total Market Index Fund (FZROX)",
                "type": "US Total Market",
                "detail": "0% expense ratio, broad US market",
            },
        ],
        "INR": [
            {
                "name": "Mirae Asset Large Cap Fund",
                "type": "Large Cap MF",
                "detail": "Top 100 Indian companies by market cap",
            },
            {
                "name": "Parag Parikh Flexi Cap Fund",
                "type": "Flexi Cap MF",
                "detail": "Indian + global equities, value-oriented",
            },
            {
                "name": "Nifty 50 Index Fund (UTI / HDFC)",
                "type": "Index Fund",
                "detail": "Top 50 NSE stocks, very low cost",
            },
        ],
        "EUR": [
            {
                "name": "iShares Core MSCI World ETF (EUR hedged)",
                "type": "Global Equity ETF",
                "detail": "Currency-hedged for Eurozone investors",
            },
            {
                "name": "Amundi MSCI Europe ETF",
                "type": "European ETF",
                "detail": "Broad European equity exposure",
            },
            {
                "name": "Vanguard ESG Global All Cap ETF",
                "type": "ESG Equity ETF",
                "detail": "Sustainability-screened global equities",
            },
        ],
        "AED": [
            {
                "name": "iShares MSCI World ETF (USD)",
                "type": "Global Equity ETF",
                "detail": "Widely accessible from UAE brokerage accounts",
            },
            {
                "name": "Vanguard S&P 500 ETF (VOO)",
                "type": "US Equity ETF",
                "detail": "Core US market exposure, low cost",
            },
            {
                "name": "DFM / ADX listed stocks",
                "type": "Local Equities",
                "detail": "Dubai or Abu Dhabi exchange-listed companies",
            },
        ],
        "DEFAULT": [
            {
                "name": "iShares MSCI ACWI ETF",
                "type": "Global Equity ETF",
                "detail": "Broad global equity coverage",
            },
            {
                "name": "Vanguard Total World Stock ETF (VT)",
                "type": "Global Equity ETF",
                "detail": "8000+ stocks across 50+ countries",
            },
            {
                "name": "Local Market Index Fund",
                "type": "Domestic Index Fund",
                "detail": "Low-cost tracker of home market index",
            },
        ],
    },
    "Real Estate (REITs)": {
        "GBP": [
            {
                "name": "iShares UK Property ETF (IUKP)",
                "type": "UK REIT ETF",
                "detail": "UK commercial property exposure",
            },
            {
                "name": "Tritax Big Box REIT",
                "type": "UK REIT",
                "detail": "Logistics and warehouse focus",
            },
            {
                "name": "Land Securities Group (LAND) - LSE",
                "type": "UK REIT",
                "detail": "Diversified UK commercial property",
            },
        ],
        "USD": [
            {
                "name": "Vanguard Real Estate ETF (VNQ)",
                "type": "US REIT ETF",
                "detail": "Broad US REIT market, 0.12% fee",
            },
            {
                "name": "iShares Global REIT ETF (REET)",
                "type": "Global REIT ETF",
                "detail": "US + international REITs combined",
            },
            {
                "name": "Realty Income Corp (O)",
                "type": "US REIT",
                "detail": "Monthly dividend, retail-focused",
            },
        ],
        "INR": [
            {
                "name": "Embassy Office Parks REIT",
                "type": "Indian REIT",
                "detail": "Largest Indian REIT by area",
            },
            {
                "name": "Mindspace Business Parks REIT",
                "type": "Indian REIT",
                "detail": "Grade-A offices across 4 cities",
            },
            {
                "name": "Brookfield India Real Estate Trust",
                "type": "Indian REIT",
                "detail": "Commercial offices, Brookfield-backed",
            },
        ],
        "AED": [
            {
                "name": "ENBD REIT",
                "type": "UAE REIT",
                "detail": "Dubai commercial & residential property",
            },
            {
                "name": "iShares Global REIT ETF (REET)",
                "type": "Global REIT ETF",
                "detail": "Accessible via UAE brokers, USD-denominated",
            },
            {
                "name": "Emaar Properties (DFM)",
                "type": "Property Stock",
                "detail": "Dubai's largest developer, DFM-listed",
            },
        ],
        "DEFAULT": [
            {
                "name": "iShares Global REIT ETF (REET)",
                "type": "Global REIT ETF",
                "detail": "Diversified global property exposure",
            },
            {
                "name": "Vanguard Real Estate ETF (VNQ)",
                "type": "US REIT ETF",
                "detail": "Most liquid REIT ETF globally",
            },
            {
                "name": "Schwab US REIT ETF (SCHH)",
                "type": "US REIT ETF",
                "detail": "Low-cost US property market access",
            },
        ],
    },
    "Commodities": {
        "GBP": [
            {
                "name": "iShares Physical Gold ETC (IGLN)",
                "type": "Gold ETC",
                "detail": "Physically backed gold, London-listed",
            },
            {
                "name": "WisdomTree Broad Commodities ETF",
                "type": "Broad Commodity ETF",
                "detail": "Diversified basket incl. energy and metals",
            },
            {
                "name": "Invesco Bloomberg Commodity ETF",
                "type": "Broad Commodity ETF",
                "detail": "Tracks Bloomberg Commodity Index",
            },
        ],
        "USD": [
            {
                "name": "SPDR Gold Shares ETF (GLD)",
                "type": "Gold ETF",
                "detail": "Largest gold ETF by AUM",
            },
            {
                "name": "iShares S&P GSCI Commodity ETF (GSG)",
                "type": "Broad Commodity ETF",
                "detail": "Energy-heavy broad commodity basket",
            },
            {
                "name": "Invesco DB Commodity Index ETF (DBC)",
                "type": "Broad Commodity ETF",
                "detail": "14-commodity diversified basket",
            },
        ],
        "INR": [
            {
                "name": "Nippon India Gold ETF",
                "type": "Gold ETF",
                "detail": "Physically backed, NSE-listed",
            },
            {
                "name": "HDFC Gold Fund of Fund",
                "type": "Gold Fund",
                "detail": "No demat required, invests in Gold ETF",
            },
            {
                "name": "Sovereign Gold Bond (RBI)",
                "type": "Govt Gold Bond",
                "detail": "2.5% annual interest + gold price upside",
            },
        ],
        "AED": [
            {
                "name": "Gold via Dubai Gold Souk / DGCX",
                "type": "Physical Gold",
                "detail": "Buy physical gold, store via vault services",
            },
            {
                "name": "SPDR Gold Shares ETF (GLD) via broker",
                "type": "Gold ETF",
                "detail": "USD-priced, accessible via UAE brokers",
            },
            {
                "name": "iShares Bloomberg Commodity ETF",
                "type": "Broad Commodity ETF",
                "detail": "Diversified commodity basket",
            },
        ],
        "DEFAULT": [
            {
                "name": "SPDR Gold Shares ETF (GLD)",
                "type": "Gold ETF",
                "detail": "Global gold price exposure",
            },
            {
                "name": "iShares Bloomberg Roll Select Commodity",
                "type": "Broad Commodity ETF",
                "detail": "Optimised roll strategy, diversified",
            },
            {
                "name": "Physical Gold via local exchange",
                "type": "Gold",
                "detail": "Buy through your country's exchange",
            },
        ],
    },
    "Cryptocurrency": {
        "GBP": [
            {
                "name": "WisdomTree Bitcoin ETP (BTCW) - LSE",
                "type": "Bitcoin ETP",
                "detail": "Regulated, physically backed BTC, London-listed",
            },
            {
                "name": "Coinbase UK (BTC / ETH)",
                "type": "Exchange",
                "detail": "FCA-registered, direct purchase",
            },
            {
                "name": "CoinShares Physical Ethereum (ETHE)",
                "type": "Ethereum ETP",
                "detail": "Physically backed ETH, LSE-listed",
            },
        ],
        "USD": [
            {
                "name": "iShares Bitcoin Trust ETF (IBIT)",
                "type": "Bitcoin ETF",
                "detail": "SEC-approved spot Bitcoin ETF by BlackRock",
            },
            {
                "name": "Fidelity Wise Origin Bitcoin Fund (FBTC)",
                "type": "Bitcoin ETF",
                "detail": "Spot Bitcoin ETF, Fidelity custodied",
            },
            {
                "name": "Ethereum ETF (ETHA) - BlackRock",
                "type": "Ether ETF",
                "detail": "SEC-approved spot Ethereum ETF",
            },
        ],
        "INR": [
            {
                "name": "CoinDCX / WazirX (BTC, ETH)",
                "type": "Exchange",
                "detail": "Registered Indian crypto exchanges",
            },
            {
                "name": "Note: 30% flat tax + 1% TDS applies on crypto gains",
                "type": "Tax Note",
                "detail": "Ensure compliance with Indian VDA tax rules",
            },
        ],
        "AED": [
            {
                "name": "Bybit / Kraken (licensed in UAE)",
                "type": "Exchange",
                "detail": "VARA-licensed exchanges in Dubai",
            },
            {
                "name": "iShares Bitcoin Trust ETF (IBIT) via broker",
                "type": "Bitcoin ETF",
                "detail": "USD-denominated, accessible via UAE brokers",
            },
        ],
        "DEFAULT": [
            {
                "name": "iShares Bitcoin Trust (IBIT)",
                "type": "Bitcoin ETF",
                "detail": "Regulated US-listed spot Bitcoin ETF",
            },
            {
                "name": "Physical Bitcoin via regulated exchange",
                "type": "Crypto",
                "detail": "Use a regulated local exchange",
            },
            {
                "name": "Ethereum via regulated exchange",
                "type": "Crypto",
                "detail": "Second largest crypto by market cap",
            },
        ],
    },
}

# ═══════════════════════════════════════════════════════════════
#  BUSINESS LOGIC & HELPERS
# ═══════════════════════════════════════════════════════════════


def get_portfolio_recommendations(alloc: dict, currency: str) -> dict:
    recs = {}
    for asset_class, pct in alloc.items():
        if pct <= 0:
            continue
        options = PORTFOLIO_OPTIONS.get(asset_class, {})
        instruments = options.get(currency, options.get("DEFAULT", []))
        recs[asset_class] = instruments[:3]
    return recs


def _calculate_allocation(profile: dict) -> dict:
    risk = profile.get("risk_appetite", "moderate").lower()
    years = float(profile.get("investment_period_years", 5))
    debt = float(profile.get("total_debt", 0))
    inflow = float(profile.get("monthly_inflow", 0))
    invest = float(profile.get("investment_amount", 0))
    currency = profile.get("currency_code", "USD")
    symbol = profile.get("currency_symbol", "$")
    avoid = [a.lower() for a in profile.get("avoid_asset_classes", [])]

    base = {
        "low": {
            "Cash & Money Market": 20,
            "Bonds / Fixed Income": 50,
            "Equities": 20,
            "Real Estate (REITs)": 10,
            "Commodities": 0,
            "Cryptocurrency": 0,
        },
        "moderate": {
            "Cash & Money Market": 10,
            "Bonds / Fixed Income": 35,
            "Equities": 40,
            "Real Estate (REITs)": 10,
            "Commodities": 5,
            "Cryptocurrency": 0,
        },
        "high": {
            "Cash & Money Market": 5,
            "Bonds / Fixed Income": 15,
            "Equities": 55,
            "Real Estate (REITs)": 10,
            "Commodities": 10,
            "Cryptocurrency": 5,
        },
    }
    alloc = base.get(risk, base["moderate"]).copy()

    # Time-horizon adjustment
    if years < 1:
        shift = 15
        alloc["Cash & Money Market"] = min(alloc["Cash & Money Market"] + shift, 40)
        alloc["Equities"] = max(alloc["Equities"] - shift, 0)
    elif years <= 3:
        shift = 8
        alloc["Bonds / Fixed Income"] = min(alloc["Bonds / Fixed Income"] + shift, 60)
        alloc["Equities"] = max(alloc["Equities"] - shift, 0)

    # Debt-to-income adjustment
    dti = debt / max(inflow, 1)
    if dti > 6:
        alloc["Cash & Money Market"] = min(alloc["Cash & Money Market"] + 10, 35)
        alloc["Equities"] = max(alloc["Equities"] - 10, 0)

    # Remove avoided asset classes and redistribute
    removed, to_zero = 0, []
    for k in list(alloc.keys()):
        if any(av in k.lower() for av in avoid):
            removed += alloc[k]
            to_zero.append(k)
    for k in to_zero:
        alloc[k] = 0
    if removed > 0:
        keys = [k for k in ["Equities", "Bonds / Fixed Income"] if k not in to_zero]
        per = removed // max(len(keys), 1)
        for k in keys:
            alloc[k] += per

    # Normalise to exactly 100%
    total = sum(alloc.values())
    if total != 100:
        pivot = next(
            k
            for k in ["Equities", "Bonds / Fixed Income", "Cash & Money Market"]
            if alloc.get(k, 0) > 0
        )
        alloc[pivot] += 100 - total

    active_alloc = {k: v for k, v in alloc.items() if v > 0}
    monthly = {k: round(invest * v / 100, 2) for k, v in active_alloc.items()}
    recommendations = get_portfolio_recommendations(active_alloc, currency)

    # Build rationale
    goal_notes = {
        "car": "A car purchase suits a balanced approach with good liquidity.",
        "house": "Property goals benefit from stable fixed-income to protect capital.",
        "retirement": "Long-term retirement allows higher equity exposure for growth.",
        "education": "Education needs capital preservation — bonds and cash weighted higher.",
        "wealth": "Wealth growth favours equities for long-term compounding.",
    }
    goals = [g.lower() for g in profile.get("investment_goals", [])]
    goal_note = next(
        (n for kw, n in goal_notes.items() if any(kw in g for g in goals)),
        "A diversified strategy balances growth and security.",
    )
    risk_note = {
        "low": "A conservative profile prioritises capital preservation.",
        "moderate": "A balanced profile seeks steady growth while managing downside.",
        "high": "An aggressive profile maximises growth, accepting higher volatility.",
    }.get(risk, "")

    rationale = f"{risk_note} {goal_note} " f"The {years:.0f}-year horizon " + (
        "favours keeping more liquid assets."
        if years < 1
        else (
            "allows meaningful equity participation."
            if years >= 5
            else "warrants a cautious mix of growth and stability."
        )
    )

    return {
        "profile_summary": {
            "name": profile.get("full_name", "Client"),
            "country": profile.get("canonical_country", ""),
            "currency": f"{currency} ({symbol})",
            "risk_appetite": risk.capitalize(),
            "invest_monthly": f"{symbol}{invest:,.2f}",
            "period_years": years,
            "goals": profile.get("investment_goals", []),
        },
        "asset_allocation_pct": active_alloc,
        "monthly_investment_split": {
            k: f"{symbol}{v:,.2f}" for k, v in monthly.items()
        },
        "portfolio_recommendations": {
            ac: [{"name": i["name"], "type": i["type"]} for i in picks]
            for ac, picks in recommendations.items()
        },
        "rationale": rationale,
        "disclaimer": (
            "This allocation is illustrative and does not constitute regulated financial advice. "
            "Please consult a qualified financial advisor before making investment decisions."
        ),
    }


def save_to_csv(session_id: str, profile: dict, result: dict):
    alloc = result.get("asset_allocation_pct", {})
    invest = float(profile.get("investment_amount", 0))
    recs = result.get("portfolio_recommendations", {})

    def pct(k):
        return alloc.get(k, 0)

    def amt(k):
        return round(invest * pct(k) / 100, 2)

    def pick(k):
        return recs.get(k, [{}])[0].get("name", "") if recs.get(k) else ""

    inflow = float(profile.get("monthly_inflow", 0))
    outflow = float(profile.get("monthly_outflow", 0))
    debt = float(profile.get("total_debt", 0))

    row = {
        "session_id": session_id,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "full_name": profile.get("full_name", ""),
        "region_stated": profile.get("region_stated", ""),
        "canonical_country": profile.get("canonical_country", ""),
        "currency_code": profile.get("currency_code", ""),
        "currency_symbol": profile.get("currency_symbol", ""),
        "monthly_inflow": inflow,
        "monthly_outflow": outflow,
        "monthly_surplus": round(inflow - outflow, 2),
        "total_debt": debt,
        "debt_to_income_months": round(debt / max(inflow, 1), 2),
        "risk_appetite": profile.get("risk_appetite", ""),
        "investment_amount": invest,
        "investment_period_years": profile.get("investment_period_years", ""),
        "investment_goals": "; ".join(profile.get("investment_goals", [])),
        "avoid_asset_classes": "; ".join(profile.get("avoid_asset_classes", [])),
        "alloc_cash_pct": pct("Cash & Money Market"),
        "alloc_bonds_pct": pct("Bonds / Fixed Income"),
        "alloc_equities_pct": pct("Equities"),
        "alloc_reits_pct": pct("Real Estate (REITs)"),
        "alloc_commodities_pct": pct("Commodities"),
        "alloc_crypto_pct": pct("Cryptocurrency"),
        "monthly_cash": amt("Cash & Money Market"),
        "monthly_bonds": amt("Bonds / Fixed Income"),
        "monthly_equities": amt("Equities"),
        "monthly_reits": amt("Real Estate (REITs)"),
        "monthly_commodities": amt("Commodities"),
        "monthly_crypto": amt("Cryptocurrency"),
        "top_pick_cash": pick("Cash & Money Market"),
        "top_pick_bonds": pick("Bonds / Fixed Income"),
        "top_pick_equities": pick("Equities"),
        "top_pick_reits": pick("Real Estate (REITs)"),
        "top_pick_commodities": pick("Commodities"),
        "top_pick_crypto": pick("Cryptocurrency"),
        "rationale": result.get("rationale", ""),
    }

    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ═══════════════════════════════════════════════════════════════
#  TOOL PROCESSOR
# ═══════════════════════════════════════════════════════════════
class ToolProcessor:
    def __init__(self, session_id: str, ws_queue: asyncio.Queue):
        self.tasks = {}
        self.session_id = session_id
        self.ws_queue = ws_queue

    async def process_tool_async(self, tool_name: str, tool_content: dict):
        task_id = str(uuid.uuid4())
        logger.info(f"Starting tool task {task_id} for tool {tool_name}")
        task = asyncio.create_task(self._run_tool(tool_name, tool_content))
        self.tasks[task_id] = task
        try:
            return await task
        finally:
            logger.info(f"Tool task {task_id} for tool {tool_name} finished.")
            self.tasks.pop(task_id, None)

    async def _run_tool(self, tool_name: str, tool_content: dict):
        logger.info(f"Running tool: {tool_name} with content: {tool_content}")
        await self.ws_queue.put(
            {"type": "system", "text": f"Running tool: {tool_name}..."}
        )

        raw = tool_content.get("content", "{}")
        try:
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError in tool {tool_name}: {e}")
            payload = {}

        if tool_name.lower() == "calculateassetallocation":
            try:
                required = [
                    "monthly_inflow",
                    "monthly_outflow",
                    "investment_amount",
                    "investment_period_years",
                    "risk_appetite",
                    "investment_goals",
                ]
                missing = [k for k in required if not payload.get(k)]
                if missing:
                    err = f"Missing required fields: {missing}. Please collect all answers first."
                    logger.error(f"Tool {tool_name} failed: {err}")
                    await self.ws_queue.put({"type": "system", "text": err})
                    return {"error": err}

                result = _calculate_allocation(payload)
                save_to_csv(self.session_id, payload, result)

                logger.info(f"Tool {tool_name} executed successfully. Result: {result}")
                await self.ws_queue.put(
                    {
                        "type": "system",
                        "text": "Customer profile saved. Allocation generated successfully.",
                    }
                )
                return result
            except Exception as e:
                import traceback

                logger.error(f"Allocation tool failed: {e}\n{traceback.format_exc()}")
                print(f"[ERROR] Allocation tool failed: {e}\n{traceback.format_exc()}")
                return {"error": f"Allocation failed: {e}"}

        logger.warning(f"Unknown tool: {tool_name}")
        return {"error": f"Unknown tool: {tool_name}"}


# ═══════════════════════════════════════════════════════════════
#  BEDROCK STREAM MANAGER (WebSocket Integration)
# ═══════════════════════════════════════════════════════════════
class BedrockStreamManager:

    START_SESSION_EVENT = json.dumps(
        {
            "event": {
                "sessionStart": {
                    "inferenceConfiguration": {
                        "maxTokens": 2048,
                        "topP": 0.9,
                        "temperature": 0.7,
                    }
                }
            }
        }
    )

    CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "type": "AUDIO", "interactive": true, "role": "USER",
                "audioInputConfiguration": {
                    "mediaType": "audio/lpcm", "sampleRateHertz": 16000,
                    "sampleSizeBits": 16, "channelCount": 1,
                    "audioType": "SPEECH", "encoding": "base64"
                }
            }
        }
    }"""

    AUDIO_EVENT_TEMPLATE = """{
        "event": {
            "audioInput": {
                "promptName": "%s", "contentName": "%s", "content": "%s"
            }
        }
    }"""

    TEXT_CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "type": "TEXT", "role": "%s", "interactive": false,
                "textInputConfiguration": {"mediaType": "text/plain"}
            }
        }
    }"""

    TEXT_INPUT_EVENT = """{
        "event": {
            "textInput": {
                "promptName": "%s", "contentName": "%s", "content": "%s"
            }
        }
    }"""

    TOOL_CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "interactive": false, "type": "TOOL", "role": "TOOL",
                "toolResultInputConfiguration": {
                    "toolUseId": "%s", "type": "TEXT",
                    "textInputConfiguration": {"mediaType": "text/plain"}
                }
            }
        }
    }"""

    CONTENT_END_EVENT = """{
        "event": {
            "contentEnd": {"promptName": "%s", "contentName": "%s"}
        }
    }"""

    PROMPT_END_EVENT = """{
        "event": {"promptEnd": {"promptName": "%s"}}
    }"""

    SESSION_END_EVENT = '{"event": {"sessionEnd": {}}}'

    _ASSET_ALLOC_SCHEMA = json.dumps(
        {
            "type": "object",
            "properties": {
                "full_name": {"type": "string"},
                "region_stated": {"type": "string"},
                "canonical_country": {"type": "string"},
                "currency_code": {"type": "string"},
                "currency_symbol": {"type": "string"},
                "monthly_inflow": {"type": "number"},
                "monthly_outflow": {"type": "number"},
                "total_debt": {"type": "number"},
                "risk_appetite": {
                    "type": "string",
                    "enum": ["low", "moderate", "high"],
                },
                "investment_amount": {"type": "number"},
                "investment_period_years": {"type": "number"},
                "investment_goals": {"type": "array", "items": {"type": "string"}},
                "avoid_asset_classes": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "full_name",
                "canonical_country",
                "currency_code",
                "currency_symbol",
                "monthly_inflow",
                "monthly_outflow",
                "total_debt",
                "risk_appetite",
                "investment_amount",
                "investment_period_years",
                "investment_goals",
            ],
        }
    )

    def start_prompt(self):
        return json.dumps(
            {
                "event": {
                    "promptStart": {
                        "promptName": self.prompt_name,
                        "textOutputConfiguration": {"mediaType": "text/plain"},
                        "audioOutputConfiguration": {
                            "mediaType": "audio/lpcm",
                            "sampleRateHertz": 24000,
                            "sampleSizeBits": 16,
                            "channelCount": 1,
                            "voiceId": "amy",
                            "encoding": "base64",
                            "audioType": "SPEECH",
                        },
                        "toolUseOutputConfiguration": {"mediaType": "application/json"},
                        "toolConfiguration": {
                            "tools": [
                                {
                                    "toolSpec": {
                                        "name": "calculateAssetAllocation",
                                        "description": (
                                            "Calculates a personalised asset allocation and portfolio "
                                            "recommendations for the client. YOU must populate "
                                            "canonical_country, currency_code, and currency_symbol "
                                            "based on the region/city the user mentioned — do NOT ask "
                                            "the user for currency. Call this tool ONLY after all 10 "
                                            "questions are answered and the user confirms the summary."
                                        ),
                                        "inputSchema": {
                                            "json": self._ASSET_ALLOC_SCHEMA
                                        },
                                    }
                                }
                            ]
                        },
                    }
                }
            }
        )

    def __init__(
        self,
        ws_queue: asyncio.Queue,
        model_id="amazon.nova-2-sonic-v1:0",
        region="us-east-1",
    ):
        self.model_id = model_id
        self.region = region
        self.session_id = str(uuid.uuid4())
        self.prompt_name = str(uuid.uuid4())
        self.content_name = str(uuid.uuid4())
        self.ws_queue = ws_queue

        self.is_active = False
        self.toolName = None
        self.toolUseId = None
        self.toolUseContent = None

        self.pending_tool_tasks = {}
        self.tool_processor = ToolProcessor(self.session_id, self.ws_queue)
        self._audio_chunk_queue = asyncio.Queue()

        self.stream_response = None
        self.response_task = None
        self.send_task = None

    async def initialize_stream(self):
        logger.info(f"Initializing Bedrock stream for session {self.session_id}")
        config = Config(
            endpoint_uri=f"https://bedrock-runtime.{self.region}.amazonaws.com",
            aws_credentials_identity_resolver=EnvironmentCredentialsResolver(),
            region=self.region,
        )
        self.client = BedrockRuntimeClient(config=config)
        self.stream_response = await self.client.invoke_model_with_bidirectional_stream(
            InvokeModelWithBidirectionalStreamOperationInput(model_id=self.model_id)
        )
        self.is_active = True
        self.send_task = asyncio.create_task(self._send_events_loop())

        # Send Session Start & Prompt Rules
        await self.send_raw_event(self.START_SESSION_EVENT)
        await self.send_raw_event(self.start_prompt())

        sys_cn = str(uuid.uuid4())
        await self.send_raw_event(
            self.TEXT_CONTENT_START_EVENT % (self.prompt_name, sys_cn, "SYSTEM")
        )
        await self.send_raw_event(
            self.TEXT_INPUT_EVENT
            % (self.prompt_name, sys_cn, json.dumps(SYSTEM_PROMPT)[1:-1])
        )
        await self.send_raw_event(self.CONTENT_END_EVENT % (self.prompt_name, sys_cn))

        # Open mic content stream
        self.content_name = str(uuid.uuid4())
        await self.send_raw_event(
            self.CONTENT_START_EVENT % (self.prompt_name, self.content_name)
        )

        self.response_task = asyncio.create_task(self._process_responses())
        logger.info("Bedrock stream initialized.")

    async def _send_events_loop(self):
        while self.is_active:
            try:
                event_str = await asyncio.wait_for(
                    self._audio_chunk_queue.get(), timeout=0.1
                )
                chunk = InvokeModelWithBidirectionalStreamInputChunk(
                    value=BidirectionalInputPayloadPart(
                        bytes_=event_str.encode("utf-8")
                    )
                )
                await self.stream_response.input_stream.send(chunk)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in send events loop: {e}")
                break

    async def send_raw_event(self, event_str: str):
        if self.is_active:
            await self._audio_chunk_queue.put(event_str)
        else:
            logger.warning("Attempted to send event on inactive stream.")

    def add_audio_chunk(self, audio_bytes: bytes):
        encoded = base64.b64encode(audio_bytes).decode("utf-8")
        event = self.AUDIO_EVENT_TEMPLATE % (
            self.prompt_name,
            self.content_name,
            encoded,
        )
        asyncio.get_event_loop().call_soon_threadsafe(
            self._audio_chunk_queue.put_nowait, event
        )

    async def _process_responses(self):
        try:
            while self.is_active:
                output = await self.stream_response.await_output()
                result = await output[1].receive()
                if result.value and result.value.bytes_:
                    json_data = json.loads(result.value.bytes_.decode("utf-8"))
                    logger.debug(f"Received event from Bedrock: {json_data}")
                    await self._handle_event(json_data)
        except Exception as e:
            logger.error(f"Error processing Bedrock responses: {e}")
            if self.is_active:
                await self.ws_queue.put(
                    {"type": "system", "text": f"Error in stream: {e}"}
                )
        finally:
            self.is_active = False

    async def _handle_event(self, json_data: dict):
        event = json_data.get("event", {})

        if "textOutput" in event:
            text = event["textOutput"]["content"]
            logger.info(f"Received text from Bedrock: {text}")
            if '{ "interrupted" : true }' in text:
                await self.ws_queue.put({"type": "interrupted"})
            else:
                await self.ws_queue.put({"type": "text", "text": text})

        elif "audioOutput" in event:
            await self.ws_queue.put(
                {"type": "audio", "data": event["audioOutput"]["content"]}
            )

        elif "contentEnd" in event:
            if event.get("contentEnd", {}).get("type") == "TOOL":
                logger.info(f"Tool request content ended for tool: {self.toolName}")
                self.handle_tool_request(
                    self.toolName, self.toolUseContent, self.toolUseId
                )
            else:
                await self.ws_queue.put({"type": "audio_end"})

        elif "toolUse" in event:
            self.toolUseContent = event["toolUse"]
            self.toolName = event["toolUse"]["toolName"]
            self.toolUseId = event["toolUse"]["toolUseId"]
            logger.info(
                f"Received tool use request: {self.toolName} with id: {self.toolUseId}"
            )

    def handle_tool_request(self, tool_name, tool_content, tool_use_id):
        cn = str(uuid.uuid4())
        logger.info(f"Handling tool request for {tool_name} with content name {cn}")
        task = asyncio.create_task(
            self._execute_tool_and_send_result(tool_name, tool_content, tool_use_id, cn)
        )
        self.pending_tool_tasks[cn] = task

    async def _execute_tool_and_send_result(
        self, tool_name, tool_content, tool_use_id, cn
    ):
        try:
            result = await self.tool_processor.process_tool_async(
                tool_name, tool_content
            )
            await self.send_raw_event(
                self.TOOL_CONTENT_START_EVENT % (self.prompt_name, cn, tool_use_id)
            )

            result_json = json.dumps(result)
            tool_res_event = json.dumps(
                {
                    "event": {
                        "toolResult": {
                            "promptName": self.prompt_name,
                            "contentName": cn,
                            "content": result_json,
                        }
                    }
                }
            )
            await self.send_raw_event(tool_res_event)
            await self.send_raw_event(self.CONTENT_END_EVENT % (self.prompt_name, cn))
            logger.info(f"Successfully sent tool result for {tool_name}")
        except Exception as e:
            logger.error(f"Failed to execute or send tool result for {tool_name}: {e}")

    # FIX 4: Clean shutdown — flush the send queue before cancelling the send task
    async def close(self):
        logger.info(f"Closing Bedrock stream for session {self.session_id}")
        if not self.is_active:
            return
        self.is_active = False
        await self.send_raw_event(
            self.CONTENT_END_EVENT % (self.prompt_name, self.content_name)
        )
        await self.send_raw_event(self.PROMPT_END_EVENT % self.prompt_name)
        await self.send_raw_event(self.SESSION_END_EVENT)

        # Wait briefly for the queue to flush before tearing down the send task
        await asyncio.sleep(0.5)
        if self.send_task:
            self.send_task.cancel()
            try:
                await self.send_task
            except asyncio.CancelledError:
                pass

        logger.info("Bedrock stream closed.")


# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP & ROUTING
# ═══════════════════════════════════════════════════════════════

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    logger.info("Application starting up...")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")


# Mount the static folder containing index.html, style.css, app.js
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection accepted.")
    ws_queue = asyncio.Queue()
    manager = BedrockStreamManager(ws_queue=ws_queue)

    try:
        await manager.initialize_stream()
    except Exception as e:
        logger.error(f"WebSocket initialization error: {e}")
        await websocket.send_json(
            {"type": "system", "text": f"Initialization error: {e}"}
        )
        await websocket.close()
        return

    # Background task: forward queue items from Bedrock to Browser
    async def sender_task():
        while manager.is_active:
            try:
                msg = await ws_queue.get()
                if msg["type"] == "audio":
                    await websocket.send_bytes(base64.b64decode(msg["data"]))
                else:
                    await websocket.send_json(msg)
            except Exception as e:
                logger.error(f"Sender task error: {e}")
                break

    send_task = asyncio.create_task(sender_task())

    try:
        # Listen loop: Browser -> Backend -> Bedrock
        while True:
            data = await websocket.receive()
            if "bytes" in data:
                # Binary audio from browser mic
                manager.add_audio_chunk(data["bytes"])
            elif "text" in data:
                # FIX 3: Handle keepalive pings silently, log everything else
                try:
                    msg = json.loads(data["text"])
                    if msg.get("type") == "ping":
                        pass  # Silently ignore keepalive pings
                    else:
                        logger.info(f"Received text from client: {data['text']}")
                except json.JSONDecodeError:
                    logger.info(f"Received non-JSON text from client: {data['text']}")
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected.")
    except Exception as e:
        logger.error(f"WebSocket listen loop error: {e}")
    finally:
        logger.info("Closing WebSocket connection.")
        await manager.close()
        send_task.cancel()


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
