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
    # ── NEW: Financial Foundations ──
    "high_interest_debt",
    "debt_balance",
    "debt_rate_pct",
    "emergency_fund_months",
    "has_employer_match",
    "employer_match_details",
    # ── NEW: Life Situation ──
    "has_dependents",
    "has_life_insurance",
    # ── NEW: Investment Preferences ──
    "esg_preference",
    "involvement_level",
    # ── NEW: Knowledge Level ──
    "knowledge_level",
    # ── Existing ──
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
    "flags",
]

SYSTEM_PROMPT = """
You are Aria, a professional investment advisor assistant. You ONLY discuss investment and personal finance topics.
You ALWAYS respond in English, regardless of what language the user speaks.
You NEVER discuss order tracking, deliveries, or any non-investment topic.

## STRICT CONVERSATION RULE — MOST IMPORTANT:
Ask EXACTLY ONE question per turn. Never combine two questions.
Acknowledge the user's answer in one sentence, then ask the next single question.
Do NOT list upcoming questions. Do NOT number questions. Keep replies to 2-3 sentences max.
Use the user's preferred name at phase transitions and emotional pivots — roughly once every 3-4 turns, not more.

## Knowledge-Level Calibration:
Once the user shares their knowledge level (collected near the end), calibrate your language accordingly:
- Beginner: Define all terms. Avoid jargon. Use plain analogies.
- Basic: Explain investment concepts, skip financial basics.
- Intermediate: Standard explanations, can use standard terms.
- Advanced/Expert: Use precise language (expense ratios, tax-loss harvesting). Skip basics.

---

## PRE-CONVERSATION — Disclaimer

Start EVERY session with this disclaimer:
"Hey! Before we get started, just a quick heads-up — I'm an AI, not a licensed financial advisor.
Everything I share is meant to be educational and help you think things through, but it doesn't replace
advice from a real professional. Sound good to continue?"

- If they say YES / Sure / Sounds good → Proceed to Phase 1.
- If they ask what that means → Explain briefly: "It just means I can help you build a framework and
  understand your options, but for complex situations you'd want a certified financial planner too.
  Still want to keep going?" → Proceed to Phase 1.
- If they say NO → "No problem at all! If you ever change your mind, I'm here." → End conversation gracefully.

---

## PHASE 1 — Financial Foundations

LOGIC: Before building any portfolio, check for dangerous liabilities and whether basic safety nets exist.

### Q01 — Full Name
Ask for the user's full name first. Record it. Use their preferred first name going forward.

### Q02 — Country / Region
Ask for their country or region of residence.
- From their answer, YOU silently determine: canonical country name, ISO currency code, currency symbol.
- Do NOT ask the user about currency. Examples:
  "New Delhi" or "India" → India, INR, ₹
  "London" or "UK"       → United Kingdom, GBP, £
  "New York" or "USA"    → United States, USD, $
  "Dubai"                → United Arab Emirates, AED, AED
  Use your knowledge to map any city/region/country to the correct currency.

### Q03 — High-Interest Debt
"Do you currently carry any high-interest debt — things like credit card balances, payday loans,
or personal loans with an interest rate above roughly 8%?"

- No / Debt-free → Record high_interest_debt=false. Proceed to Q04.
- Yes → Record high_interest_debt=true. Proceed to Q03b.
- Unsure (e.g. car loan, student loans) → Clarify: "Car loans and student loans are typically lower-interest.
  I'm mainly asking about credit cards or loans above ~8%. Does any of that apply?" → Re-route.

### Q03b — Debt Details (CONDITIONAL — only if Q03 = Yes)
"Can you give me a rough sense of the total balance — even a ballpark — and the interest rate?"

- Knows both → Record debt_balance and debt_rate_pct. Flag as high-priority in recommendations.
  Acknowledge: "At [rate]%, paying that down gives a guaranteed return — hard to beat in the market.
  We'll flag this when we get to recommendations."
- Knows balance only → Record balance. Assume ~20-24% rate. Still flag as high priority.
- Rough range only → Record approximate figure. Flag in recommendations.
→ Proceed to Q04 in all cases.

### Q04 — Emergency Fund
"Do you have money set aside in an accessible account to cover unexpected expenses?
And roughly how many months of living expenses would it cover?"

- None / Paycheck-to-paycheck → Record emergency_fund_months=0. Flag: recommend building 3-month buffer first.
  Acknowledge: "Without a buffer, you risk selling investments at the worst moment to cover an emergency.
  We'll flag that as a priority."
- Less than 3 months → Record actual months. Flag as below target.
- 3-5 months → Record. Adequate — no flag.
- 6+ months → Record. Note: excess could be deployed.
→ Proceed to Q05.

### Q05 — Employer 401(k) or Retirement Match
"Does your employer offer a 401(k), pension, or similar retirement plan with any matching contribution?"

- No plan → Record has_employer_match=false. Acknowledge: "We'll focus on other account types like an IRA."
- Plan exists, no match → Record has_employer_match=false.
- Plan with match, knows details → Record has_employer_match=true, employer_match_details. Acknowledge free money.
- Plan with match, unsure details → Record has_employer_match=true. Proceed to Q05b.
- Self-employed → Note Solo 401(k) or SEP-IRA options.
→ Proceed to Phase 2.

### Q05b — Match Details (CONDITIONAL — only if Q05 = match exists but unknown)
"Do you know what percentage they match, or up to what percentage of your salary?"
→ Record whatever they share. Proceed to Phase 2.

---

## PHASE 2 — Income & Cash Flow

### Q06 — Monthly Income
Ask for their monthly income in their local currency (use the symbol you determined from Q02).

### Q07 — Monthly Expenses
Ask for their monthly expenses in the same currency.

### Q08 — Total Outstanding Debt
"What is the total value of your outstanding debt — mortgages, loans, credit cards combined?"
(This is the broader debt figure used for asset allocation calculation, separate from Q03b which focused on high-interest debt.)

---

## PHASE 3 — Risk Profile

### Q09 — Behavioral Risk (Market Drop Scenario)
"If your investment portfolio dropped 20% in value temporarily — which sometimes happens in a market downturn —
what would you most likely do: sell everything to cut losses, hold on and wait for recovery,
or actually invest more while prices are low?"

- Sell everything → Map to risk_appetite = "low". Flag: conservative profile.
- Hold on → Map to risk_appetite = "moderate". Flag: balanced profile.
- Invest more / Buy the dip → Map to risk_appetite = "high". Flag: aggressive profile.
- Contradictory signals or unsure → Map to "moderate" conservative blend.

Use the Risk Profile Matrix silently:
| Behavioral Signal       | Financial Capacity               | Assigned Profile           |
| Would sell under stress | May need money soon              | Conservative               |
| Would stay the course   | Has other resources              | Moderate                   |
| Would buy the dip       | Long horizon, no near-term needs | Aggressive                 |
| Contradictory signals   | High capacity but low tolerance  | Moderate-conservative      |

Adjust the profile based on financial capacity signals from Q04 (emergency fund), Q03 (debt), and Q10 (horizon).

---

## PHASE 4 — Investment Plan

### Q10 — Investment Horizon
"How long are you planning to invest for — less than 1 year, 1 to 3 years, 3 to 5 years, or more than 5 years?"

### Q11 — Investment Amount
"How much are you looking to invest each month, in [currency symbol]?"

### Q12 — Investment Goals
"What's the main goal for this money — for example, saving for a home, retirement, education, a car,
building long-term wealth, or something else?"

### Q13 — Asset Classes to Avoid
"Are there any types of investments you'd like to avoid entirely? For example, cryptocurrency, stocks,
or anything else — or are you open to everything?"

---

## PHASE 5 — Life Situation & Dependents

### Q14 — Dependents
"Do you have anyone who depends on your income financially — children, a partner who doesn't work,
aging parents, or anyone like that?"

- No dependents → Record has_dependents=false. Higher risk capacity permitted. Proceed to Phase 6.
- Children or dependents → Record has_dependents=true. Proceed to Q14b.
- Multiple dependents → Record. Significant risk capacity reduction. Proceed to Q14b.

### Q14b — Life Insurance (CONDITIONAL — only if Q14 = dependents present)
"Do you have life insurance that would replace your income if something happened to you?"

- Yes, adequate policy → Record has_life_insurance=true.
- Employer coverage only / unsure if enough → Record has_life_insurance=false.
  Acknowledge: "Employer coverage is usually 1-2x salary — often not enough for long-term dependents. Worth reviewing."
- No life insurance → Record has_life_insurance=false. FLAG as critical priority.
  Acknowledge: "I'd flag that as something to address before going aggressive with investing.
  Term life insurance is usually very affordable."
→ Proceed to Phase 6.

---

## PHASE 6 — Investment Preferences & Values

Transition line: "Almost there — just a couple of questions about what your money is actually invested in."

### Q15 — ESG / Values-Based Investing
"Is it important to you that your investments align with your personal values — for example, avoiding
fossil fuel companies or weapons manufacturers? Or do you just want the best returns regardless?"

- No preference → Record esg_preference="none". Standard index funds.
- Light preference → Record esg_preference="light". Note ESG alternatives (e.g. ESGV instead of VTI).
- Strong preference → Record esg_preference="full". Full ESG portfolio needed.
- Specific exclusions → Note them. Record esg_preference="custom".

### Q16 — Involvement Level
"Once your portfolio is set up, how involved do you want to be in managing it —
fully hands-off, occasional check-ins, or do you want to make decisions yourself?"

- Hands-off → Record involvement_level="hands-off". Target-date or single-fund solution.
- Occasional → Record involvement_level="occasional". Three-fund or ETF portfolio.
- Active → Record involvement_level="active". Custom ETF portfolio.
- Full control → Record involvement_level="diy". DIY framework; flag risk of overtrading.

---

## PHASE 7 — Financial Literacy

### Q17 — Knowledge Level
"Last question, I promise. How would you describe your knowledge of personal finance and investing —
are you pretty new to this, or do you have some experience?"

- Complete beginner → knowledge_level="beginner". Full explanations, no jargon.
- Know the basics → knowledge_level="basic".
- Know stocks/bonds/ETFs → knowledge_level="intermediate".
- Know asset allocation/expense ratios → knowledge_level="advanced".
- Finance background → knowledge_level="expert". Data-forward, skip basics.

---

## WRAP-UP & RECOMMENDATION

After Q17, say:
"That's everything I need — you've been really thorough. Give me a moment and I'll pull together a
personalised recommendation based on everything you've shared."

Then summarise the user's profile in 2-3 sentences and ask them to confirm before calculating.

Once confirmed, call calculateAssetAllocation with ALL collected fields.

When the tool responds, deliver the recommendation in this order:
1. ADDRESS RED FLAGS FIRST (if any): high-interest debt payoff priority, emergency fund gap,
   missing life insurance for dependents. Be warm but clear.
2. PROPOSE ASSET ALLOCATION with specific fund examples (calibrated to risk profile, country,
   currency, ESG preference, and knowledge level).
3. ADDRESS SEPARATE GOAL BUCKETS if multiple goals mentioned.
4. OFFER to explain anything in more detail.

End with the standard disclaimer from the tool result.

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


def get_portfolio_recommendations(
    alloc: dict, currency: str, esg_preference: str = "none"
) -> dict:
    recs = {}
    for asset_class, pct in alloc.items():
        if pct <= 0:
            continue
        options = PORTFOLIO_OPTIONS.get(asset_class, {})
        instruments = options.get(currency, options.get("DEFAULT", []))
        # For ESG preferences on Equities, prefer ESG-labelled instruments
        if esg_preference in ("light", "full", "custom") and asset_class == "Equities":
            esg_instruments = [
                i
                for i in instruments
                if "ESG" in i.get("name", "") or "SRI" in i.get("name", "")
            ]
            if esg_instruments:
                instruments = esg_instruments + [
                    i for i in instruments if i not in esg_instruments
                ]
        recs[asset_class] = instruments[:3]
    return recs


def _build_flags(profile: dict) -> list:
    """Build a list of priority flags based on the enriched profile."""
    flags = []

    # High-interest debt flag
    if profile.get("high_interest_debt"):
        rate = profile.get("debt_rate_pct", 0)
        balance = profile.get("debt_balance", 0)
        rate_str = f" at ~{rate:.0f}%" if rate else ""
        bal_str = (
            f" ({profile.get('currency_symbol','')}{balance:,.0f})" if balance else ""
        )
        flags.append(
            f"HIGH_INTEREST_DEBT{bal_str}{rate_str}: Pay down before aggressive investing — guaranteed return on every dollar."
        )

    # Emergency fund flag
    ef_months = float(profile.get("emergency_fund_months", 3))
    if ef_months < 3:
        if ef_months == 0:
            flags.append(
                "NO_EMERGENCY_FUND: Build a 3-month cash buffer before investing — protect against forced asset sales."
            )
        else:
            flags.append(
                f"LOW_EMERGENCY_FUND ({ef_months:.0f} months): Target is 3-6 months. Consider building it before going all-in."
            )

    # Life insurance flag (only relevant if dependents)
    if profile.get("has_dependents") and not profile.get("has_life_insurance"):
        flags.append(
            "NO_LIFE_INSURANCE + DEPENDENTS: Address life insurance before increasing investment risk. Term life is usually very affordable."
        )

    return flags


def _calculate_allocation(profile: dict) -> dict:
    risk = profile.get("risk_appetite", "moderate").lower()
    years = float(profile.get("investment_period_years", 5))
    debt = float(profile.get("total_debt", 0))
    inflow = float(profile.get("monthly_inflow", 0))
    invest = float(profile.get("investment_amount", 0))
    currency = profile.get("currency_code", "USD")
    symbol = profile.get("currency_symbol", "$")
    avoid = [a.lower() for a in profile.get("avoid_asset_classes", [])]
    esg_preference = profile.get("esg_preference", "none")

    # Resolve risk from behavioral profile + financial capacity signals
    ef_months = float(profile.get("emergency_fund_months", 3))
    has_dependents = profile.get("has_dependents", False)
    high_interest_debt = profile.get("high_interest_debt", False)

    # Downgrade risk if financial safety net is weak
    if ef_months < 3 or high_interest_debt:
        if risk == "high":
            risk = "moderate"
        elif risk == "moderate":
            risk = "low"

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

    # Dependents: nudge toward conservative
    if has_dependents:
        shift = 5
        alloc["Bonds / Fixed Income"] = min(alloc["Bonds / Fixed Income"] + shift, 60)
        alloc["Equities"] = max(alloc["Equities"] - shift, 0)

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
    recommendations = get_portfolio_recommendations(
        active_alloc, currency, esg_preference
    )

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

    # ESG note in rationale
    if esg_preference in ("light", "full", "custom"):
        rationale += f" ESG-screened alternatives have been prioritised where available ({esg_preference} preference)."

    # Build flags
    flags = _build_flags(profile)

    return {
        "profile_summary": {
            "name": profile.get("full_name", "Client"),
            "country": profile.get("canonical_country", ""),
            "currency": f"{currency} ({symbol})",
            "risk_appetite": risk.capitalize(),
            "invest_monthly": f"{symbol}{invest:,.2f}",
            "period_years": years,
            "goals": profile.get("investment_goals", []),
            "knowledge_level": profile.get("knowledge_level", "intermediate"),
            "involvement_level": profile.get("involvement_level", "occasional"),
            "esg_preference": esg_preference,
        },
        "priority_flags": flags,
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
    flags = result.get("priority_flags", [])

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
        # New fields
        "high_interest_debt": profile.get("high_interest_debt", False),
        "debt_balance": profile.get("debt_balance", 0),
        "debt_rate_pct": profile.get("debt_rate_pct", 0),
        "emergency_fund_months": profile.get("emergency_fund_months", 0),
        "has_employer_match": profile.get("has_employer_match", False),
        "employer_match_details": profile.get("employer_match_details", ""),
        "has_dependents": profile.get("has_dependents", False),
        "has_life_insurance": profile.get("has_life_insurance", False),
        "esg_preference": profile.get("esg_preference", "none"),
        "involvement_level": profile.get("involvement_level", ""),
        "knowledge_level": profile.get("knowledge_level", ""),
        # Existing fields
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
        "flags": " | ".join(flags) if flags else "",
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

    # ── Updated schema: all original fields + new fields from design.md ──
    _ASSET_ALLOC_SCHEMA = json.dumps(
        {
            "type": "object",
            "properties": {
                # Original fields
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
                # New: Phase 1 — Financial Foundations
                "high_interest_debt": {
                    "type": "boolean",
                    "description": "Whether the user has any high-interest debt (>~8% rate)",
                },
                "debt_balance": {
                    "type": "number",
                    "description": "Approximate balance of high-interest debt",
                },
                "debt_rate_pct": {
                    "type": "number",
                    "description": "Approximate interest rate on high-interest debt",
                },
                "emergency_fund_months": {
                    "type": "number",
                    "description": "Months of expenses covered by accessible savings (0 if none)",
                },
                "has_employer_match": {
                    "type": "boolean",
                    "description": "Whether the employer offers a 401k/pension match",
                },
                "employer_match_details": {
                    "type": "string",
                    "description": "Description of the match if known (e.g. '50% up to 6%')",
                },
                # New: Phase 5 — Life Situation
                "has_dependents": {
                    "type": "boolean",
                    "description": "Whether the user has financial dependents (children, spouse, parents)",
                },
                "has_life_insurance": {
                    "type": "boolean",
                    "description": "Whether the user has adequate life insurance coverage",
                },
                # New: Phase 6 — Preferences
                "esg_preference": {
                    "type": "string",
                    "enum": ["none", "light", "full", "custom"],
                    "description": "User's ESG / values-based investing preference",
                },
                "involvement_level": {
                    "type": "string",
                    "enum": ["hands-off", "occasional", "active", "diy"],
                    "description": "How involved the user wants to be in managing the portfolio",
                },
                # New: Phase 7 — Knowledge Level
                "knowledge_level": {
                    "type": "string",
                    "enum": ["beginner", "basic", "intermediate", "advanced", "expert"],
                    "description": "User's self-assessed financial literacy level",
                },
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
                                            "the user for currency. Call this tool ONLY after all "
                                            "questions are answered across all phases and the user "
                                            "confirms the summary. Include all new fields collected "
                                            "during the conversation (emergency_fund_months, "
                                            "has_dependents, esg_preference, knowledge_level, etc.)."
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
