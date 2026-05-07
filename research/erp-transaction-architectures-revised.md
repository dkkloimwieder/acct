# Enterprise Resource Planning Transactional Architectures and General Ledger Mechanisms

**A Corrected and Expanded Reference, with a Proposed Unified Model**

*Comparative analysis of SAP S/4HANA, Oracle Fusion Cloud ERP, Microsoft Dynamics 365 Finance & Operations, and Oracle NetSuite. Revised May 2026.*

---

## 1. Preface and Scope

This document is a corrected and substantially expanded successor to an earlier comparative analysis of ERP transactional architectures. It does three things:

1. **Corrects** the factual errors and sourcing problems identified in the prior version — most notably the conflation of legacy Oracle E-Business Suite event types with modern Oracle Fusion Cloud, the understated number of Microsoft Dynamics 365 posting layers, and the contamination of the SAP "standard document types" list with industry-specific custom values.
2. **Enumerates** transaction types, accounting events, journal categories, and GL impacts comprehensively across all four platforms, drawing on current vendor documentation (SAP Help Portal, Oracle Cloud Documentation, Microsoft Learn, NetSuite Application Help) rather than third-party blogs.
3. **Synthesizes** a unified taxonomy of business events that any modern ERP should be expected to handle, organized by economic substance rather than by vendor terminology. This last section is the analytical payoff: a reference model that names what the same accounting event is called in each system, identifies the GL pattern, and proposes a minimum capability set.

A note on terminology: the four systems use the same English words to mean meaningfully different things. "Document type" in SAP is a header classification driving number ranges and posting permissions; in NetSuite, the closest equivalent is "transaction type." "Journal" in Dynamics 365 is a workspace where many transactions accumulate before posting; in Oracle Fusion, it is the SLA output. Where the words diverge, this document uses the system's native term in italics for clarity.

---

## 2. Architectural Foundations

Modern ERP systems differ less in *what* they record than in *where* the accounting logic lives between operational origin and the general ledger. Four philosophically distinct approaches dominate the enterprise market.

**SAP S/4HANA — Universal Journal (table ACDOCA).** Every posting — financial, controlling, asset accounting, material ledger, profitability analysis — writes to a single line-item table. The reconciliation between FI and CO that defined classic SAP ERP is eliminated by data model design, not by background jobs. Parallel accounting is supported via standard parallel ledgers (leading + non-leading) and, since S/4HANA 1709, **extension ledgers** that record only delta entries on top of a base ledger for efficient adjustments. Universal Parallel Accounting (UPA), introduced more recently, allows ledger-specific values across cost management, asset management, and material ledger — not just GL.

**Oracle Fusion Cloud ERP — Subledger Accounting (SLA) engine.** Operational subledgers raise *accounting events*, classified into *event entities → event classes → event types*. The SLA engine intercepts events, evaluates *Journal Entry Rule Sets* composed of *Journal Line Rules* and *Account Rules*, and posts to one or many ledgers. The same operational event can therefore be accounted differently in primary, secondary, and reporting ledgers without re-entering data. This is the most decoupled architecture of the four; it is also the most configuration-heavy.

**Microsoft Dynamics 365 Finance & Operations — Posting Profiles + Posting Definitions, plus 10 Posting Layers.** *Posting profiles* derive ledger accounts via a cascading hierarchy (Table → Group → All) keyed off the entity (vendor, customer, item, asset, project) involved. *Posting definitions* are an optional second mechanism, primarily used for U.S. public-sector encumbrance accounting and certain bank scenarios, that supports more complex line-by-line account derivation. The ten posting layers — **Current, Operations, Tax, and Custom Layer 1 through 7** — let a single transaction land in different layers for IFRS, local GAAP, tax, and management views.

**Oracle NetSuite — GL Impact Page + SuiteGL.** Every posting transaction has a "GL Impact" page accessible directly from the record, derived primarily from item-level account assignments, entity-level posting accounts, accounting preferences, and tax codes. NetSuite's general ledger is *not* hard-coded — it is configurable through the **SuiteGL** framework, which has three layers: the *Custom GL Lines plug-in* (SuiteScript that adds GL lines to standard transactions, e.g. for Brazilian tax requirements), *Custom Transaction Types* (define new transaction records with their own GL behavior, statuses, and workflows), and *Custom GL Segments* (additional reporting dimensions beyond the core class/department/location/subsidiary).

The architectural distinction matters because it determines who controls accounting outcomes. In SAP, a configuration consultant working with Document Types and account determination tables (T030, OBYC, etc.) decides where everything posts. In Oracle Fusion, an SLA specialist owning rule sets does. In Dynamics 365, a finance functional consultant maintaining posting profiles does. In NetSuite, GL outcomes are influenced by a much wider population — anyone who can configure an item record or accounting preference — which is a strength for agility and a risk for governance.

---

## 3. SAP S/4HANA

### 3.1 Architectural notes

SAP S/4HANA targets large global enterprises, particularly those with deep manufacturing, complex supply chains, and high transaction volumes. Three S/4HANA-specific data model choices shape its transactional behavior:

- **ACDOCA (the Universal Journal line-item table)** consolidates the line items previously stored in BSEG (FI), COEP (CO), ANEP/ANEA (FI-AA), MLIT (ML), and CE1xxxx (CO-PA). All actuals — financial, controlling, asset accounting, material ledger — share one table.
- **Parallel ledgers** record different valuations under different accounting principles. Each company code has one *leading ledger* (typically `0L`, IFRS or group GAAP) and zero or more *non-leading ledgers* (e.g., local GAAP, tax). All ledgers are complete — every relevant posting flows to all of them.
- **Extension ledgers** (since SAP S/4HANA 1709) store only delta postings on top of an underlying base ledger. They are used for management adjustments, predictive accounting (commitments), and IFRS-vs-US-GAAP delta reporting. They have limitations: no postings to vendor/customer reconciliation accounts, no postings to GL accounts with open-item management, and limited integration with asset accounting.

The transaction "code" (T-code) is the user-interface entry point in SAP. The **document type** (a 2-character key on the FI document header) is what classifies the posting for number range, account types allowed, and reporting. The **movement type** (a 3-digit key) classifies the inventory movement and drives automatic account determination via OBYC. The **CO business transaction** (stored in table TJ01) classifies the controlling event.

### 3.2 Standard FI Document Types

The list below is the SAP-delivered standard set. Customer-defined document types — by SAP naming convention any document type starting with the letter `Z` or `Y` — are not standard and should not appear in a comparative reference.

| Doc type | Description | Typical GL impact |
|---|---|---|
| AA | Asset Posting | Dr/Cr Fixed Asset subledger; auto-posts to Asset Capitalization / Reconciliation accounts |
| AB | Accounting Document (general) | Manual/general; allows posting to all account types — the most permissive doc type |
| AF | Depreciation Postings | Dr Depreciation Expense; Cr Accumulated Depreciation |
| AN | Net Asset Posting | Asset acquisition net of input tax |
| AP | Periodic Asset Posting | Periodic asset run (e.g., investment support) |
| AZ | Asset Disposals | Dr Accumulated Depreciation, Loss on Disposal; Cr Asset Cost; net to Cash/Receivable |
| CO | Secondary Cost (CO-internal) | CO-only postings (allocations, settlements) — secondary cost elements, no FI counterpart in the legal ledger |
| DA | Customer Document | Customer-side miscellaneous |
| DG | Customer Credit Memo | Dr Sales Returns/Revenue contra; Cr Accounts Receivable |
| DR | Customer Invoice | Dr Accounts Receivable; Cr Revenue (and Output Tax) |
| DZ | Customer Payment | Dr Cash/Bank; Cr Accounts Receivable |
| EU | Euro Rounding Differences | Currency rounding adjustments |
| EX | External Number | External numbering for interfaced documents |
| KA | Vendor Document | Vendor-side miscellaneous |
| KG | Vendor Credit Memo | Dr Accounts Payable; Cr Expense / Inventory contra |
| KN | Net Vendors | Vendor invoice net of cash discount |
| KP | Account Maintenance (Vendor) | Vendor open-item maintenance |
| KR | Vendor Invoice | Dr Expense / Inventory / GR/IR; Cr Accounts Payable |
| KZ | Vendor Payment | Dr Accounts Payable; Cr Cash/Bank |
| ML | Material Ledger Settlement | Material ledger periodic settlement; price differences flowing to inventory |
| PR | Price Change | Inventory revaluation |
| RA | Subsequent Credit Memo Settlement | AP/AR clearing |
| RE | Invoice Receipt (Gross) | Logistics invoice verification — Dr GR/IR; Cr AP (gross) |
| RN | Invoice Receipt (Net) | Logistics invoice verification (net method) |
| RV | Billing Document Transfer | Billing transfer from SD to FI: Dr AR; Cr Revenue |
| SA | G/L Account Document | Standard G/L journal entry |
| SB | G/L Account Posting (system) | System-generated G/L document |
| SK | Cash Document | Cash receipt / disbursement |
| SU | Subsequent Debit Document | Subsequent debit on AP |
| UE | Data Transfer | Data takeover / migration |
| WA | Goods Issue | Dr COGS / Expense; Cr Inventory |
| WE | Goods Receipt | Dr Inventory; Cr GR/IR |
| WI | Inventory Document | Physical inventory adjustment |
| WL | Goods Issue / Delivery | SD goods issue: Dr COGS; Cr Inventory |
| WN | Net Goods Receipt | GR with net-method invoice handling |
| X1 | Recurring Entry Document | Template for recurring postings (no GL impact until executed) |
| X2 | Sample Document | Template only, never posts |

Note on conventions: every document type also has a *reverse document type* (e.g., KR reversed by KA, AB reversed by AB, AF reversed by AF), and customers commonly extend the standard set with `Z`-prefix types for industry or regulatory needs (payroll postings, treasury, public-sector funds, etc.) — these belong in customer-specific documentation, not in a vendor-comparative reference.

### 3.3 SD Billing Document Types (selected)

Billing documents in SD generate FI documents (typically `RV`) but carry their own typing for the sales side:

| SD billing type | Description | FI consequence |
|---|---|---|
| F1 | Order-related invoice | Dr AR; Cr Revenue, Cr Output Tax |
| F2 | Delivery-related invoice (standard) | Dr AR; Cr Revenue, Cr Output Tax |
| F8 | Pro forma invoice | Non-posting |
| G2 | Credit memo | Dr Revenue contra; Cr AR |
| L2 | Debit memo | Dr AR; Cr Revenue |
| RE | Returns | Dr Revenue contra; Cr AR (paired with goods receipt of returns) |
| S1 | Cancellation invoice | Reverses F1/F2 |
| IV | Intercompany billing | Cross-company AR/AP |

### 3.4 MM Movement Types

Movement types drive the automatic account determination logic in OBYC (transaction key + valuation class + movement type → GL account). Every movement type has a reversal (typically the next even number — e.g., 101 reversed by 102, 201 by 202, 601 by 602).

**Goods receipts (1xx)**

| MT | Description | GL impact |
|---|---|---|
| 101 | GR for purchase order, into unrestricted | Dr Inventory (BSX); Cr GR/IR (WRX) |
| 102 | Reversal of 101 | Reverses 101 |
| 103 | GR into blocked stock (no value posted yet) | None — unvalued blocked stock |
| 105 | Release from blocked stock to unrestricted | Dr Inventory; Cr GR/IR |
| 122 | Return delivery to vendor | Dr GR/IR; Cr Inventory |
| 123 | Reversal of 122 | Reverses 122 |
| 161 | Returns to vendor (PO referenced as returns item) | Dr GR/IR; Cr Inventory |
| 162 | Reversal of 161 | Reverses 161 |

**Goods issues (2xx)**

| MT | Description | GL impact |
|---|---|---|
| 201 | GI to cost center | Dr Department Expense (GBB-VBR); Cr Inventory |
| 221 | GI to project (WBS) | Dr Project Cost; Cr Inventory |
| 231 | GI to sales order | Dr Cost on sales order; Cr Inventory |
| 241 | GI to asset | Dr Asset under Construction; Cr Inventory |
| 261 | GI to production order | Dr WIP / Order; Cr Inventory |
| 281 | GI to network | Dr Network/Project; Cr Inventory |
| 291 | GI to all account assignments | Dr Expense (per assignment); Cr Inventory |

**Stock transfers (3xx, 4xx)**

| MT | Description | GL impact |
|---|---|---|
| 301 | Plant-to-plant transfer, one step | Inventory movement; valuation change if plants have different prices |
| 303 | Plant-to-plant transfer, two-step (issue) | Dr Stock-in-Transit; Cr Sending Plant Inventory |
| 305 | Plant-to-plant transfer, two-step (receipt) | Dr Receiving Plant Inventory; Cr Stock-in-Transit |
| 309 | Material-to-material transfer | Issue from material A and receipt to material B |
| 311 | Storage location to storage location, one-step | Internal — no FI posting |
| 313 / 315 | Storage location transfer, two-step (issue / receipt) | Internal stock-in-transit |
| 411 | Transfer special stock to own | Dr Own Inventory; Cr Special Stock |
| 413 | Transfer to sales order stock | Movement to project/sales-order stock |

**Customer returns without shipping (45x)**

| MT | Description | GL impact |
|---|---|---|
| 451 | Customer return without SD delivery, into blocked-stock-returns | Returns into unvalued blocked stock |
| 453 | Transfer blocked-stock-returns → unrestricted | Dr Inventory; Cr Cost of Returns |
| 455 | Storage-location-to-storage-location of blocked stock returns | Internal location move |
| 457 | Transfer blocked-stock-returns → quality inspection | Move to QI valued stock |
| 459 | Transfer blocked-stock-returns → blocked stock | Move to blocked valued stock |

Note: the previous reference's grouping treated 451/453/455/457/459 as a single "stock-status transfer" family. They are not. 455 specifically is a *location-to-location* transfer of blocked stock returns and does not transfer status — distinct from 453/457/459 which transfer status.

**Scrapping and adjustments (5xx)**

| MT | Description | GL impact |
|---|---|---|
| 501 | GR without PO into unrestricted | Dr Inventory; Cr Income from goods received without PO |
| 503 | GR without PO into QI | Dr Inventory (QI); Cr Income |
| 521 | GR without production order into unrestricted | Dr Inventory; Cr Variance |
| 531 | GR by-product from production order | Dr Inventory (by-product); Cr Order |
| 541 / 543 | Stock transfer to subcontractor / consumption from subcontracting | Subcontracting moves |
| 551 | Scrapping from unrestricted | Dr Scrap Expense (GBB-VNG); Cr Inventory |
| 553 | Scrapping from QI | As 551, from QI valuation |
| 555 | Scrapping from blocked | As 551, from blocked valuation |
| 561 | Initial stock entry | Dr Inventory; Cr Initial Stock Income |

**Shipping / customer (6xx)**

| MT | Description | GL impact |
|---|---|---|
| 601 | GI for delivery | Dr COGS (GBB-VAX/VAY); Cr Inventory |
| 603 / 605 | Stock transfer in transit between plants (issue / receipt) | Stock-in-transit handling |
| 641 | GI for stock transport order (single step) | Dr Receiving Inventory; Cr Sending Inventory |
| 643 | GI for cross-company STO | Cross-company AR/AP through internal billing |
| 645 / 647 | Cross-company STO with shipping (issue and receipt) | Dr/Cr Inventory across company codes |
| 651 | Customer return via SD delivery, into blocked-stock-returns | Unvalued blocked-stock-returns |
| 653 | Customer return into unrestricted | Dr Inventory; Cr COGS |
| 655 | Customer return into QI | Dr Inventory (QI); Cr COGS |
| 657 | Customer return into blocked | Dr Blocked Inventory; Cr COGS |
| 661 | Returns to vendor via SD delivery | Dr GR/IR; Cr Inventory |
| 671 / 673 / 675 / 677 | Returns for stock transport order (variants) | STO-returns variants |

**Physical inventory (7xx)** — these are *automatic* movement types triggered by physical inventory difference posting:

| MT | Description | GL impact |
|---|---|---|
| 701 | GR from physical inventory in unrestricted (count > book) | Dr Inventory; Cr Inventory Adjustment Income (GBB-INV) |
| 702 | GI to physical inventory in unrestricted (count < book) | Dr Inventory Adjustment Expense (GBB-INV); Cr Inventory |
| 703 / 704 | GR / GI from physical inventory in QI | As 701/702 for QI |
| 707 / 708 | GR / GI from physical inventory in blocked | As 701/702 for blocked |
| 711 / 712 | Warehouse Management physical inventory variants | WM equivalents |

### 3.5 CO Business Transactions

Controlling events are typed by *business transactions* (table TJ01), not by FI document types. The set below covers the most common; both *Plan* and *Actual* counterparts exist for most transactions. The previous reference enumerated only the Plan side.

**Plan (planning postings)**

| Code | Description |
|---|---|
| CPPP | ABC Process Assessment (Plan) |
| FIPA | Automatic Payment Schedule |
| JVPL | JV Planning Data Document |
| KAZP | Plan Cost Center Accrual |
| KOAP | Plan Settlement |
| KPPB | Standard Cost Estimate |
| KSP0 | Plan Splitting |
| KSPB | Plan Assessment to PA |
| KZPP | Plan Periodic Overhead |
| KZRP | Plan Interest Calculation |
| PAPL | Plan Sales / Profit (profit planning) |
| RKPB | Plan Periodic Reposting |
| RKPL | Plan Indirect Activity Allocation |
| RKPP | Primary Planning with Template |
| RKPQ | Manual Cost Planning |
| RKPS | Secondary Planning with Template |
| RKPU | Plan Overhead Cost Assessment |
| RKPV | Plan Overhead Cost Distribution |

**Actual (postings against actuals)**

| Code | Description |
|---|---|
| RKL | Actual Activity Allocation (confirmation postings) |
| RKLN | Sender-determined activity allocation |
| RKIB | Actual Periodic Repostings |
| RKIL | Actual Indirect Activity Allocation |
| RKIU | Actual Assessment |
| RKIV | Actual Distribution |
| RKU1 | Repostings (manual) |
| RKU2 | Reposting Costs |
| RKU3 | Reposting Revenues |
| KAZI | Actual Cost Center Accrual |
| KSWP | Statistical Key Figure Postings |
| KOAO | Actual Settlement |

In S/4HANA, secondary cost elements were unified with G/L accounts — they are now G/L accounts of category 21 (Secondary Costs) — and CO postings update ACDOCA directly, alongside primary FI postings. Many of the classic CO planning transactions above were modernized or replaced by Fiori-based planning apps, with planning data now stored in `ACDOCP` rather than the legacy COSP/COSS structures.

### 3.6 Asset Accounting transaction types (FI-AA)

| Type | Description | GL impact |
|---|---|---|
| 100 / 105 / 110 | External asset acquisition (with vendor) | Dr Asset; Cr AP / Asset Clearing |
| 120 | Acquisition with affiliated company | Dr Asset; Cr Intercompany |
| 200 / 210 | Retirement (with revenue / scrap) | Dr Accumulated Depr, Loss on Disposal; Cr Asset Cost |
| 300 / 320 | Transfer (within company / cross-company) | Dr/Cr Asset accounts of receiving/sending |
| 400 / 410 | Acquisition of asset under construction | Dr AuC; Cr AP |
| 700 | Investment support (subsidy received) | Dr AP; Cr Investment Support |
| 900 | Settlement of AuC to fixed asset | Dr Final Asset; Cr AuC |

Depreciation is posted by the program `RAPOST2000` / Fiori app *Post Depreciation*, generating document type `AF`.

### 3.7 Revenue recognition and lease accounting

- **SAP Revenue Accounting and Reporting (RAR)** handles ASC 606 / IFRS 15 contract-based revenue recognition. Events include *Contract Created*, *Performance Obligation Fulfilled*, *Revenue Recognized*, *Contract Modified*, *Revenue Reclassified*. Revenue postings flow into ACDOCA. RAR is being progressively replaced by SAP S/4HANA's native event-based revenue recognition for service and project industries.
- **SAP Lease Accounting (FI-LA)** handles ASC 842 / IFRS 16 lessee and lessor accounting. Events include *Right-of-Use Asset Recognition*, *Lease Liability Recognition*, *Interest Accrual*, *Amortization*, *Remeasurement*, *Modification*, *Termination*, *Sublease Recognition*.

---

## 4. Oracle Fusion Cloud ERP

### 4.1 Architectural notes

Oracle Fusion Cloud ERP — distinct from the legacy Oracle E-Business Suite, with which it shares only branding and some terminology — uses a **rule-based Subledger Accounting (SLA) engine** sitting between operational subledgers and the General Ledger. The hierarchy is:

- **Event entity** — a grouping of related events (e.g., *Invoices*, *Receipts*, *Cash Flows*, *Transactions*, *Depreciation*)
- **Event class** — a category of business event within the entity (e.g., *Invoice*, *Credit Memo*, *Receipt*, *Adjustment*)
- **Event type** — a specific lifecycle state of the class (e.g., *Invoice Validated*, *Invoice Adjusted*, *Invoice Canceled*)

When a subledger (Payables, Receivables, Cost Management, Assets, etc.) commits a transaction, it raises an accounting event of a specific type. The SLA engine evaluates the *Subledger Accounting Method* assigned to each ledger, finds the matching *Journal Entry Rule Set* for that event class, applies the constituent *Journal Line Rules* and *Account Rules*, and produces journal entries — possibly different ones for the primary, secondary, and reporting ledgers attached to the same business unit.

This is the most decoupled architecture of the four. The trade-off: troubleshooting a wrong account requires tracing event → rule set → line rule → account rule → mapping set, possibly across multiple ledgers.

The lists below reflect the **current Oracle Fusion Cloud** event classes and types, drawn from Oracle's 25B/26B Cloud documentation. They are *not* the same as the legacy Oracle E-Business Suite event types (which were named in `UPPER_CASE_UNDERSCORED` style, e.g., `CASH_APPLIED`, `MEMO_APPLICATION`, `MISC_INSERT`). Citing those legacy names for a Fusion Cloud system is a substantive error.

### 4.2 Payables event classes and types

| Event class | Event types |
|---|---|
| Adjustment Entry | Manual |
| Bills Payable | Bill Payable Matured; Bill Payable Maturity Adjusted; Bill Payable Maturity Reversed |
| Credit Memos | Credit Memo Validated; Credit Memo Adjusted; Credit Memo Canceled |
| Debit Memos | Debit Memo Validated; Debit Memo Adjusted; Debit Memo Canceled |
| Escheated Payments | Payment Escheated |
| Invoices | Invoice Validated; Invoice Adjusted; Invoice Canceled |
| Payments | Payment Created; Payment Adjusted; Payment Canceled; Manual Payment Adjusted |
| Prepayment Applications | Prepayment Applied; Prepayment Unapplied; Prepayment Application Adjusted |
| Prepayments | Prepayment Validated; Prepayment Adjusted; Prepayment Canceled |
| Reconciled Payments | Payment Cleared; Payment Uncleared; Payment Clearing Adjusted |
| Refunds | Refund Recorded; Refund Adjusted; Refund Canceled |
| Third-Party Merge | Full Merge; Partial Merge |

**Typical GL patterns:**
- *Invoice Validated* → Dr Expense / Inventory / Asset Clearing (depending on distribution); Cr Liability
- *Payment Created* → Dr Liability; Cr Cash Clearing (or Cash, if no clearing)
- *Payment Cleared* → Dr Cash Clearing; Cr Cash
- *Prepayment Validated* → Dr Prepayment Asset; Cr Liability
- *Prepayment Applied* → Dr Liability (on invoice); Cr Prepayment Asset
- *Bill Payable Matured* → Dr Liability; Cr Bills Payable

### 4.3 Receivables event classes and types

| Event class | Event types |
|---|---|
| Adjustment | Adjustment Created; Adjustment Reversed; Adjustment Updated |
| Bills Receivable | Bill Created; Bill Endorsed; Bill Factored; Bill Holding; Bill Remitted; Bill Reversed; Bill Risk Eliminated; Bill Unpaid; Bill Written-Off |
| Chargeback | Chargeback Created; Chargeback Updated |
| Credit Memo | Credit Memo Created; Credit Memo Updated; Credit Memo Reversed |
| Debit Memo | Debit Memo Created; Debit Memo Updated |
| Invoice | Invoice Created; Invoice Updated |
| Miscellaneous Receipt | Miscellaneous Receipt Created; Miscellaneous Receipt Reversed; Miscellaneous Receipt Updated |
| Receipt | Receipt Created; Receipt Reversed; Receipt Updated; Receipt Application |

**Typical GL patterns:**
- *Invoice Created* → Dr Receivable; Cr Revenue, Cr Tax, Cr Freight (per AutoAccounting / SLA rules)
- *Receipt Created* → Dr Cash / Confirmation / Remittance; Cr Receivable (or Unapplied if not yet matched)
- *Miscellaneous Receipt Created* → Dr Cash; Cr Activity Account (per Receivables Activity setup)
- *Adjustment Created* → Dr/Cr Receivable against Adjustment Activity account
- *Chargeback Created* → Dr Receivable (new chargeback); Cr Receivable (cleared invoice)
- *Bill Factored* → Dr Cash, Loss on Sale; Cr Receivable, Bill Liability

### 4.4 Assets event classes and types

| Event entity | Event class | Event types |
|---|---|---|
| Transactions | Additions | Addition |
|  | Adjustments | Adjustment |
|  | Capitalization | CIP Reverse; Capitalization |
|  | Category Reclass | Reclass |
|  | CIP Additions | CIP Addition |
|  | CIP Adjustments | CIP Adjustment |
|  | CIP Category Reclass | CIP Reclass |
|  | CIP Retirements | CIP Retirement; CIP Reinstatement |
|  | CIP Revaluation | CIP Revaluation |
|  | CIP Transfers | CIP Transfer |
|  | CIP Unit Adjustments | CIP Unit Adjustment |
|  | Depreciation Adjustments | Depreciation Adjustment |
|  | Retirement Adjustments | Retirement Adjustment |
|  | Retirements | Retirement; Reinstatement |
|  | Revaluation | Revaluation |
|  | Terminal Gain and Loss | Terminal Gain Loss |
|  | Transfers | Transfer |
|  | Unit Adjustments | Unit Adjustment |
|  | Unplanned Depreciation | Unplanned Depreciation |
| Depreciation | Depreciation | Depreciation |
|  | Rollback Depreciation | Rollback Depreciation |
| Inter Asset Transactions | Source Line Transfers | Source Line Transfer |
|  | CIP Source Line Transfers | CIP Source Line Transfer |
|  | Reserve Transfers | Reserve Transfer |
| Deferred Depreciation | Deferred Depreciation | Deferred Depreciation |

**Typical GL patterns:**
- *Addition* → Dr Asset Cost; Cr Asset Clearing (offsets the AP-side asset clearing on the invoice)
- *Depreciation* → Dr Depreciation Expense; Cr Accumulated Depreciation
- *Retirement* → Dr Accumulated Depreciation, Proceeds Clearing, Loss on Disposal; Cr Asset Cost, Cost of Removal Clearing, Gain on Disposal
- *Reinstatement* → reverses Retirement
- *Transfer* → Dr Receiving Cost / Accumulated Depreciation; Cr Sending Cost / Accumulated Depreciation
- *Capitalization* → Dr Asset Cost; Cr CIP Cost (when CIP is placed in service)
- *Revaluation* → adjusts cost and reserve through Revaluation Reserve account

### 4.5 Cost Management and Receipt Accounting

**Receipt Accounting** event classes (subset):

| Event class | GL pattern |
|---|---|
| Receipt into Receiving Inspection | Dr Receiving Inspection; Cr Inventory AP Accrual |
| Delivery to Inventory | Dr Inventory Valuation; Cr Receiving Inspection |
| Delivery to Expense | Dr Expense; Cr Inventory AP Accrual (or Receiving Inspection) |
| Return to Receiving Inspection | Reverses delivery |
| Return to Supplier | Reverses receipt; Dr AP Accrual; Cr Receiving Inspection / Inventory |
| Invoice Price Adjustment | Variance from PO price to invoice — flows to Inventory Valuation or IPV account |
| Retroactive Price Adjustment | Subsequent price changes flowed back to inventory or expense |
| Period End Accrual (for non-accrue-on-receipt items) | Dr Expense; Cr Period-End Accrual (reversed in next period) |
| Period End Uninvoiced Receipt Accrual Clearing | Reverses prior period |

**Cost Accounting** event classes (subset):

| Event class | GL pattern |
|---|---|
| Inventory Receipt / Issue / Transfer | Standard inventory movements with valuation in functional currency and possibly secondary currencies |
| Inventory Adjustment | Dr/Cr Inventory; offset to specified Adjustment account |
| Cost Adjustment | Recosting against an existing transaction — variance to Standard Cost Variance or to inventory under perpetual average |
| Work in Process Issue / Completion | Dr WIP; Cr Inventory (issue); Dr Inventory; Cr WIP (completion) |
| Period-End Material Cost Variance | Capitalized variance allocations for actual costing |
| Overhead Absorption | Dr WIP / Inventory; Cr Overhead Absorbed |
| Standard Cost Update | Inventory revaluation against Standard Cost Adjustment account |
| Consigned Inventory Ownership Transfer | Title transfer event; Dr Inventory; Cr AP Accrual |
| Sales Issue | Dr COGS; Cr Inventory (driven from Order Management shipments) |
| RMA Receipt | Dr Inventory; Cr COGS (reverse) |
| Interorganization Transfer (with markup) | Cross-org transfer accounting through Inter-Organization Receivable / Payable |

### 4.6 Lease Accounting

Oracle Fusion Lease Accounting (a separate cloud module for ASC 842 / IFRS 16) defines five predefined event classes on the Lease Accounting subledger:

| Event class | Description | GL pattern |
|---|---|---|
| Lease Booking | Initial recognition of lease | Dr Right-of-Use Asset; Cr Lease Liability |
| Lease Expense | Periodic accrual of lease expense | Dr Interest Expense, Amortization Expense; Cr Lease Liability, Accumulated Amortization (IFRS 16); single Lease Expense for ASC 842 operating |
| Lease Payment | Cash payment of lease | Dr Lease Liability; Cr Cash / Payables (via Payables integration) |
| Lease Modification / Remeasurement | Change to lease terms | Adjusts ROU Asset and Lease Liability; difference to Reserve / Income |
| Lease Termination | Lease ends early | Dr Lease Liability, Accumulated Amortization; Cr ROU Asset; gain/loss to P&L |

Lease Accounting supports primary and secondary accounting standards (e.g., IFRS 16 primary + ASC 842 secondary) on a single contract by generating parallel postings to different ledgers.

### 4.7 Project Portfolio Management (PPM)

Project Costing and Project Billing each have their own event classes:

**Project Costing event classes (subset):**

| Event class | Event types | GL pattern |
|---|---|---|
| Borrowed and Lent Distribution | Distribution; Adjustment | Cross-organization cost sharing |
| Burden Cost Distribution | Distribution; Adjustment | Indirect cost burden — Dr Burdened Cost; Cr Burden Absorbed |
| Inventory Cost Distribution | Distribution; Adjustment | Project consumption from inventory |
| Labor Cost Distribution | Distribution; Adjustment | Dr Project Labor Cost; Cr Payroll Clearing |
| Miscellaneous Cost Distribution | Distribution; Adjustment | Other project costs |
| Total Burdened Cost Distribution | Distribution; Adjustment | Total cost roll-up |
| Cross-Charge | Borrowed and Lent; Intercompany | Inter-organization charges |
| Capital Project Capitalization | Capitalize; Reverse Capitalization | Dr Asset (in FA); Cr CIP |

**Project Billing event classes (subset):**

| Event class | Event types | GL pattern |
|---|---|---|
| Invoice | Invoice Created; Invoice Adjusted; Invoice Canceled | Dr Receivable; Cr Unbilled Receivable / Revenue |
| Revenue | Revenue Generated; Revenue Adjusted | Dr Unbilled Receivable; Cr Project Revenue |
| Funding | Funding Allocated; Funding Reversed | Award/contract funding tracking |

### 4.8 Cash Management and Bank Account Transfer

| Event class | Event types | GL pattern |
|---|---|---|
| Bank Account Transfer | Settled; Cleared; Cancelled; Uncleared | Dr Cash-in-Transit (intra-company) or Intercompany Receivable; Cr offsetting cash account |
| Bank Statement Cash Flow | Recorded; Reversed | Records bank-originated entries (charges, interest) for reconciliation |
| External Transaction | Created; Reversed | User-entered cash transactions for reconciliation |

### 4.9 Channel Revenue Management (separate module)

Channel Revenue Management is a distinct Oracle Cloud module — *not* a sub-area of Receivables — handling customer rebate accruals and claims:

| Event class | Description |
|---|---|
| Customer Accrual Creation / Adjustment | Accrue customer rebates/incentives — Dr Selling Expense; Cr Accrued Liability |
| Customer Claim Settlement (by Payables / Check / Credit Memo / Extensible Payables / Extensible Receivables) | Pay or net the rebate liability against the appropriate settlement vehicle |
| Customer Accrual Reversal | Reverses unused accrual |

### 4.10 Payroll (Oracle Fusion Cloud HCM)

Payroll uses a parallel SLA model. Event classes include:

| Event class | Event types |
|---|---|
| Run Costs | Costing the payroll run — distributes gross pay, employer taxes, and accruals across cost centers |
| Payment Costs | Costs the actual payment — Dr Net Pay Liability; Cr Cash |
| Estimate Costs | Estimated/predicted costing |
| Partial Period Accruals | Accrues unposted payroll across period boundary |
| Retroactive Costs | Adjustment to prior periods |
| Element Entry events | Insert; Update; Delete; Logical Date Change — drive the line-level changes that downstream costing processes pick up |


---

## 5. Microsoft Dynamics 365 Finance & Operations

### 5.1 Architectural notes

Dynamics 365 Finance & Operations (D365 F&O) — currently delivered as the *Dynamics 365 Finance* and *Dynamics 365 Supply Chain Management* applications, jointly with the *Dynamics 365 Project Operations* add-on for project accounting — uses an account-derivation architecture conceptually distinct from both SAP's strict typing and Oracle Fusion's rule-based engine. Three concepts dominate:

- **Posting Profiles.** For each subledger entity type — vendors, customers, fixed assets, inventory, projects, banks, sales tax — there is a posting profile that maps entities (or groups of entities, or all entities) to GL accounts. The match is *cascading*: the system looks first for a row matching the specific entity (Table relation), then a matching entity group (Group relation), then the catch-all (All relation). This makes the model both flexible and dangerous: heavy reliance on Table-level entries scales poorly and creates maintenance burden.
- **Posting Definitions.** A second, optional account-derivation mechanism, distinct from posting profiles. Posting definitions support more complex line-by-line transformations (e.g., split a single bank transaction into multiple ledger lines based on attributes). They are used predominantly in U.S. public-sector encumbrance accounting, certain bank reconciliation scenarios, and budgeting. Posting definitions are not a replacement for posting profiles — they coexist.
- **Posting Layers.** Every ledger transaction is tagged with one of ten posting layers: **Current, Operations, Tax, Custom Layer 1, Custom Layer 2, Custom Layer 3, Custom Layer 4, Custom Layer 5, Custom Layer 6, Custom Layer 7**. (There is also a "None" pseudo-layer used when fixed asset books explicitly do not post to GL.) Reports can include any combination of layers — e.g., *Current* alone for IFRS, *Current + Operations* for management reporting, *Current + Tax* for tax-basis statutory reporting. This is the parallel-accounting mechanism, equivalent in function to SAP's parallel ledgers and Oracle Fusion's secondary ledgers.

Module abbreviations used in this section: GL (General Ledger), AP (Accounts Payable), AR (Accounts Receivable), FA (Fixed Assets), INV (Inventory), PROJ (Project), GL-CC (Cost Accounting).

### 5.2 Ledger Journal Types

Per Microsoft's documentation, the supported financial journal types include:

| Journal type | Purpose |
|---|---|
| Allocation | Create allocation transactions per a defined Ledger allocation rule |
| Approval | Post approved vendor invoices (used in invoice approval workflows) |
| Bank check reversal | Reverse a posted check (with optional review process) |
| Bank deposit slip cancellation | Cancel a deposit slip |
| Budget | Process budget appropriations (driven by posting definitions) |
| Customer accept bill of exchange | Customer-side BoE acceptance |
| Customer bank remittance | Generate bill-of-exchange remittance file |
| Customer draw bill of exchange | Customer BoE draw |
| Customer payment | Customer payment journal — Dr Cash; Cr AR |
| Customer protest bill of exchange | Protest of unpaid BoE |
| Customer redraw bill of exchange | Re-issue BoE |
| Customer settle bill of exchange | Settle BoE upon maturity |
| Daily | General journal — daily transactions |
| Elimination | Inter-company elimination (used in financial consolidation) |
| Fixed asset budget | Fixed asset budget register entries |
| Invoice register | Pre-register vendor invoices (a workflow stage before approval) |
| Payroll disbursement | Issue payments based on payroll pay statements |
| Periodic | Recurring/periodic ledger entries |
| Post fixed assets | Fixed asset transaction posting |
| Project — expense | Project expense distributions |
| Reporting currency adjustment | Adjustments solely in reporting currency |
| Statistic transactions | Non-financial statistical entries (used for cost/management reporting) |
| Vendor bank remittance | Generate promissory-note remittance file |
| Vendor disbursement | Vendor payment journal — Dr AP; Cr Cash |
| Vendor draw promissory note | Vendor PN draw |
| Vendor invoice pool | Pre-posting invoice pool for review |
| Vendor invoice pool excl. posting | Pool variant that does not post |
| Vendor invoice recording | Record vendor invoice |
| Vendor redraw promissory note | Re-issue vendor PN |
| Vendor settle promissory note | Settle PN |

Operational journals — *Inventory journals* (Movement, Profit/Loss, Transfer, Bill of Materials, Item arrival), *Production journals* (Picking list, Route card, Job card, Report as finished), *Counting journals*, *Stocktaking journals* — are not in the table above because they are configured outside the financial *Journal names* page, but they are equally posting transactions that produce ledger entries.

### 5.3 Posting Profiles by module

| Module | Posting profile | Posts to (typical accounts) |
|---|---|---|
| AP | Vendor posting profile | Liability (Summary), Settlement, Discount, Liability for Discount, Arrival, Offset Account |
| AR | Customer posting profile | Summary AR, Settlement, Discount, Liability for Discount, Bill of Exchange, Promissory Note |
| INV | Item posting profile (per item group) | Inventory Receipt, Inventory Issue, Sales Order Issue, Purchase Order Receipt, Variance, Cost of Goods Sold, Profit/Loss, Inventory Adjustment, Production line, WIP, Picked, Reported as Finished |
| FA | Fixed asset posting profile | Acquisition, Depreciation, Acquisition Adjustment, Depreciation Adjustment, Bonus, Disposal Sale, Disposal Scrap, Net Book Value on Disposal, Sale Value on Disposal, Write-up, Write-down, Provisions for Reserves, Reversal of Provisions, Revaluation, Custom 1, Custom 2 |
| PROJ | Project posting profile | Cost — Item / Hour / Expense; WIP — Cost / Sales / Profit; Accrued Loss; Accrued Revenue; Invoiced Revenue; On-Account Invoicing |
| Bank | Bank posting profile | Bank Account, Discount, Charges, Errors, Realized/Unrealized Gain/Loss |
| Sales tax | Sales tax posting | Sales Tax Receivable, Sales Tax Payable, Use Tax, Settle, Round-off |

### 5.4 Fixed Asset transaction types

The Fixed Asset module supports the following transaction types, each governed by the Fixed Asset Posting Profile:

| Transaction type | GL pattern |
|---|---|
| Acquisition | Dr Fixed Asset Capitalization; Cr Fixed Asset Acquisition Clearing |
| Acquisition adjustment | Adjusts capitalization basis |
| Depreciation | Dr Depreciation Expense; Cr Accumulated Depreciation |
| Depreciation adjustment | Adjustment of prior depreciation |
| Special / Bonus depreciation | Dr Bonus Depreciation Expense; Cr Bonus Reserve |
| Extraordinary depreciation | Dr Extraordinary Depreciation Expense; Cr Accumulated Depreciation |
| Write-up | Dr Asset Cost; Cr Write-up Income / Reserve |
| Write-down | Dr Write-down Expense; Cr Asset Cost |
| Revaluation | Adjusts asset basis through Revaluation Reserve |
| Provisions for reserves | Tax-driven reserve postings |
| Reversal of provisions | Reverses prior reserve provision |
| Disposal — Sale | Reverses Acquisition and Accumulated Depreciation; recognizes Sale Proceeds; Gain/Loss to Sale of Fixed Assets |
| Disposal — Scrap | Reverses Acquisition and Accumulated Depreciation; Loss to Loss on Scrap |
| Custom 1, Custom 2 | Localization-driven additional posting types |

Disposal posts in detail (with separate Acquisition vs. Acquisition Prior Years, Depreciation vs. Depreciation Prior Years lines) when *Post disposal transactions in detail* is enabled in Fixed Assets parameters.

### 5.5 Project types

D365 F&O supports six project types, each with distinct revenue recognition and capitalization rules:

| Project type | Billing? | Revenue recognition | Cost treatment |
|---|---|---|---|
| Time and material | Yes (per consumption) | As incurred (per hour, expense, or item) | Direct cost to project |
| Fixed-price | Yes (per milestone) | Percentage-of-completion *or* completed-contract | Estimates required; WIP accumulates until recognition |
| Investment | No | None during project | Costs accumulate as Investment / WIP-Investment, then capitalized to Fixed Assets at completion |
| Cost | No | None | Direct cost to project; can reallocate to operational accounts |
| Internal | No | None | Used for internal time/expense tracking; can post to operational accounts |
| Time | No | None | Non-financial time tracking only — no project costs |

(The previous reference collapsed *Cost* and *Internal* into one bucket; they are configured separately and have different behaviors.)

### 5.6 Inventory and Manufacturing transaction types

| Transaction | GL pattern |
|---|---|
| Purchase order receipt (product receipt) | Dr Inventory Receipt (or Purchase Accrual); Cr Purchase Accrual |
| Vendor invoice (PO-matched) | Dr Purchase Accrual; Cr AP |
| Inventory movement journal | Dr/Cr Inventory; offset to specified ledger account |
| Inventory transfer journal | Cross-warehouse transfer; can revalue if standard cost differs |
| Inventory adjustment | Dr/Cr Inventory; offset to Profit/Loss account |
| BOM journal | Issue components; receive assembled item |
| Counting / stocktaking journal | Dr/Cr Inventory; offset to Counting Loss/Profit |
| Production order — Picking list | Dr WIP; Cr Inventory |
| Production order — Report as finished | Dr Inventory; Cr WIP |
| Production order — Costing | Dr Variance accounts; calculates standard cost variance |
| Sales order — packing slip | Dr Issue (Cost of Units Delivered); Cr Inventory |
| Sales order — invoice | Dr COGS; Cr Issue; Dr AR; Cr Revenue |
| Inter-company sales/purchase | Cross-company posting through automated chained orders |

### 5.7 Revenue Recognition (ASC 606 / IFRS 15) and Lease Accounting

- **Revenue Recognition module** (D365 F&O) — events: *Allocate*, *Schedule*, *Recognize*, *Reallocate*. Allocates the transaction price to performance obligations and posts revenue per the schedule, posting to *Deferred Revenue* and *Revenue* accounts.
- **Asset Leasing module** (D365 F&O native, ASC 842 / IFRS 16) — events: *Initial Recognition* (Dr ROU Asset; Cr Lease Liability), *Lease Payment* (Dr Lease Liability, Interest Expense; Cr Cash), *Amortization* (Dr Lease Expense / Amortization; Cr Accumulated Amortization), *Modification / Remeasurement*, *Termination*, *Impairment*. Asset Leasing supports dual reporting via posting layers: e.g., lease accounted under IFRS 16 in the Current layer and under ASC 842 (or a tax basis) in a Custom layer.


---

## 6. Oracle NetSuite

### 6.1 Architectural notes

NetSuite, originally founded in 1998 as NetLedger and a true cloud-native financial system from the start, takes a fourth approach: **transactional records carry their own GL Impact**, calculated at save time and immediately viewable via a *GL Impact* link on every posting record. The GL impact is *configurable*, not hard-coded — a fact frequently misstated in third-party comparisons. Configuration occurs through:

- **Item-level account assignments** — every item record specifies the asset, COGS, income, and (where applicable) expense accounts to use, optionally per subsidiary in OneWorld
- **Entity-level account assignments** — customers, vendors, and employees can override default AR/AP/expense accounts at the entity level
- **Accounting preferences** — global behavior toggles (e.g., posting period of revenue commitments, accounting method for COGS, default discount account)
- **Tax codes** — drive sales tax / VAT / GST GL postings
- **SuiteGL framework** — three customization layers, all introduced from 2014 onward:
  - **Custom GL Lines plug-in** — SuiteScript implementations that modify or extend the GL impact of standard or custom transactions; widely used for non-US statutory requirements (Brazilian tax, Italian, French) and for splitting cost into sub-accounts based on complex logic
  - **Custom Transaction Types** — define new transaction records with their own posting/non-posting statuses, forms, permissions, and number sequences
  - **Custom GL Segments** — additional reporting dimensions beyond the built-in subsidiary, class, department, location

### 6.2 Standard NetSuite Transaction Types

The lists below organize NetSuite transaction types by functional area, with explicit *Posting* / *Non-Posting* designation. The previous reference incorrectly classified some non-posting types (e.g., Wave, Inventory Distribution, Inventory Status Change) as posting transactions.

#### Journal & Ledger

All posting unless marked.

| Type | GL pattern |
|---|---|
| Journal | Manual user-entered Dr/Cr lines |
| Intercompany Journal | Manual cross-subsidiary journal (legacy) |
| Advanced Intercompany Journal | Newer cross-subsidiary journal with automatic elimination |
| Intercompany Elimination Journal | Eliminates intercompany balances at consolidation |
| Allocation Journal | System-generated based on Allocation Schedule |
| Amortization Journal | System-generated for prepaid/deferred expense schedules |
| Revenue Recognition Journal | System-generated for revenue schedules / arrangements |
| Advanced Revenue Recognition Journal | Generated by Advanced Revenue Management module |
| Revenue Reclassification Journal | Period-end revenue reclassification |
| Currency Revaluation | FX revaluation of foreign-currency balances |
| Revaluation Journal | Period revaluation outputs |
| Reversing Journal | Auto-reverses next period |
| Period End Journal | Period close adjustments |
| Cross Charge Journal | Inter-subsidiary cross-charges in OneWorld |
| Balancing Journal | Balances eliminations for OneWorld |
| Time Posting Journal | Posts time-tracking entries to projects |
| Historical Transaction Processing Journal | Migration / historical period adjustments |
| GL Impact Adjustment | Direct GL adjustment to a transaction |
| Statistical Journal | **Non-posting** to financial GL; statistical-only |
| Liability Adjustment | **Non-posting** (always) — payroll/tax liability adjustment |
| System Journal | System-generated misc journals |
| Entity Open Balance Journal | Migration starter balances |
| Offset Journal | Adjustments to offset accounts |
| Custom (via SuiteGL) | User-defined via Custom Transaction Types |

#### Procure-to-Pay (Purchases & Payables)

| Type | Posting? | GL pattern |
|---|---|---|
| Requisition | Non-posting | Approval workflow |
| Vendor Request for Quote | Non-posting | RFQ tracking |
| Request for Quote | Non-posting | Same as above (alt name) |
| Purchase Contract | Non-posting | Blanket terms |
| Blanket Purchase Order | Non-posting | Master release schedule |
| Purchase Order | **Always non-posting** | Commitment only |
| Outsourced Purchase Order | Non-posting | Contract manufacturing |
| Inbound Shipment | Non-posting | In-transit tracking |
| Item Receipt | Posting | Dr Inventory; Cr Inventory Received Not Billed (Accrued Purchases) |
| Bill (Vendor Bill) | Posting | Dr Inventory Received Not Billed (or Expense); Cr Accounts Payable |
| Bill Credit (Vendor Credit) | Posting | Dr AP; Cr Inventory / Expense |
| Bill Payment | Posting | Dr AP; Cr Cash |
| Vendor Prepayment | Posting | Dr Prepayment Asset; Cr Cash |
| Vendor Prepayment Application | Posting | Dr AP; Cr Prepayment Asset |
| Check | Posting | Dr Expense / AP; Cr Cash |
| Expense Report | Posting | Dr Expense; Cr Employee AP / Reimbursement |
| Vendor Return Authorization | **Always non-posting** | Returns approval |
| Bill Variances Journal | Posting | PPV / Exchange variances on vendor bills vs receipts |

#### Order-to-Cash (Sales & Receivables)

| Type | Posting? | GL pattern |
|---|---|---|
| Opportunity | **Always non-posting** | Sales pipeline |
| Estimate (Quote) | **Always non-posting** | Customer quote |
| Sales Order | **Always non-posting** | Commitment |
| Fulfillment Request | Non-posting | Pre-fulfillment routing |
| Item Fulfillment | Posting | Dr COGS; Cr Inventory |
| Store Pickup Fulfillment | Posting | Same as Item Fulfillment, BOPIS variant |
| Invoice | Posting | Dr Accounts Receivable; Cr Revenue, Cr Tax Payable |
| Invoice Group | Posting | Aggregated invoice |
| Cash Sale | Posting | Dr Cash / Undeposited Funds; Cr Revenue, Cr Tax (no AR step) |
| Cash Refund | Posting | Dr Revenue (reversal); Cr Cash |
| Statement Charge | Posting | Dr AR; Cr Revenue |
| Finance Charge | Posting | Dr AR; Cr Finance Charge Income |
| Customer Deposit | Posting | Dr Cash; Cr Customer Deposit Liability |
| Customer Payment | Posting | Dr Cash / Undeposited Funds; Cr AR |
| Customer Payment Authorization | Posting | Card authorization (when captured separately) |
| Credit Memo | Posting | Dr Revenue (reversal); Cr AR |
| Customer Refund | Posting | Dr Customer Refund / AR; Cr Cash |
| Credit Card | Posting | Dr Expense / Inventory; Cr Credit Card Payable |
| Credit Card Refund | Posting | Reverses Credit Card |
| Deposit | Posting | Dr Cash; Cr Undeposited Funds |
| Deposit Application | Posting | Applies prepayment to invoice |
| Return Authorization | **Always non-posting** | Customer returns approval |
| Revenue Arrangement | **Always non-posting** | ASC 606 performance obligation grouping |
| Revenue Commitment | Posting | Dr Unbilled AR; Cr Revenue (advance recognition before invoicing) |
| Revenue Commitment Reversal | Posting | Reverses Revenue Commitment |
| Revenue Contract | Non-posting | Contract structure for ARM |

#### Inventory and Manufacturing

| Type | Posting? | GL pattern |
|---|---|---|
| Inventory Adjustment | Posting | Dr/Cr Inventory; offset to specified Adjustment account |
| Inventory Worksheet | Posting | Bulk inventory adjustment |
| Inventory Cost Revaluation | Posting | Standard cost / landed cost revaluations |
| Inventory Transfer | Posting | Cross-location inventory move; revaluation if costs differ |
| Inventory Distribution | Non-posting | WMS pre-allocation |
| Inventory Status Change | Non-posting | Move stock between statuses (Available / Restricted) |
| Inventory Count | Non-posting | Cycle count entry; differences post via Inventory Adjustment |
| Bin Putaway Worksheet | Non-posting | Bin assignment |
| Bin Transfer | Non-posting | Bin-to-bin in same location |
| Wave | **Non-posting** | WMS pick wave |
| Transfer | Posting | Inter-subsidiary transfer (OneWorld) |
| Transfer Order | **Always non-posting** | Inter-location transfer commitment |
| Item Fulfillment (against Transfer Order) | Posting | Dr Receiving Inventory; Cr Sending Inventory |
| Assembly Build | Posting | Dr Finished Goods Inventory; Cr Component Inventory |
| Assembly Unbuild | Posting | Reverses Assembly Build |
| Work Order | **Always non-posting** | Production order |
| Work Order Issue | Posting | Dr WIP; Cr Inventory (component) |
| Work Order Completion | Posting | Dr Finished Goods; Cr WIP |
| Work Order Close | Posting (variance) | Variance to standard cost |
| Outsourced Manufacturing | Posting | Dr WIP / Inventory; Cr AP / Outsourced Manufacturing Liability |
| Engineering Change Order | Non-posting | BOM/routing change tracking |
| Supply Change Order | Non-posting | Demand-planning supply change |
| Ownership Transfer | Posting | Consigned inventory ownership change |

#### Payroll & Human Resources

(Available with NetSuite SuitePeople Payroll module)

| Type | Posting? | GL pattern |
|---|---|---|
| Paycheck | Posting | Dr Wage Expense, Employer Tax Expense, Benefit Expense; Cr Cash, Tax Payable, Benefit Payable |
| Paycheck Journal | Posting | Bulk paycheck journal entry (when payroll is processed externally) |
| Payroll Batch | Container | Groups paychecks |
| Payroll Journal | Posting | System summary journal |
| Payroll Adjustment | **Always non-posting** | Adjustments before run completion |
| Payroll Liability Check | Posting | Pays accrued payroll liabilities |
| Tax Liability Check | Posting | Pays accrued tax liabilities |
| Commission (Employee) | Posting (after authorization) | Dr Commission Expense; Cr Commission Payable |
| Commission (Partner) | Posting (after authorization) | As above for channel partners |

#### Fixed Assets (NetSuite Fixed Assets Management module)

| Type | Posting? | GL pattern |
|---|---|---|
| Asset Proposal | Non-posting | Creates draft asset record |
| Asset Acquisition | Posting | Dr Asset Cost; Cr Asset Clearing (links to original AP transaction) |
| Asset Depreciation | Posting | Dr Depreciation Expense; Cr Accumulated Depreciation |
| Asset Revaluation | Posting | Adjusts cost or accumulated depreciation |
| Asset Transfer | Posting | Cross-subsidiary or cross-class transfer |
| Asset Disposal — Sale | Posting | Dr Accumulated Depr, Cash, Loss; Cr Asset Cost, Gain |
| Asset Disposal — Scrap / Write-off | Posting | Dr Accumulated Depr, Loss; Cr Asset Cost |
| Asset Split | Non-posting (structurally) | Splits asset into multiple records |

#### SuiteBilling (subscription / recurring revenue)

| Type | Posting? | GL pattern |
|---|---|---|
| Subscription | Non-posting | Master subscription record |
| Subscription Change Order | Non-posting | Modifications |
| Charge | Non-posting (until invoiced) | Generated periodic charges |
| Charge Run | Non-posting | Batch charge generation |

### 6.3 Always non-posting transaction types

Per NetSuite documentation, the following transaction types **never** generate GL impact, regardless of approval state:

> Estimate, Liability Adjustment, Opportunity, Payroll Adjustment, Purchase Order, Return Authorization, Revenue Arrangement, Sales Order, Transfer Order, Vendor Return Authorization, Work Order

This list matters: in audit and reconciliation discussions, distinguishing always-non-posting types from sometimes-non-posting (pending approval) types from posting types is essential. Many other transaction types are non-posting *until approved* — Commission entries, for example, always require authorization before posting; Journal Entries may require approval per accounting preference settings.


---

## 7. Toward a Unified Model

Having enumerated transaction types vendor by vendor, the more useful question is: *what does any modern ERP need to handle, expressed in vendor-neutral terms?* The taxonomy below organizes business events by economic substance and identifies the consistent GL impact pattern. For each event, a cross-walk shows what each vendor calls the same thing.

The framing principle: **a business event is a real-world occurrence** (a vendor delivers goods; a customer pays; an asset wears out one more month). **A transaction is the system's record of that event**. **A GL entry is the accounting representation**, governed by accounting standards. The vendor differences are almost entirely in steps two and three — the recording and the rule layer — not in step one. Naming conventions diverge widely; underlying economics converge.

### 7.1 The reference taxonomy of business events

#### A. Procure-to-Pay cycle

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| A1 | Purchase commitment created (no GL impact yet) | None (commitment register only) | Purchase Order; commitments via extension ledger since 1809 | Purchase Order | Purchase order | Purchase Order (non-posting) |
| A2 | Goods physically received | Dr Inventory / Receiving Inspection; Cr GR/IR or Receipt Accrual | MM Movement Type 101 (doc type WE) | Receipt Accounting: *Receipt into Receiving Inspection* / *Delivery to Inventory* | Product receipt (PO) | Item Receipt |
| A3 | Vendor invoice received | Dr GR/IR / Inventory; Cr Accounts Payable (with tax) | LIV via doc type RE/RN | Payables: *Invoice Validated* | Vendor invoice (matched or non-matched) | Bill |
| A4 | Vendor invoice price differs from PO | Dr/Cr Price Variance (PPV) | Price difference in MIRO; flows via OBYC | *Invoice Price Adjustment* event | Vendor invoice variance | Bill (variance to PPV via Bill Variances Journal) |
| A5 | Payment issued to vendor | Dr AP; Cr Cash (or Cash Clearing) | Doc type KZ via F110/F-53 | Payables: *Payment Created* | Vendor disbursement journal | Bill Payment / Check |
| A6 | Payment cleared at bank | Dr Cash Clearing; Cr Cash (when clearing used) | Bank reconciliation, doc type ZP | Payables: *Payment Cleared* | Bank reconciliation match | Reconcile Bank Statement |
| A7 | Vendor credit / debit memo | Reverses original AP / inventory entry | Doc type KG (credit) / KR with negative | *Credit Memo Validated* / *Debit Memo Validated* | Vendor invoice (credit note) | Bill Credit |
| A8 | Prepayment to vendor | Dr Prepayment Asset; Cr Cash | Down Payment via FBA1 / F-47 | *Prepayment Validated* | Vendor prepayment journal | Vendor Prepayment |
| A9 | Prepayment applied to invoice | Dr AP; Cr Prepayment Asset | Down Payment Clearing via F-54 | *Prepayment Applied* | Settle prepayment | Vendor Prepayment Application |
| A10 | Goods returned to vendor | Reverses receipt; Dr GR/IR (or AP); Cr Inventory | MM 122 / 161 | *Return to Supplier* event | PO return | Vendor Return Authorization → Item Receipt (negative) |

#### B. Order-to-Cash cycle

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| B1 | Sales commitment created | None | Sales Order (doc) | Order Management Sales Order | Sales order | Sales Order (non-posting) |
| B2 | Goods shipped to customer | Dr COGS; Cr Inventory | MM 601 (doc type WL) | Cost Accounting: *Sales Issue* | Sales order packing slip | Item Fulfillment |
| B3 | Customer invoice issued | Dr AR; Cr Revenue, Cr Tax Payable | SD billing → doc type RV | Receivables: *Invoice Created* | Sales order invoice | Invoice |
| B4 | Customer pays in cash at point of sale | Dr Cash; Cr Revenue, Cr Tax (no AR step) | Counter sale (industry-specific) | OM Cash sale flow | Retail point-of-sale | Cash Sale |
| B5 | Customer payment received | Dr Cash / Undeposited; Cr AR | Doc type DZ via F-28 | Receivables: *Receipt Created* | Customer payment journal | Customer Payment |
| B6 | Customer payment unapplied / on-account | Dr Cash; Cr Customer Deposit Liability | On-account assignment | *Receipt Created* with no application | Customer payment on-account | Customer Deposit |
| B7 | Customer credit memo / refund | Dr Revenue contra; Cr AR (then Dr AR; Cr Cash for refund) | Doc type DG | *Credit Memo Created* | Sales credit note | Credit Memo / Customer Refund |
| B8 | Customer goods returned | Dr Inventory; Cr COGS (then credit memo) | MM 651/653 | RMA receipt + Credit Memo | RMA flow | Return Authorization → Item Receipt → Credit Memo |
| B9 | Customer write-off / bad debt | Dr Bad Debt Expense; Cr AR | Doc type AB / DA | Receivables: *Adjustment Created* | Customer write-off | Write Off (Customer Payment with write-off) |
| B10 | Late charge / finance charge | Dr AR; Cr Finance Charge Income | Interest calculation (FINT) | Receivables: late charge processing | Customer interest journal | Finance Charge |

#### C. Inventory and Cost of Goods

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| C1 | Inventory consumption to cost center | Dr Department Expense; Cr Inventory | MM 201 | Cost Accounting: Inventory Issue | Inventory journal — issue | Inventory Adjustment (Out) |
| C2 | Inventory consumption to project / WBS | Dr Project Cost; Cr Inventory | MM 221 | Project Costing: Inventory Cost Distribution | Project journal — item | Inventory commitment to project |
| C3 | Inter-warehouse transfer (one location to another) | Dr Receiving Inventory; Cr Sending Inventory | MM 301 / 311 / 313 / 315 | Inventory transfer transactions | Inventory transfer journal | Transfer Order → Item Fulfillment / Item Receipt |
| C4 | Cross-company stock transfer | Cross-company AR/AP through internal billing | MM 645/647 (cross-company STO) | Inter-org transfer w/ markup; intercompany AR/AP | Inter-company chained orders | Cross-subsidiary Transfer / Intercompany Sales |
| C5 | Inventory revaluation (price change) | Dr/Cr Inventory; offset to Inventory Revaluation | MM doc type PR; Material Ledger | Cost Accounting: *Standard Cost Update* | Inventory cost revaluation | Inventory Cost Revaluation |
| C6 | Physical inventory shrinkage | Dr Shrinkage Expense; Cr Inventory | MM 702/704/708 | Cost Accounting: Inventory Adjustment | Counting / stocktaking journal | Inventory Adjustment |
| C7 | Physical inventory overage | Dr Inventory; Cr Inventory Adjustment Income | MM 701/703/707 | Cost Accounting: Inventory Adjustment (positive) | Counting journal (positive) | Inventory Adjustment (positive) |
| C8 | Scrap | Dr Scrap Expense; Cr Inventory | MM 551/553/555 | Cost Accounting: Inventory Adjustment to scrap account | Inventory write-off journal | Inventory Adjustment |
| C9 | Production: components issued | Dr WIP; Cr Inventory | MM 261 | Cost Accounting: Work in Process Issue | Production picking list | Work Order Issue |
| C10 | Production: finished goods received | Dr Finished Goods; Cr WIP | MM 101 against production order | Cost Accounting: Work in Process Completion | Report as finished | Work Order Completion / Assembly Build |
| C11 | Production variance | Dr Variance accounts; Cr WIP | KKAO settlement; Material Ledger | Period-end Material Cost Variance | Production order costing | Work Order Close (variance) |
| C12 | Overhead absorption | Dr WIP / Inventory; Cr Overhead Absorbed | CO-PC overhead calculation | Cost Accounting: *Overhead Absorption* | Indirect cost calculation | Custom GL Lines plug-in (non-native) |
| C13 | Landed cost allocation | Dr Inventory (landed); Cr Landed Cost Clearing | Cond. types for delivery costs in MM | Receipt Accounting landed cost | Landed cost module | Landed Cost categories |

#### D. Fixed Assets

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| D1 | Asset acquired (capitalized) | Dr Asset Cost; Cr Asset Clearing (offsets AP) | FI-AA transaction type 100; doc type AA | Assets event class *Additions* | FA acquisition journal | Asset Acquisition |
| D2 | CIP (asset under construction) accumulation | Dr CIP Asset; Cr Liability / Project | AuC asset class | *CIP Additions* event class | Project type Investment | Manual journal or custom script |
| D3 | CIP placed in service | Dr Final Asset; Cr CIP | AuC settlement (KO88) | *Capitalization* event class | Project end-of-investment capitalization | Asset transfer (CIP to placed-in-service) |
| D4 | Periodic depreciation | Dr Depreciation Expense; Cr Accumulated Depreciation | Depreciation run; doc type AF | Assets: *Depreciation* event | FA depreciation proposal | Asset Depreciation |
| D5 | Bonus / accelerated depreciation (tax book) | Dr Bonus Expense; Cr Bonus Reserve | FI-AA depreciation areas / parallel ledgers | Bonus rule on tax book | FA Special depreciation in Tax posting layer | Multi-Book Accounting depreciation in Tax book |
| D6 | Asset transfer (between locations / cost centers / cos) | Dr Receiving Asset; Cr Sending Asset (cost and reserve) | FI-AA tx type 300/320 | *Transfers* event | FA Transfer | Asset Transfer |
| D7 | Asset adjustment / cost change | Adjusts cost basis; recalculates remaining depreciation | FI-AA tx 105 | *Adjustments* event | FA acquisition adjustment | Asset Revaluation |
| D8 | Asset disposal — sale | Dr Accum Depr, Cash; Cr Asset Cost; Gain/Loss to P&L | FI-AA tx 200 | *Retirements* event (Retirement) | FA disposal sale | Asset Disposal — Sale |
| D9 | Asset disposal — scrap | Dr Accum Depr, Loss on Disposal; Cr Asset Cost | FI-AA tx 210 | *Retirements* event | FA disposal scrap | Asset Disposal — Scrap |
| D10 | Asset reinstated (disposal reversed) | Reverses disposal entries | FI-AA reverse retirement | *Reinstatement* event type | FA reverse disposal | Manual journal / asset re-entry |
| D11 | Asset revaluation (statutory) | Dr Asset Cost, Cr Revaluation Reserve | FI-AA revaluation | *Revaluation* event class | FA Revaluation | Asset Revaluation |
| D12 | Asset impairment / write-down | Dr Impairment Expense; Cr Accumulated Depr or Asset Cost | Unplanned depreciation | *Unplanned Depreciation* event | FA write-down | Asset Revaluation (negative) |

#### E. Cash and Banking

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| E1 | Bank account funded / drawn | Dr/Cr Cash | Doc type SK / SA | Cash Management external transaction | General journal | Journal / Check / Deposit |
| E2 | Bank-to-bank transfer | Dr Receiving Cash; Cr Sending Cash (Cash-in-Transit if reconciled separately) | Cross-bank doc | *Bank Account Transfer* event class | Bank transfer journal | Transfer (cash) |
| E3 | Bank charges | Dr Bank Fees Expense; Cr Cash | Bank reconciliation posting | *Bank Statement Cash Flow* (charge) | Bank reconciliation entry | Journal |
| E4 | Bank interest income / expense | Dr/Cr Cash; Cr/Dr Interest Income/Expense | Bank reconciliation posting | *Bank Statement Cash Flow* | Bank reconciliation entry | Journal |
| E5 | FX revaluation of cash / AR / AP balances | Dr/Cr Realized/Unrealized FX Gain or Loss | F.05 / FAGL_FCV | GL Revaluation; SLA on payment apply | Foreign currency revaluation | Currency Revaluation |
| E6 | FX realized gain/loss on payment | Recognized at payment | Embedded in payment posting | Embedded in *Payment Created* | Embedded in payment | Embedded in Bill Payment / Customer Payment |

#### F. Period Close and Adjustments

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| F1 | Manual journal entry | Dr/Cr per user | Doc type SA | GL Manual Journal | General journal | Journal |
| F2 | Recurring / template journal | Dr/Cr per template | Doc type SA via FBD1; sample doc X1 | Recurring Journal | Periodic / recurring journal | Memorized Transaction |
| F3 | Accrual / provision | Dr Expense; Cr Accrued Liability | Accrual Engine; doc type SA or specific | GL Accrual; Subledger Accrual | Accrual scheme | Journal (often via Amortization template) |
| F4 | Reversal of accrual (next period) | Reverses F3 | Auto-reverse flag on doc | Reversing journal | Reversing journal | Reversing Journal |
| F5 | Allocation (cost / profit center) | Dr/Cr per allocation rule | RKIU/RKIV; KSU5/KSV5 | Allocation Manager / Calculation Manager | Allocation journal | Allocation Schedule / Journal |
| F6 | Inter-company elimination | Eliminates IC AR/AP, sales/COGS on consolidation | Group consolidation (BCS / Group Reporting) | Financial Consolidation Cloud / Consolidation Hub | Elimination journal | Intercompany Elimination Journal |
| F7 | Currency translation (CTA) | Translates non-functional ledger to reporting currency; CTA to OCI | ACDOCA reporting currency | Reporting currency / secondary ledger | Reporting currency adjustment journal | Consolidated reporting currency |
| F8 | Period close (lock) | Closes period for posting | OB52 / posting period variant | Manage Accounting Periods | Close period in GL | Close Accounting Period |
| F9 | Year-end carry-forward | Closes income/expense to retained earnings | F.16 GL balance carryforward | Open First Period of Year | Year-end close | Auto-generated by Period End close |

#### G. Revenue Recognition (ASC 606 / IFRS 15)

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| G1 | Performance obligation identified | None (or memo) | RAR Performance Obligation | Revenue Mgmt Cloud Performance Obligation | Revenue Recognition: Allocate | Revenue Arrangement (non-posting) |
| G2 | Transaction price allocated to POs | None | RAR Allocation | Revenue Mgmt allocation | Revenue Recognition: Allocate event | Revenue Element allocation |
| G3 | Revenue recognized as POs satisfied | Dr Unbilled AR / Contract Asset; Cr Revenue | Event-based revenue recognition | Revenue Mgmt: revenue recognition event | Revenue Recognition: Recognize event | Revenue Recognition Journal |
| G4 | Invoiced ahead of recognition | Dr AR; Cr Deferred Revenue | RAR Contract Liability handling | Deferred Revenue account | Deferred Revenue | Deferred Revenue at invoice |
| G5 | Recognized ahead of invoicing | Dr Unbilled AR; Cr Revenue | RAR Contract Asset | Unbilled Receivable | Accrued Revenue | Revenue Commitment |
| G6 | Revenue reclassification | Adjusts deferred ↔ recognized | Period-end RAR run | Revenue reclassification | Revenue reclassification | Revenue Reclassification Journal |

#### H. Lease Accounting (ASC 842 / IFRS 16)

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| H1 | Lease initial recognition | Dr ROU Asset; Cr Lease Liability | FI-LA Lease Booking | Lease Accounting: *Lease Booking* | Asset Leasing: Initial Recognition | Lease module / SuiteApp posting |
| H2 | Periodic lease expense | Dr Interest Expense + Amortization (IFRS 16) or Lease Expense (ASC 842 op); Cr Liability + Accum Amortization | FI-LA Lease Expense Accrual | *Lease Expense* event class | Asset Leasing: Amortization run | Periodic Lease Expense |
| H3 | Lease cash payment | Dr Lease Liability; Cr Cash (via Payables) | FI-LA Lease Payment | *Lease Payment* event class | Asset Leasing: Lease Payment | Bill Payment (linked to lease) |
| H4 | Lease modification | Adjusts ROU and Liability; difference to gain/loss | FI-LA Modification | *Lease Modification* event class | Asset Leasing: Remeasurement | Lease modification entry |
| H5 | Lease termination | Dr Liability, Accum Amortization; Cr ROU Asset; gain/loss | FI-LA Termination | *Lease Termination* event class | Asset Leasing: Termination | Lease termination entry |

#### I. Payroll and Human Capital

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion HCM | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| I1 | Payroll run costed | Dr Wage Expense, Employer Tax, Benefit Expense; Cr Net Pay Liability, Tax Payable, Benefit Payable | HCM payroll posting; doc type P3 (or Z*) | Payroll: *Run Costs* event | Payroll Disbursement journal (when integrated) | Paycheck / Paycheck Journal |
| I2 | Payroll payment | Dr Net Pay Liability; Cr Cash | Doc type ZP / ZS | Payroll: *Payment Costs* event | Payroll disbursement | Paycheck (combined with I1 if same flow) |
| I3 | Tax remittance | Dr Tax Payable; Cr Cash | Standard payment | Standard payment | Vendor disbursement | Tax Liability Check |
| I4 | Period accrual (salaries earned, not paid) | Dr Wage Expense; Cr Wages Payable | Accrual Engine | Payroll: *Partial Period Accruals* | Accrual scheme | Journal |
| I5 | Vacation / PTO accrual | Dr Vacation Expense; Cr Vacation Liability | Time Mgmt + payroll | HCM Absence costing | HR / Time and attendance | SuitePeople accrual entries |
| I6 | Commission / variable comp | Dr Commission Expense; Cr Commission Payable | Incentive Compensation Mgmt | Incentive Compensation Cloud | Commissions module | Commission |

#### J. Tax

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| J1 | Output tax on sales | Cr Output VAT / Sales Tax Payable | Embedded in SD billing | Embedded in Receivables Invoice | Sales tax posting on invoice | Embedded in Invoice / Cash Sale |
| J2 | Input tax on purchases | Dr Input VAT / Recoverable Tax | Embedded in MM/AP | Embedded in Payables Invoice | Sales tax posting on PO/invoice | Embedded in Bill |
| J3 | Tax remittance to authority | Dr Tax Payable; Cr Cash | Standard AP payment | AP Payment to tax authority | Vendor disbursement | Tax Liability Check |
| J4 | Use tax / reverse charge | Cr both Output and Input VAT (offset) | Tax procedures (FTXP) | Oracle Tax: self-assessed | Use tax posting | Tax code with reverse-charge config |
| J5 | Withholding tax | Dr Expense (gross); Cr AP, Cr Withholding Payable | Withholding tax codes | Oracle Tax withholding | Withholding tax codes | 1099 / Withholding |

#### K. Intercompany

| # | Business event | GL pattern | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|---|
| K1 | Intercompany sale | Sending: Dr IC Receivable; Cr Revenue. Receiving: Dr Inventory/Expense; Cr IC Payable | Cross-company doc; intercompany billing | Intercompany Receivables / Payables; Intercompany subledger | Inter-company sales/purchase chained orders | Intercompany Sales (OneWorld) |
| K2 | Intercompany cost reallocation | Sending: Dr IC Receivable; Cr Cost Recovery. Receiving: Dr Cost; Cr IC Payable | Cost Center to Cost Center cross-company | Project cross-charge / Borrowed and Lent | Intercompany journal | Cross Charge Journal |
| K3 | Intercompany cash transfer | Dr IC Receivable; Cr Cash (sender). Mirror at receiver | Bank transfer with cross-company | Cash Mgmt Bank Account Transfer (intercompany) | Bank transfer journal | Intercompany Journal |
| K4 | Intercompany elimination at consolidation | Eliminates IC AR/AP, sales/COGS | Group Reporting (BCS replacement) | Financial Consolidation Cloud | Elimination journal | Intercompany Elimination Journal |
| K5 | Markup on intercompany inventory | Captures unrealized profit, eliminates at consolidation | Material Ledger group valuation | Inter-org transfer with markup | Inter-company stock with markup | Manual journal / SuiteApp |

#### L. Statistical and Memo postings (no financial impact)

Statistical postings — used for non-financial measures like headcount, units produced, or square footage — that drive allocations or KPIs without affecting financial statements:

| # | Use | SAP S/4HANA | Oracle Fusion | D365 F&O | NetSuite |
|---|---|---|---|---|---|
| L1 | Statistical key figures (allocation drivers) | Statistical Key Figures (SKFs) | Statistical accounts in GL | Statistic transactions journal type | Statistical Account / Statistical Journal |
| L2 | Memo / non-posting tracking | Recurring entry templates | Memo journals | Posting Layer "None" entries | Always-non-posting transactions (Estimate, SO, PO, etc.) |

### 7.2 The capability set a modern ERP must support

Reading across the cross-walks above, a small set of capabilities is required of any system claiming to be a modern enterprise ERP. These are the things that, if missing or weak, force the customer into spreadsheets, third-party tools, or manual workarounds.

**1. Real-time GL with subledger drill-down.** The legacy world of nightly batch posting and FI-vs-CO reconciliation is over. Every modern ERP exposes the GL impact of an operational transaction immediately and lets a reader drill from a GL line back to the source document. SAP achieves this through ACDOCA; Oracle Fusion through SLA's tight coupling and the underlying HCM/Financials shared model; D365 through synchronous posting; NetSuite through the GL Impact page on the record itself.

**2. Configurable accounting derivation.** Hard-coded GL accounts are unsupportable. Each system needs *some* mechanism — SAP's account determination tables and substitutions, Oracle Fusion's SLA rules, D365's posting profiles, NetSuite's item/entity assignments and SuiteGL — that lets accountants change where things post without code changes. The architectural difference is how *abstract* the rules are: Oracle Fusion is most abstract (and most powerful, and most opaque); NetSuite is least abstract (visible on the record itself, but limited in conditional logic without scripting).

**3. Parallel accounting under multiple standards.** Public companies operating across jurisdictions must report under IFRS and one or more local GAAPs and one or more tax bases — simultaneously, on the same operational events. SAP solves this with parallel ledgers + extension ledgers + (newer) Universal Parallel Accounting. Oracle Fusion solves it with primary/secondary/reporting ledgers driven by SLA. D365 solves it with the ten posting layers. NetSuite solves it with Multi-Book Accounting (a paid module). The depth of support varies — SAP's UPA reaches further into operational processes (e.g., ledger-specific material prices) than the others.

**4. Multi-currency with FX revaluation and translation.** Functional currency and reporting currency are distinct concepts; foreign-currency open items must be revalued at period end with realized vs. unrealized gain/loss tracked separately; CTA (cumulative translation adjustment) must flow to OCI. All four systems support this; the configurability of revaluation rules and the granularity of historical-rate vs. current-rate translation differ.

**5. Period management and close orchestration.** A period must be openable, closable, soft-closeable (close to subledgers but not GL), reopenable, and lockable per module. Year-end carryforward of P&L to retained earnings must be automatic. All four support the basics; orchestration tooling (close checklists, task management, automated dependencies) is stronger in cloud-era products (NetSuite Period Close Checklist, Oracle Account Reconciliation Cloud, D365's task-based close) than in core SAP S/4HANA, which historically relied on third-party close-management tools or SAP Financial Closing Cockpit.

**6. Intercompany handling end-to-end.** From operational intercompany sales (with automatic mirroring) to intercompany cost allocation to consolidation eliminations. SAP, Oracle Fusion, and D365 each support full IC handling natively for groups using their consolidation modules. NetSuite supports it through OneWorld with Advanced Intercompany Journals and elimination subsidiaries.

**7. Subledger event-class architecture.** The recurring pattern across vendors — *event class → event type → rule set → GL line* — is no accident. It exists because business events have lifecycle states (created → validated → adjusted → canceled → cleared) and each state may need different accounting treatment, possibly across multiple ledgers. A modern ERP should expose this lifecycle explicitly. Systems that do not (older mid-market products that conflate "create" and "post") are categorically less capable.

**8. Revenue recognition under ASC 606 / IFRS 15.** Performance-obligation-based recognition with allocation of transaction price, contract assets and liabilities, and reclassification at period end. SAP RAR / event-based revenue recognition; Oracle Revenue Management Cloud; D365 Revenue Recognition; NetSuite Advanced Revenue Management. All four have native modules; depth of automation for variable consideration, contract modifications, and combined contracts varies considerably.

**9. Lease accounting under ASC 842 / IFRS 16.** ROU asset and lease liability recognition, periodic interest and amortization, modifications and remeasurements, terminations. SAP FI-LA, Oracle Fusion Lease Accounting, D365 Asset Leasing, NetSuite Lease Accounting (recent SuiteApp). All four have it natively; D365's posting layer model is particularly elegant for IFRS-vs-GAAP dual reporting on the same lease.

**10. Cost accounting beyond GL.** Standard costing, actual costing, FIFO, LIFO (where permitted), moving average; multi-currency inventory valuation; landed cost; overhead absorption; production variance analysis. SAP's Material Ledger is the most sophisticated. Oracle Fusion Cost Accounting is comprehensive. D365 Inventory Costing supports most methods. NetSuite supports Standard, FIFO, LIFO, Average, and Group Average natively but lacks SAP's depth in multi-step variance analysis.

**11. Project / job costing with capitalization paths.** A modern ERP must support time-and-materials, fixed-price (with milestones and POC), investment (capitalized to fixed assets), and internal projects, and route project costs to the correct GL destination automatically. SAP PS, Oracle Fusion PPM, D365 Project Operations, NetSuite SuiteProjects all do this; depth of integration with the procurement and HR side varies.

**12. Sub-second drill-back from GL to source.** From a P&L line, a user should be able to traverse: GL account → period balance → individual journal lines → originating subledger document → operational transaction (PO, invoice, fulfillment) → contract / requisition / agreement. The four systems support this to varying depths; SAP's ACDOCA gives the most direct technical drill-back because the source data is in one table.

**13. Audit trail and immutability.** Every posting must be auditable: who, when, what was changed, what original/replacement values were used. Reversal mechanisms must produce a separate document, not delete the original. All four systems comply with SOX / IFRS audit requirements; NetSuite's audit trail is browser-accessible and fine-grained, while SAP and Oracle rely on dedicated audit reporting workbenches.

**14. Custom transaction types and custom GL segments.** The ability to extend the model — to add a new kind of transaction with its own statuses and posting behavior, and to add new reporting dimensions beyond the standard chart fields. NetSuite's SuiteGL is the most explicit framework for this. SAP supports it through extensions to ACDOCA and custom T-codes. Oracle Fusion has Accounting Hub Cloud Service for non-Fusion source applications and supports custom event classes. D365 has Extensions and Posting Definitions.

**15. Statistical / non-financial postings.** For allocations driven by headcount, square footage, machine hours, or units produced. Required for accurate cost accounting in any non-trivial organization.

### 7.3 An "optimal" reference architecture

Pulling these threads together, the optimal modern ERP transaction architecture has the following properties:

**Single line-item store, multi-ledger.** A single physical store for transaction lines (SAP's ACDOCA approach is the best-realized example), with ledger as a dimension on each line rather than a separate physical structure. This eliminates reconciliation between FI/CO/AA/ML at the data level.

**Event-driven subledger accounting layer.** Every operational transaction raises typed lifecycle events (created → validated → adjusted → canceled → cleared, with extensions for module-specific states). A configurable rule layer (Oracle Fusion's SLA is the best-realized example) translates events into ledger lines via journal entry rule sets, supporting:
- Multiple simultaneous accounting treatments per event (different rules per ledger / per accounting standard)
- Conditional account derivation based on transaction attributes and reference data
- Date-effective rules for retroactive accounting policy changes
- Full audit of which rule produced which line

**Visible GL impact at the operational record.** Even with a powerful rule layer, accountants and operational users need to see *what posted* without leaving the source record (NetSuite's GL Impact page is the best-realized example). Drill-back must be reciprocal: from GL line to source, *and* from source to GL impact.

**Posting-layer model for parallel accounting.** Lightweight tagging of every posting with one of N layers (D365's posting layer model is the best-realized example for tagged transactions; SAP's parallel-ledger-plus-extension-ledger model is the best-realized for ledger-as-derivation). The user picks which combination of layers to include in any given report. Custom layers handle bespoke needs (e.g., management adjustments, restated comparatives).

**Configurable customization framework.** A first-class extension model — custom transaction types, custom segments, scripted GL line modifications (NetSuite's SuiteGL is the best-realized framework example) — that survives upgrades.

**Always-non-posting types are explicit.** Quotes, orders, RFQs, requisitions, opportunities, transfer orders, return authorizations, work orders — all should be modeled as commitment-only records with no GL impact. Approval workflows can change posting state; permanent non-posting types should be enumerated and documented.

**Native modules for the modern accounting standards.** ASC 606 / IFRS 15 revenue recognition and ASC 842 / IFRS 16 lease accounting must be native and integrated with the same operational subledgers, not external bolt-ons.

**Posting/non-posting separation visible everywhere.** Reports, audit logs, and the user interface should make it instantly obvious which transaction types post to GL and which do not — both in the data model and in the UI.

No single existing system embodies *all* of these strengths. SAP S/4HANA has the most elegant data model and the deepest operational integration but the steepest implementation complexity. Oracle Fusion has the most powerful rule layer but the least visible GL impact at the operational record. D365 F&O has the most accessible posting-layer model but the least sophisticated cost accounting. NetSuite has the most visible GL impact and the most extensible customization framework but the least depth in cost accounting and the lowest scalability ceiling. The "optimal" model is therefore a composite — what each vendor would build if it took the best of the others' approaches.

---

## 8. Summary of corrections to the prior reference

For traceability, the substantive corrections vs. the May 2025 / 2026 prior version of this document are summarized here:

1. **Oracle Fusion Cloud Payables event types** — replaced legacy E-Business Suite naming (`CASH_APPLIED`, `FUTURE_CLEARED`, `MEMO_APPLICATION`, `MISC_INSERT`, etc.) with the current Cloud event classes and types as documented in Oracle Cloud 25B/26B. Added missing event classes: Bills Payable, Escheated Payments, Reconciled Payments, Refunds, Third-Party Merge, Adjustment Entry.
2. **Oracle Fusion Receivables** — added missing event classes (Adjustment, Bills Receivable, Chargeback) and clarified Channel Revenue Management as a separate module rather than a subset of Receivables.
3. **Oracle Fusion Assets** — replaced minimal four-event list with the complete predefined event class set across all four event entities (Transactions, Depreciation, Inter Asset Transactions, Deferred Depreciation).
4. **Added Oracle Fusion modules** previously omitted entirely: Receipt Accounting, Cost Accounting, Lease Accounting, Cash Management, Project Costing/Billing, Channel Revenue Management.
5. **SAP standard FI document types** — removed customer-specific contaminants (AL, BA, CA, DN, DS, ZP, ZR, ZS, ZV, etc.) that were not part of the SAP standard set; added missing standard types (DZ, KA, KG, AZ, AP, X1, X2, EU, EX, FC, CO).
6. **SAP CO business transactions** — corrected mislabeling as "transaction codes" (they are business transactions in TJ01); added Actual counterparts to the Plan-only list previously given; corrected definition of PAPL ("Plan Sales / Profit", not "Profitability Planning"); removed embedded Naver blog URL artifact.
7. **SAP movement types** — added the missing 702 (and 703/704/707/708 for QI and blocked physical inventory variants); separated 451/453/455/457/459 grouping (455 is location-to-location, distinct from the others which transfer status); added 315 (storage location two-step receipt leg); clarified that movement types come in pairs (101/102, 201/202, 601/602, etc.).
8. **D365 F&O posting layers** — corrected from three to ten (Current, Operations, Tax, plus Custom 1 through 7); explained their use for parallel accounting.
9. **D365 F&O project types** — corrected from five (with Cost and Internal collapsed) to six distinct types (Time and material, Fixed-price, Investment, Cost, Internal, Time).
10. **D365 F&O Posting Profiles vs. Posting Definitions** — distinguished the two as separate mechanisms rather than synonyms.
11. **D365 F&O ledger journal types** — added missing types (Allocation, Approval, Bank check reversal, Bank deposit slip cancellation, Budget, Daily, Elimination, Fixed asset budget, Invoice register, Payroll disbursement, Periodic, Reporting currency adjustment, etc.).
12. **D365 F&O fixed asset transactions** — added missing types (Bonus depreciation, Extraordinary depreciation, Provisions for reserves, Reversal of provisions, Acquisition adjustment, Depreciation adjustment).
13. **NetSuite architecture description** — replaced "real-time, hard-coded" framing with accurate description of NetSuite's configurable GL via item/entity assignments + SuiteGL framework (Custom GL Lines plug-in + Custom Transaction Types + Custom GL Segments).
14. **NetSuite transaction types** — explicitly classified each as posting or non-posting, distinguishing always-non-posting types per Oracle/NetSuite documentation (Estimate, Liability Adjustment, Opportunity, Payroll Adjustment, Purchase Order, Return Authorization, Revenue Arrangement, Sales Order, Transfer Order, Vendor Return Authorization, Work Order). Corrected misclassification of Wave, Inventory Distribution, Inventory Status Change, Engineering Change Order, Inventory Worksheet, Inventory Count, and Bin Putaway / Bin Transfer as posting transactions.
15. **Added topic coverage previously absent or thin**: parallel ledgers and extension ledgers, Universal Parallel Accounting, lease accounting (all four vendors), revenue recognition (all four vendors), cost accounting and material ledger, intercompany handling, statistical postings, period close orchestration.
16. **Removed marketing-source citations** in favor of vendor primary documentation (SAP Help Portal, Oracle Cloud Documentation, Microsoft Learn, Oracle NetSuite Application Help). Removed embedded URL artifacts from the prior version.

---

## 9. Source notes

This document was written against the following primary sources, in approximate order of authority:

- **SAP**: SAP Help Portal (`help.sap.com`), SAP Community blog posts authored by SAP employees, SAP Press publications. Movement type semantics drawn from SAP Inventory Management documentation. Document type set drawn from standard T003 delivery.
- **Oracle Fusion Cloud**: Oracle Cloud Documentation 25B, 25C, 25D, and 26B releases (`docs.oracle.com/en/cloud/saas/financials/...` and `docs.oracle.com/en/cloud/saas/human-resources/...` and `docs.oracle.com/en/cloud/saas/supply-chain-and-manufacturing/...`).
- **Microsoft Dynamics 365 Finance & Operations**: Microsoft Learn (`learn.microsoft.com/en-us/dynamics365/finance/...`), MicrosoftDocs GitHub repository for the public docs.
- **Oracle NetSuite**: Oracle NetSuite Application Help (`docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/...`).
- Industry analyst material and partner blogs were cross-referenced but were not used as primary sources for any factual claim.

The document is dated to the products' state as of early 2026. Cloud ERP releases are quarterly; specific event class lists evolve, particularly in Oracle Fusion Cloud (new event classes are added in roughly half the quarterly releases) and in NetSuite (custom transaction types and SuiteGL capabilities expand through SuiteApps). Treat the enumerations as current-as-of-revision rather than perpetual.
