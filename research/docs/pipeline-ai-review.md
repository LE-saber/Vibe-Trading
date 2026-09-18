# P10F non-tool AI review

Status: P10F scoped historical review gate PASS for `p10f-20260903-003`, with
the manual terminology qualifications below. Production pipeline NOT_ACCEPTED.

S0–S4 remain program-owned. S5 reads a committed, hash-verified S4 shortlist and
the existing Core Evidence Store. It does not recalculate, reorder or change scores.
Its separate `review-inputs.json` freezes stage hashes, config, provider/model and
review code hashes, so accepted older S4 artifacts can be reviewed without rewriting
their original computation specification. Corrupt or mismatched inputs fail closed.

`scripts/review-research.py --run-id <completed-S4-run>` invokes the configured Vibe
model through `src.providers.llm.build_llm()` in Vibe's isolated Python environment.
There is no AgentLoop, tool binding, Tool.execute, market request or order operation.
The configured temperature must be zero. Current probe observed DeepSeek
`deepseek-v4-pro`; that is an observation, not a hardcoded fallback model.
For this provider config v1.4 requests JSON mode with thinking explicitly disabled
and a 4096-token output budget, recording the effective parameters in the frozen runtime.
DeepSeek's documented `max_tokens` is sent through SDK `extra_body`, because the
installed LangChain adapter renames its top-level argument to `max_completion_tokens`.
JSON mode still needs schema
and empty/truncated-response validation; see the [official DeepSeek JSON guide](https://api-docs.deepseek.com/guides/json_mode/).

Each packet contains a bounded system instruction and user evidence message,
explicit research/reference dates, canonical identity, original scores, actual
share-volume/CNY amounts, compact metrics, missing fields and sources. Complete
per-field provenance remains outside the prompt in the same durable packet.
The worker enforces2500–configured maximum characters, always below5000.

Responses must be strict JSON with exactly `symbol`, `verdict`, `strengths`, `risks`,
`evidence_conflicts`, `missing_critical_info`, `summary`. Every claim/summary cites
a supplied field. Unsupported fields, different identities, tool requests, score
changes, invented numeric prose and forecast/quantity fields are rejected. Narrative
is qualitative; actual numbers remain visible in the packet. EXCLUDE needs cited
risks/conflicts, KEEP needs cited strengths. Structural checks cannot prove semantic
truth; raw responses and evidence remain available for human inspection.

Config v1.4 controls concurrency(default3), maximum attempts(default2), timeout(default180s),
packet size(default4000), reasoning effort(default none), and output tokens(default4096).
`none` maps to DeepSeek `extra_body.thinking.type=disabled`; `configured/low/high/max`
remain supported explicitly. The [provider documents](https://api-docs.deepseek.com/guides/thinking_mode/)
that temperature has no effect in thinking mode; v1.3's configured zero temperature
must therefore not be described as effective deterministic sampling. Non-thinking
review uses the same configured model, not a fallback provider.
Older frozen screening configurations remain readable, but cannot acquire unrecorded
S5 generation settings: a new configured run is required. Child-only timeout and retry settings prevent Vibe's
long global timeout/SDK retries from silently expanding the pipeline budget. User
dotenv/settings and running services are unchanged.

Each stock saves its packet, attempt start, exact request/raw response where available,
attempt result and final checkpoint. Successful work is reused; a durable response can
be recovered after interruption. An ambiguous interrupted attempt consumes its slot
and is not silently resent. Confirmed model calls and unknown-count failures are
reported separately. Exhausted failures are system REVIEW/ai_eligible=false, not
model approval. One ordinary provider/schema failure does not cancel other stocks.
Changing inputs/model/code requires a fresh review specification; the immutable
checkpoint is never overwritten to grant extra retries.

S5 output symbols preserve the original finalist set for audit and the downstream
quote gate. This does NOT mean all passed: each row exposes `ai_status`, `ai_verdict`,
`ai_eligible`, `review_origin`, and the structured review. Only validated KEEP/WATCH
are eligible for later consideration. S5 completion leaves the overall pipeline
PARTIAL, pending realtime/account/portfolio and Web acceptance. No executable
quantities or final portfolio are created here.

An optional real single-name protocol smoke test is separate from batch acceptance:
`scripts/smoke-ai-review.py --run-id <id> --symbol <S4-symbol> --audit-id <unused-id>`.
It makes one actual model call and persists its packet, response and validation
result. Reusing a populated audit ID is rejected before any new billed request.

## Real protocol observations (not batch acceptance)

- `p10f-smoke-20260902-001`: bounded 90-second timeout, no durable response.
- `p10f-smoke-20260902-002`: actual DeepSeek JSON response, rejected for combined
  citations and references outside the supplied evidence fields.
- `p10f-smoke-20260903-001`: actual DeepSeek JSON response, rejected for `fields.`
  citation prefixes. Manual inspection also found a false conflict between complete
  single-date Dragon Tiger query coverage and non-aggregation of multi-window records.
  These facts describe different scopes; they are not inherently inconsistent.

The failed records remain immutable. A later protocol change does not retroactively
mark these attempts accepted. The full S0–S4 v1.3 run `p10f-20260903-001` used frozen
September 2 research context / September 1 completed daily data; it is historical
engineering evidence, not current September 3 market or execution evidence.

The first real twelve-name batch completed in about317 seconds:8 schema-valid,
4 exhausted failures,19 attempts,15 confirmed model calls and4 unknown-count failures.
No tools were called. A real resume reused all checkpoints without creating another
attempt(19 before/after). Its stage status is PARTIAL and its semantic assessment is
Needs revision, including false evidence conflicts and one ratio-scale error written
with Chinese numerals. See the run's `ai-semantic-review.md`. No output was rewritten.

v1.4 adds explicit scope definitions and empty-array guidance, groups repeated source
clocks without losing source/section associations, and rejects Chinese-number value
prose while retaining normal period labels. Individual real regressions
`p10f-smoke-20260903-002`(300189.SZ) and`...003`(600371.SH) passed the new protocol;
their cited directions/risks/missing fields were checked against packets. These are
small protocol checks only; final batch evidence follows.

## Accepted historical batch — 2026-09-03

`p10f-20260903-003` recalculated the full5009→300→60→20→12 path using
the same v1.4 config/snapshot as run002 and frozen real sources. S0–S4 full
semantic contents match exactly; no prior stage outputs were reused as computation.
Research date remains September2, completed daily reference September1.

- 12 valid structured reviews:9 WATCH,3 REVIEW;0 failures;16 confirmed requests,
 0 unknown-count attempts and0 tool calls. S5 elapsed51.599617 seconds.
- Base packets reserve128 characters for bounded, validator-generated retry feedback.
 No old response prose is echoed. All16 saved requests reconstruct to their exact
 `sent_messages_hash`; actual sent lengths3840–3967, below4000 including retries.
- Real resume creates no additional attempts:16 before/after. Successful checkpoints
 and raw responses are retained unchanged.
- `ai-review-audit.json/.md` verifies structural integrity, identity, original scores,
 source/config/code hashes, bounded attempts and full S0–S4 comparison with run002.
- `ai-semantic-review.md` records separate inspection of all12 final narratives.
 Earlier false coverage conflicts and the Chinese-numeral ratio-scale error did not
 recur. A few responses loosely say financing balance while citing five-day financing
 net buy/repayment. That wording is not independently observed balance-change data;
 the qualification must accompany downstream research reports. Original prose is kept.
- Cross-environment regression: Core436 + Market13 + Vibe114 =563 passed,4 skipped;
 one upstream deprecation, unchanged watched source hashes, no native-error markers.
 Report:`storage/artifacts/pipeline-engineering/p10a-tests-20260902T235906.json`.

Run001(PARTIAL,8 valid/4 failed) and run002(PARTIAL,10 valid/2 failed) remain as
immutable protocol-development evidence, not retroactively accepted batches.
Scoped gate:`storage/artifacts/pipeline-engineering/p10f-p10f-20260903-003.json/.md`.
S5 SUCCESS does not mean portfolio approval: quotes, real account, portfolio and
Web E2E are still outstanding. No final positions or executable orders were produced.
