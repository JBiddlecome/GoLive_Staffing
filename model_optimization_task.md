# TASK: OpenAI Model Optimization & Cost Reduction

## Objective
Review the codebase for the 'GoLive Tools' project and update all OpenAI API calls to use the most cost-effective models available as of May 2026.

## Current Context
Based on my recent spend charts:
- Primary models used: `gpt-4o` and `gpt-4o-mini`.
- High usage in: Python/Flask routes and background automation tasks.

## Targeted Model Upgrades
Please search the repository for all `client.chat.completions.create` calls and apply the following logic:

1. **Production Logic Upgrade:**
   - Swap `gpt-4o` for **`gpt-5.4-mini`**. It provides superior reasoning at roughly 60% less cost.
   
2. **Simple Automation/Classification:**
   - Swap `gpt-4o-mini` for **`gpt-4.1-nano`** ($0.10/$0.40 per 1M tokens). Use this for any non-complex routing or data parsing.

3. **Optimization Refactors:**
   - **Environment Variables:** If model names are hardcoded, refactor them to use an environment variable (e.g., `os.getenv("OPENAI_MODEL")`).
   - **Prompt Caching:** Review headers/system prompts. If a large system prompt is used repeatedly, ensure the code is structured to trigger OpenAI's [Prompt Caching](https://openai.com/api/pricing/) for the 90% discount.
   - **Batch API:** Identify any async tasks (like bulk marketing outreach) that can be moved to the `Batches` endpoint for an additional 50% discount.

## Verification
After making changes, run the local test suite to ensure that the response parsing logic (especially for JSON outputs) remains compatible with the newer models.