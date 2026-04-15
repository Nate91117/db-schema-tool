"""Multi-provider AI client abstraction.

Wraps Anthropic, Google Gemini, and OpenAI with a common interface so
stage2 and stage3 don't depend on any one SDK.

Usage:
    client = AIClient(provider="anthropic", model="claude-haiku-4-5-20251001")
    text, tokens = client.complete(system_prompt, user_prompt, max_tokens=4096)
"""
from __future__ import annotations

import os
import logging

log = logging.getLogger("dbscan")

# Default model IDs per provider and stage
DEFAULT_MODELS: dict[str, dict[str, str]] = {
    "anthropic": {
        "score": "claude-haiku-4-5-20251001",
        "annotate": "claude-sonnet-4-6",
    },
    "google": {
        "score": "gemini-2.0-flash",
        "annotate": "gemini-2.5-pro",
    },
    "openai": {
        "score": "gpt-4o-mini",
        "annotate": "gpt-4o",
    },
}

SUPPORTED_PROVIDERS = list(DEFAULT_MODELS.keys())


def get_default_model(provider: str, stage: str) -> str:
    """Return the default model for a provider and stage ('score' or 'annotate')."""
    return DEFAULT_MODELS.get(provider, {}).get(stage, "")


class AIClient:
    """Unified AI client for Anthropic, Google Gemini, and OpenAI.

    Args:
        provider: "anthropic", "google", or "openai"
        model:    Model ID to use (e.g. "claude-haiku-4-5-20251001")
        api_key:  API key; falls back to provider-specific env vars if omitted
    """

    def __init__(self, provider: str, model: str, api_key: str | None = None):
        self.provider = provider.lower()
        self.model = model
        self._api_key = api_key
        self._client = None     # Anthropic / OpenAI client object
        self._genai = None      # google.generativeai module (lazy)

        if self.provider not in SUPPORTED_PROVIDERS:
            raise ValueError(
                f"Unknown provider '{provider}'. "
                f"Choose from: {', '.join(SUPPORTED_PROVIDERS)}"
            )

        self._init_client()

    # ── Initialisation ────────────────────────────────────────────────────────

    def _init_client(self) -> None:
        if self.provider == "anthropic":
            try:
                import anthropic
            except ImportError:
                raise ImportError(
                    "Anthropic SDK not installed.\n"
                    "  pip install anthropic"
                )
            key = self._api_key or os.getenv("ANTHROPIC_API_KEY")
            if not key:
                raise ValueError(
                    "Anthropic API key not set. "
                    "Pass --api-key or set ANTHROPIC_API_KEY."
                )
            self._client = anthropic.Anthropic(api_key=key)

        elif self.provider == "google":
            try:
                import google.generativeai as genai
            except ImportError:
                raise ImportError(
                    "Google Generative AI SDK not installed.\n"
                    "  pip install google-generativeai"
                )
            key = (
                self._api_key
                or os.getenv("GOOGLE_API_KEY")
                or os.getenv("GEMINI_API_KEY")
            )
            if not key:
                raise ValueError(
                    "Google API key not set. "
                    "Pass --api-key or set GOOGLE_API_KEY / GEMINI_API_KEY."
                )
            genai.configure(api_key=key)
            self._genai = genai

        elif self.provider == "openai":
            try:
                import openai
            except ImportError:
                raise ImportError(
                    "OpenAI SDK not installed.\n"
                    "  pip install openai"
                )
            key = self._api_key or os.getenv("OPENAI_API_KEY")
            if not key:
                raise ValueError(
                    "OpenAI API key not set. "
                    "Pass --api-key or set OPENAI_API_KEY."
                )
            self._client = openai.OpenAI(api_key=key)

    # ── Public interface ──────────────────────────────────────────────────────

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
    ) -> tuple[str, int]:
        """Send a prompt and return (response_text, token_count).

        Token count is best-effort — may be 0 for providers that don't surface it
        easily.  Raises the underlying SDK exception on failure (callers handle
        retries).
        """
        if self.provider == "anthropic":
            return self._complete_anthropic(system_prompt, user_prompt, max_tokens)
        elif self.provider == "google":
            return self._complete_google(system_prompt, user_prompt, max_tokens)
        elif self.provider == "openai":
            return self._complete_openai(system_prompt, user_prompt, max_tokens)
        return "", 0

    # ── Provider implementations ──────────────────────────────────────────────

    def _complete_anthropic(
        self, system_prompt: str, user_prompt: str, max_tokens: int
    ) -> tuple[str, int]:
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = response.content[0].text.strip()
        tokens = response.usage.input_tokens + response.usage.output_tokens
        return text, tokens

    def _complete_google(
        self, system_prompt: str, user_prompt: str, max_tokens: int
    ) -> tuple[str, int]:
        genai = self._genai
        model = genai.GenerativeModel(
            model_name=self.model,
            system_instruction=system_prompt,
        )
        response = model.generate_content(
            user_prompt,
            generation_config=genai.GenerationConfig(
                max_output_tokens=max_tokens,
            ),
        )
        text = response.text.strip()
        try:
            tokens = response.usage_metadata.total_token_count
        except Exception:
            tokens = 0
        return text, tokens

    def _complete_openai(
        self, system_prompt: str, user_prompt: str, max_tokens: int
    ) -> tuple[str, int]:
        response = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = response.choices[0].message.content.strip()
        try:
            tokens = response.usage.total_tokens
        except Exception:
            tokens = 0
        return text, tokens

    def __repr__(self) -> str:
        return f"AIClient(provider={self.provider!r}, model={self.model!r})"
