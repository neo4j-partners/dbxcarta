"""Transport-neutral graph_rag retrieval and prompt assembly.

Single source of truth for the question-to-prompt stage of the graph_rag
arm. The evaluation harness (``eval/arms.py``) builds context and prompt
through ``build_graph_rag_context``, keeping retrieval and prompt assembly in
one place.

The model call is deliberately excluded. The harness generates SQL through a
Model Serving REST call (see ``generation.py`` and ``local_generation.py``) and
adds a local response cache on top. The seam stops once the prompt is built so
the caller keeps its own model transport and post-generation handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dbxcarta.client.embed import embed_questions
from dbxcarta.client.prompt import graph_rag_prompt

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from dbxcarta.client.retriever import ContextBundle, Retriever
    from dbxcarta.client.settings import ClientSettings


@dataclass(frozen=True)
class GraphRagContext:
    """Result of the shared graph_rag retrieval-and-prompt stage.

    ``bundle`` is the raw retrieved bundle so the harness can compute
    evaluation traces without a second retrieval; ``context_text`` and
    ``prompt`` are the rendered artifacts both callers feed to their model.
    """

    question: str
    bundle: ContextBundle
    context_text: str
    prompt: str

    @property
    def seed_ids(self) -> list[str]:
        return self.bundle.seed_ids


def build_graph_rag_context(
    ws: WorkspaceClient,
    settings: ClientSettings,
    question: str,
    *,
    embedding: list[float] | None = None,
    retriever: Retriever | None = None,
) -> GraphRagContext:
    """Embed (if needed), retrieve, render context, and build the prompt.

    Stops before any model call. When ``embedding`` is given it is used
    as-is; otherwise the question is embedded via the configured endpoint.
    When ``retriever`` is given it is used and left open so the caller owns
    its lifecycle (the harness reuses one retriever across a threaded
    batch); otherwise a ``GraphRetriever`` is constructed and closed here.
    """
    if embedding is None:
        embeddings, embed_error = embed_questions(ws, settings.dbxcarta_embed_endpoint, [question])
        if embeddings is None:
            raise RuntimeError(
                f"graph_rag embedding failed against "
                f"{settings.dbxcarta_embed_endpoint!r}: {embed_error}"
            )
        embedding = embeddings[0]

    owns_retriever = retriever is None
    if retriever is None:
        # Lazy import keeps Neo4j out of the import path for callers that
        # only use the lightweight client API. See pyproject 'graph' extra.
        from dbxcarta.client.graph_retriever import GraphRetriever

        retriever = GraphRetriever(settings)
    try:
        bundle = retriever.retrieve(question, embedding)
    finally:
        if owns_retriever:
            retriever.close()

    context_text = bundle.to_text()
    prompt = graph_rag_prompt(
        question,
        settings.dbxcarta_catalog,
        settings.schemas_list,
        context_text,
    )
    return GraphRagContext(
        question=question,
        bundle=bundle,
        context_text=context_text,
        prompt=prompt,
    )
