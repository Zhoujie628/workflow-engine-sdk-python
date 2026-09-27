# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Optional helper for fetching AgentCards from the Registry Center.

Users can use this, or fetch AgentCards from any other source.
The SDK does not depend on this module.

TLS options mirror :class:`~workflow_engine.client.a2a_transport.A2ATransport`
(``ssl_verify`` plus an optional custom CA, mTLS client identity and CRL). A
boolean switch alone left a deployment behind a private CA with no option except
disabling verification outright, so the registry helpers accept the same
material the transport does.
"""

import json
from typing import Any, Dict, List, Optional
from loguru import logger

from workflow_engine.client.agentcard_normalizer import normalize_agent_dict
from workflow_engine.client.ssl_context import create_ssl_context


def _tls_options(
    ssl_verify: bool = True,
    ca_certs_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
    client_key_password: Optional[str] = None,
    crl_path: Optional[str] = None,
):
    """Build the httpx ``verify`` argument, failing closed on bad TLS material."""
    return create_ssl_context(
        verify_server=ssl_verify,
        ca_certs_path=ca_certs_path,
        cert_path=client_cert_path,
        key_path=client_key_path,
        key_password=client_key_password,
        crl_path=crl_path,
    )


def _http_timeout(connect_seconds: float, read_seconds: float):
    import httpx

    if connect_seconds <= 0 or read_seconds <= 0:
        raise ValueError("connect and read timeouts must be positive")
    return httpx.Timeout(
        connect=connect_seconds, read=read_seconds,
        write=read_seconds, pool=connect_seconds,
    )


async def load_psop(
    base_url: str,
    psop_id: str,
    access_token: str = None,
    ssl_verify: bool = True,
    ca_certs_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
    client_key_password: Optional[str] = None,
    crl_path: Optional[str] = None,
    *,
    connect_timeout_seconds: float = 30.0,
    read_timeout_seconds: float = 30.0,
) -> "Workflow":
    """Fetch a PSOP from the orchestration center external API.

    Uses the public external endpoint GET /api/v1/orchestrate/psop/{psop_id}.
    Pass access_token when the orchestration center has external auth enabled.
    Set ssl_verify=False for self-signed certs (dev only).
    """
    import httpx
    from workflow_engine.core.models import Workflow
    url = f"{base_url}/api/v1/orchestrate/psop/{psop_id}"
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    logger.info(f"[Registry] Loading PSOP from {url} (ssl_verify={ssl_verify})")
    async with httpx.AsyncClient(
        verify=_tls_options(
            ssl_verify, ca_certs_path, client_cert_path, client_key_path,
            client_key_password, crl_path,
        ),
        timeout=_http_timeout(connect_timeout_seconds, read_timeout_seconds),
        follow_redirects=False,
    ) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    wf = Workflow.from_dict(data.get("data", data))
    logger.info(f"[Registry] Loaded workflow: {wf.name}, {len(wf.steps)} steps")
    return wf


async def search_psop(
    base_url: str,
    intent: str,
    top_n: int = 5,
    access_token: str = None,
    ssl_verify: bool = True,
    ca_certs_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
    client_key_password: Optional[str] = None,
    crl_path: Optional[str] = None,
    *,
    connect_timeout_seconds: float = 30.0,
    read_timeout_seconds: float = 30.0,
) -> List["WorkflowSearchResult"]:
    """Search for matching PSOP workflows from the orchestration center.

    Uses the public external endpoint POST /api/v1/orchestrate/search.
    Returns a list of WorkflowSearchResult summary objects. To get the full
    workflow with steps, take ``workflow_id`` from a result and call
    ``load_psop(base_url, workflow_id, ...)``. Mirrors the Java SDK's
    LoadPsop.search which returns WorkflowSearchResult.
    """
    import httpx
    from workflow_engine.core.models import WorkflowSearchResult
    url = f"{base_url}/api/v1/orchestrate/search"
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    body = {"intent": intent, "top_n": top_n}
    logger.info(f"[Registry] Searching PSOP at {url} (intent_chars={len(intent)}, top_n={top_n})")
    async with httpx.AsyncClient(
        verify=_tls_options(
            ssl_verify, ca_certs_path, client_cert_path, client_key_path,
            client_key_password, crl_path,
        ),
        timeout=_http_timeout(connect_timeout_seconds, read_timeout_seconds),
        follow_redirects=False,
    ) as client:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    raw_results = data.get("data", [])
    results = [WorkflowSearchResult.from_dict(r) for r in raw_results]
    logger.info(f"[Registry] Search returned {len(results)} workflow(s)")
    return results


class RegistryClient:
    """Fetches AgentCards from the Registry Center.

    ``access_token`` is attached to every request, so a registry with
    authentication enabled is reachable through the same client the card
    helpers use.
    """

    def __init__(
        self,
        url: str,
        ssl_verify: bool = True,
        access_token: Optional[str] = None,
        ca_certs_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
        client_key_password: Optional[str] = None,
        crl_path: Optional[str] = None,
        *,
        connect_timeout_seconds: float = 30.0,
        read_timeout_seconds: float = 30.0,
    ):
        self.url = url.rstrip("/")
        self.ssl_verify = ssl_verify
        self.access_token = access_token
        self._timeout = _http_timeout(connect_timeout_seconds, read_timeout_seconds)
        # Built once so a missing or invalid TLS file fails at construction
        # rather than halfway through a fetch.
        self._verify = _tls_options(
            ssl_verify, ca_certs_path, client_cert_path, client_key_path,
            client_key_password, crl_path,
        )

    def _headers(self) -> Dict[str, str]:
        if not self.access_token:
            return {}
        return {"Authorization": f"Bearer {self.access_token}"}

    async def fetch_agent_cards(self) -> List[Any]:
        """Fetch all AgentCards. Returns protobuf objects if a2a-sdk available, else dicts."""
        import httpx
        logger.info(f"[Registry] Fetching all agent cards from {self.url}")
        async with httpx.AsyncClient(verify=self._verify, timeout=self._timeout) as client:
            resp = await client.get(
                f"{self.url}/rest/v1/registry-center/agent-cards",
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            raw_cards = data.get("agentCards", data.get("data", []))
        logger.info(f"[Registry] Received {len(raw_cards)} agent card(s)")
        from a2a.types import AgentCard
        from google.protobuf.json_format import Parse
        cards = []
        for raw in raw_cards:
            normalized = normalize_agent_dict(raw)
            cards.append(Parse(json.dumps(normalized), AgentCard()))
        logger.info(f"[Registry] Parsed {len(cards)} AgentCard(s) into protobuf objects")
        return cards

    async def fetch_agent_card(self, name: str, organization: str = None) -> Any:
        """Fetch a single AgentCard by name."""
        import httpx
        logger.info(f"[Registry] Fetching agent card: name={name}, org={organization}")
        params = {"name": name}
        if organization:
            params["organization"] = organization
        async with httpx.AsyncClient(verify=self._verify, timeout=self._timeout) as client:
            resp = await client.get(
                f"{self.url}/rest/v1/registry-center/agent-cards",
                params=params,
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            cards = data.get("agentCards", data.get("data", []))
            if not cards:
                logger.warning(f"[Registry] Agent card not found: name={name}")
                return None
            raw = cards[0]
            from a2a.types import AgentCard
            from google.protobuf.json_format import Parse
            normalized = normalize_agent_dict(raw)
            card = Parse(json.dumps(normalized), AgentCard())
            logger.info(f"[Registry] Agent card parsed: name={name}")
            return card

    async def register_agent_card(self, agent_card) -> dict:
        """Register or update an AgentCard in the registry.

        POSTs to /rest/v1/registry-center/agent-cards with the card wrapped
        in an ``agentCards`` list. Mirrors the Java SDK's
        RegistryClient.registerAgentCard.
        """
        import httpx
        if isinstance(agent_card, dict):
            card_payload = agent_card
        else:
            from google.protobuf.json_format import MessageToDict
            card_payload = MessageToDict(agent_card)
        url = f"{self.url}/rest/v1/registry-center/agent-cards"
        payload = {"agentCards": [card_payload]}
        logger.info(f"[Registry] Registering agent card: name={card_payload.get('name', '?')}")
        async with httpx.AsyncClient(verify=self._verify, timeout=self._timeout) as client:
            resp = await client.post(url, json=payload, headers=self._headers())
            resp.raise_for_status()
            result = resp.json()
        logger.info(f"[Registry] Agent card registered: name={card_payload.get('name', '?')}")
        return result

    @property
    def base_url(self) -> str:
        return self.url
