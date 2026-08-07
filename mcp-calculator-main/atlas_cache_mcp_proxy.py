#!/usr/bin/env python3"""ATLAS CACHE-FIRST MCP PROXYVersion: 1.1.0

Purpose

Sits between Xiaozhi's MCP bridge and duckduckgo-mcp-server.

Preserves the same MCP tools exposed by the upstream server.

Intercepts only tools/call for:

search        -> use fresh GitHub Pages research cache when relevant.

fetch_content -> use cached fetched_text when URL already exists.

Falls back transparently to the live DuckDuckGo MCP server on cache miss,stale cache, disabled cache, or cache/network errors.

Does NOT create a keep-alive mechanism and does NOT replace Render's bridge.

Render/GitHub cache configuration

Preferred:ATLAS_RESEARCH_CACHE_URL=https://OWNER.github.io/REPO/atlas_research.json

Alternative:ATLAS_GITHUB_REPOSITORY=OWNER/REPO

Optional:ATLAS_CACHE_REFRESH_SECONDS=60ATLAS_CACHE_MAX_AGE_SECONDS=3600ATLAS_CACHE_TIMEOUT_SECONDS=3ATLAS_CACHE_MIN_SCORE=1ATLAS_CACHE_MAX_SEARCH_CHARS=9000ATLAS_CACHE_MAX_FETCH_CHARS=12000

The upstream DuckDuckGo M
