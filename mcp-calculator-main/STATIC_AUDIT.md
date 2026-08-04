# ATLAS CACHE-FIRST — STATIC AUDIT

## Kết luận

**STATIC AUDIT: PASS**

Runtime production vẫn **CHƯA được gọi PASS** cho đến khi deploy trên Render + GitHub Pages thật và thấy log cache hit/fallback.

## Phạm vi

- `mcp_pipe.py`: giữ bridge NODE-2 hiện tại.
- `atlas_cache_mcp_proxy.py`: lớp cache-first mới, nằm giữa bridge và DuckDuckGo MCP.
- `mcp_config.json`: đổi target `duckduckgo-web-search` sang proxy local.
- `research_worker.py`: giữ worker GitHub Actions hiện tại.
- `atlas_research.yml`: giữ lịch Research + deploy GitHub Pages hiện tại.

## Static checks

| Kiểm tra | Kết quả |
|---|---|
| `mcp_pipe.py` Python compile | PASS |
| `atlas_cache_mcp_proxy.py` Python compile | PASS |
| `research_worker.py` Python compile | PASS |
| `mcp_config.json` JSON parse | PASS |
| Proxy giữ nguyên MCP `initialize` passthrough | PASS mô phỏng |
| Proxy giữ nguyên `tools/list` passthrough | PASS mô phỏng |
| `search` cache hit trả MCP CallToolResult hợp lệ | PASS mô phỏng |
| `fetch_content` cache hit trả MCP CallToolResult hợp lệ | PASS mô phỏng |
| Cache miss chuyển sang DuckDuckGo live | PASS mô phỏng |
| Cache lỗi/cũ không chặn live fallback | PASS theo code path |
| Không thêm ping nội bộ 24/7 | PASS |
| Không thêm dependency ngoài requirements hiện có | PASS (`aiohttp` đã có) |

## Runtime PASS cần thấy sau deploy

1. GitHub Pages `atlas_research.json` mở được và `generated_at` còn mới.
2. Render log có:
   - `ATLAS Cache Proxy v1.0.0 starting`
   - `[CACHE] ENABLED`
   - `[CACHE] REFRESH PASS`
3. Hỏi tin nằm trong cache:
   - `[CACHE] SEARCH HIT`
4. Nếu Xiaozhi gọi URL đã cache:
   - `[CACHE] FETCH HIT`
5. Hỏi nội dung không có trong cache:
   - `[CACHE] SEARCH MISS`
   - sau đó `MCP tool result OK | tool=search` từ đường live.

## Giới hạn đã khóa rõ

Cache-first làm giảm thời gian `search/fetch` khi NODE-2 đang chạy. Nó **không thể tự bỏ qua Render** vì MCP của Xiaozhi vẫn đi qua bridge NODE-2. Nếu Render Free bị idle spin-down, cần inbound monitor bên ngoài hoặc gói dịch vụ không sleep để loại phần cold-start đó.
