ATLAS NODE-2 — FIX ĐỌC TIN / FETCH

1. mcp_config.json đã thêm server-wide:
   --fetch-backend auto
   để fetch_content thử httpx trước và tự fallback sang browser/curl khi site chặn.

2. research_worker.py đã sẵn gọi fetch_content với backend=auto.

3. mcp_pipe.py giữ nguyên V2.2.0 TEST; không biến sleep-test hữu hạn thành keep-alive vĩnh viễn.

4. Render Free vẫn có thể sleep. Bộ này KHÔNG tuyên bố loại bỏ cold-start của Render Free.
   Muốn live MCP không cold-start thì cần hạ tầng always-on; GitHub Pages cache chỉ là đường cache/public, không thay thế WebSocket bridge nếu Xiaozhi bắt buộc đi qua Render.

PASS sau deploy cần thấy khi hỏi một tin mới:
- MCP tools/call -> search
- MCP tool result OK | tool=search
- MCP tools/call -> fetch_content
- MCP tool result OK | tool=fetch_content

Nếu chỉ thấy search mà không thấy fetch_content thì vấn đề nằm ở cách Xiaozhi/agent chọn tool, không phải DuckDuckGo server config.
