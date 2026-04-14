#!/usr/bin/env python3
import http.server
import os

PORT = 8080
BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard")

class DashHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", ""):
            self.send_response(302)
            self.send_header("Location", "/dashboard")
            self.end_headers()
        else:
            super().do_GET()

    def translate_path(self, path):
        for prefix in ("/dashboard", "/dash"):
            if path.startswith(prefix):
                path = path[len(prefix):] or "/"
                break
        return super().translate_path(path)

    def log_message(self, fmt, *args):
        pass  # silence request logs

if __name__ == "__main__":
    handler = lambda *a, **kw: DashHandler(*a, directory=BASE, **kw)
    with http.server.HTTPServer(("", PORT), handler) as s:
        s.serve_forever()
