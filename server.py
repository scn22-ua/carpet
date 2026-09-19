# server.py
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(os.environ.get("PORT", 3000))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Hello World desde Railway! \n   Me llamo Sergi y estoy aprendiendo a desplegar aplicaciones en Railway. \n   Este es un ejemplo de una aplicacion en Python que responde a solicitudes HTTP. \n   Espero que te guste! \n")

print(f"Escuchando en el puerto {port}", flush=True)

HTTPServer(("0.0.0.0", port), Handler).serve_forever()