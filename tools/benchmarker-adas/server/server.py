from flask import Flask, request, jsonify, Response
import importlib
import time

# Dynamically import your function
module = importlib.import_module("main")  # assumes your function lives in main.py
handler = getattr(module, "handler")

app = Flask(__name__)


@app.route("/", methods=["POST"])
def handle() -> Response:
    event = request
    start = time.perf_counter()
    result = handler(event)
    elapsed = time.perf_counter() - start

    return jsonify({"result": result, "server_elapsed": elapsed})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
