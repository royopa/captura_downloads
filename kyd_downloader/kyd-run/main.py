import base64
import json
import logging
import os
from datetime import datetime, timezone

from flask import Flask, request
from parsers import MainProcessor

from kyd.data.logs import save_process_logs

project_id = "kyd-storage-001"
bucket_id = "ks-layer1"

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    processor = MainProcessor(project_id, bucket_id)
    envelope = request.get_json()
    process_log = None
    data = {"name": None}
    if not envelope:
        msg = "no Pub/Sub message received"
        logging.error(f"error: {msg}")
        return (f"Bad Request: {msg}", 400)
    elif not isinstance(envelope, dict):
        msg = "invalid Pub/Sub message format"
        logging.error(f"error: {msg}")
        return (f"Bad Request: {msg}", 400)
    elif "message" in envelope:
        event = envelope["message"]
        logging.info("Processing PUB/SUB")
        if isinstance(event, dict) and "data" in event:
            event = base64.b64decode(event["data"]).decode("utf-8")
            try:
                logging.info("Process received %s", event)
                data = json.loads(event)
                if data["download_status"] != 200:
                    logging.info(
                        "Skipping log %s downloaded at %s",
                        data["name"],
                        data["time"],
                    )
                    return ("", 204)
                process_log = processor.process(data)
            except Exception as ex:
                logging.error(ex)
    else:
        event = envelope
        logging.info("Processing POST")
        logging.info("Process received %s", event)
        try:
            data = event  # json.loads(event)
            if data["download_status"] != 200:
                logging.info(
                    "Skipping log %s downloaded at %s",
                    data["name"],
                    data["time"],
                )
                return ("", 204)
            process_log = processor.process(data)
        except Exception as ex:
            logging.error(ex)

    if process_log is None:
        process_log = {
            "parent": data,
            "time": datetime.now(timezone.utc),
            "processor_name": data["name"],
            "error": "Unknown error",
        }
    save_process_logs(process_log)
    return ("", 204)


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080
    app.run(host="127.0.0.1", port=PORT, debug=True)
