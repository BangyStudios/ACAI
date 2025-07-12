import logging
from threading import Thread
from flask import Flask, jsonify

from daemon import Daemon
from utils import logger

log = logging.getLogger("logger")

def create_app():
    app = Flask(__name__)
    daemon = {"instance": None}  # Use a mutable container to hold the daemon instance

    @app.route("/")
    def home():
        return "Please use the /daemon endpoint to start the daemon, see readme for more info"

    @app.route("/daemon/start", methods=["GET"])
    def daemon_start():
        """Start the Amber scheduler if not already running"""
        if daemon["instance"] is not None:
            return jsonify(status="info", 
                           message="Daemon is already running"), 200
        try:
            daemon["instance"] = Daemon()
            thread = Thread(target=daemon["instance"].start)
            thread.daemon = True
            thread.start()
            return jsonify(status="success", 
                           message="Daemon started successfully"), 200
        except Exception as e:
            daemon["instance"] = None  # Reset on failure
            return jsonify(status="error", 
                           message=f"Failed to start daemon: {str(e)}"), 500

    # Start daemon by default
    try:
        daemon["instance"] = Daemon()
        thread = Thread(target=daemon["instance"].start)
        thread.daemon = True
        thread.start()
        log.info("Daemon started on app startup")
    except Exception as e:
        daemon["instance"] = None
        log.error(f"Daemon failed to start on app startup: {str(e)}")

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5006, debug=False)
