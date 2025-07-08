from threading import Thread
from flask import Flask, jsonify

from daemon import Daemon
from utils import logger

app = Flask(__name__)
daemon = None

@app.route("/")
def home():
    return "Please use the /daemon endpoint to start the daemon, see readme for more info"

@app.route("/daemon/start", methods=["GET"])
def daemon_start():
    """Start the Amber scheduler if not already running"""
    global daemon
    
    if daemon is not None:
        return jsonify(status="info", 
                     message="Daemon is already running"), 200
    try:
        daemon = Daemon()
        thread = Thread(target=daemon.start)
        thread.daemon = True
        thread.start()
        return jsonify(status="success", 
                      message="Daemon started successfully"), 200
                      
    except Exception as e:
        daemon = None  # Reset on failure
        return jsonify(status="error", 
                      message=f"Failed to start daemon: {str(e)}"), 500
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5006, debug=False)