from flask import Flask, render_template_string, send_from_directory
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(os.path.expanduser("~/.env"))

app = Flask(__name__)

# Serve individual visual HTML files (from visual1 to visual7)
@app.route("/visual<int:n>")
def serve_visual(n):
    mapbox_token = os.getenv("MAPBOX_TOKEN")
    filename = f"visual{n}.html"
    
    if not os.path.exists(filename):
        return f"{filename} not found", 404
    
    with open(filename, "r") as f:
        html_content = f.read()
    
    html_content = html_content.replace("{{ MAPBOX_TOKEN }}", mapbox_token)
    return render_template_string(html_content)

# Serve GeoJSON and other static files from /data/
@app.route("/data/<path:path>")
def serve_data(path):
    return send_from_directory("data", path)

#  Index page listing all visuals
@app.route("/")
def index():
    return """
    <h2>Visualizations</h2>
    <ul>
        <li><a href="/visual1">Visual 1</a></li>
        <li><a href="/visual2">Visual 2</a></li>
        <li><a href="/visual3">Visual 3</a></li>
        <li><a href="/visual4">Visual 4</a></li>
        <li><a href="/visual5">Visual 5.1</a></li>
        <li><a href="/visual6">Visual 5.2</a></li>
        <li><a href="/visual7">Visual 5.3</a></li>
    </ul>
    """

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
