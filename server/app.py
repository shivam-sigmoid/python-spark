import flask

import sys

sys.path.append("../")

app = flask.Flask(__name__)


@app.route("/")
def home():
    return "Hello World"


if __name__ == "__main__":
    app.run(debug=True, port=5005)
