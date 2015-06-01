import ujson as json

from flask import Flask, request, abort
from db import create_connection

app = Flask(__name__)
db = create_connection(host="localhost")

@app.route('/channel/')
def get_channels():
    try:
        cursor = db.cursor()
        cursor.execute("""select * from list_channels()""")
        channels = cursor.fetchall()
        return json.dumps([channel[0] for channel in channels])
    except:
        abort(404)

@app.route('/channel/<channel>/buildid')
def get_buildids(channel):
    try:
        cursor = db.cursor()
        cursor.execute("select * from list_buildids(%s)", (channel, ))
        result = cursor.fetchall()
        pretty_result = map(lambda r: {"version": r[0], "buildid": r[1]}, result)
        return json.dumps(pretty_result)
    except:
        abort(404)

@app.route('/channel/<channel>/buildid/<version>_<buildid>', methods=["GET"])
def get_buildid(channel, version, buildid):
    try:
        dimensions = request.args.get("dimensions", "{}")
        cursor = db.cursor()
        cursor.execute("select * from get_buildid_metric(%s, %s, %s, %s)", (channel, version, buildid, dimensions))
        result = cursor.fetchall()
        pretty_result = map(lambda r: {"label": r[0], "histogram": r[1]}, result)
        return json.dumps(pretty_result)
    except:
        abort(404)

if __name__ == "__main__":
    app.run(debug=True)
