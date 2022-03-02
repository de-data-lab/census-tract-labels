from flask import Flask, Response, request
import json
from urllib.parse import urlparse
from census_tract_labeling import censusTractLabel

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/')
def home():
    current_url = urlparse(request.base_url)
    entry_point = current_url.hostname + '/tracts'

    return 'Use the API entry point ' + entry_point + ' to query'

@app.route('/tracts', methods = ['GET'])
def get_data():
    address = request.args.get('address') if request.args.get('address') else None
    try:        
        labelled_tract = censusTractLabel([address])
        results = json.dumps(labelled_tract.census_tract_addresses())
    except:
        results = json.dumps("No match found")

    return Response(results, mimetype='application/json')

if (__name__ == "__main__"):
     app.run(host = '0.0.0.0', port = 8000)
