import pymongo
import pandas as pd
from flask import Flask , render_template, session, redirect

app = Flask(__name__)


client = pymongo.MongoClient('mongodb+srv://anoze:mongo@cluster0.9df5ax2.mongodb.net/test')
mydb = client["oidata"]
col = mydb["NIFTY"]
def retriveData():
    res = col.find_one( sort=[( '_id', pymongo.DESCENDING )])
    return pd.DataFrame(res['value'])

@app.route('/')
def home():
    try:
        df = retriveData()
    except:
        df = pd.DataFrame()
    return render_template('home.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)