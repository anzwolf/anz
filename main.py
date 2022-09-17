import pandas as pd
from pymongo import MongoClient
import pandas as pd
import requests
import json
import datetime
import time
now = datetime.datetime.now()
client = MongoClient('mongodb+srv://anoze:mongo@cluster0.9df5ax2.mongodb.net/test')
mydb = client["oidata"]

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36",
            "Referer": "https://www.nseindia.com",
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Sec-Fetch-User': '?1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',}

def fetch(index):
    r_bytes = requests.get('https://www.nseindia.com/api/option-chain-indices?symbol='+index,headers=headers, verify=True,timeout=(5, 14)).content
    my_json = r_bytes.decode('utf8').replace("'", '"')
    #r = json.dumps(json.loads(my_json), indent=4, sort_keys=True)
    return my_json

def saveDataInMongo(index='BANKNIFTY' ):
    data = fetch(index)
    dfdata = pd.json_normalize(json.loads(data)["records"]["data"])
    dfdata = dfdata.sort_values('expiryDate',ascending=False)
    expDates = json.loads(data)['records']['expiryDates']
    print(expDates)

    df = dfdata[['strikePrice' ,  'expiryDate',
                'PE.openInterest','CE.openInterest',
                'CE.changeinOpenInterest','CE.pchangeinOpenInterest',
                'CE.totalTradedVolume', 'PE.totalTradedVolume',
                'CE.underlyingValue'
                ]] 
    
    strprice = df["CE.underlyingValue"].values
    df['CE.underlyingValue'] = df['CE.underlyingValue'].fillna(0)
    if len(strprice)>0:
        k = df["CE.underlyingValue"].values
        strprice = max(list(k))
    else:
        k = df["CE.underlyingValue"].unique()

        try:
            strprice = max(list(k))
        except:
            strprice = 36000

    df = df[(df["strikePrice"] >= int(strprice - strprice % 100) - 800) & (
                    df["strikePrice"] <= int(strprice - strprice % 100) + 800)]
                    
    for exp in expDates:
        df = df[df['expiryDate']==exp]
        df = df.rename(columns = {'PE.openInterest' : "PEOI",
                                'CE.openInterest' : "CEOI",
                                'CE.changeinOpenInterest' : 'CEchOI',
                                'PE.changeinOpenInterest': 'PEchOI',
                                'PE.pchangeinOpenInterest':'PEpchoi',
                                'CE.pchangeinOpenInterest': 'CEpchoi',
                                'CE.totalTradedVolume': 'CEVol',
                                'PE.totalTradedVolume' : 'PEVol',
                                'CE.underlyingValue' : 'price',
                                'strikePrice': 'strike' })
        df['time'] = datetime.datetime.now()
        df = df.sort_values('strike',ascending=False)
        print(df)
        if index == "BANKNIFTY":
            mycol = mydb["BNF"]
            #mongodb+srv://anoze:@cluster0.9df5ax2.mongodb.net/test
            #df = pd.DataFrame.from_dict({'A': {1: datetime.datetime.now()}})
            #{'_id': 'my_user_id', 'value': 'my_value'
            mycol.insert_one( {'_id': datetime.datetime.now(),'exp':exp,'value': df.to_dict('records')})
        if index == "NIFTY":
            mycol = mydb["NIFTY"]
            #mongodb+srv://anoze:@cluster0.9df5ax2.mongodb.net/test
            #df = pd.DataFrame.from_dict({'A': {1: datetime.datetime.now()}})
            #{'_id': 'my_user_id', 'value': 'my_value'
            mycol.insert_one( {'_id': datetime.datetime.now(),'exp': exp ,'value': df.to_dict('records')})
        break


start_time = int(9)*60 + int(15)
end_time = int(15)*60 + int(35)
current_time = now.hour*60 + now.minute
while True:
    if start_time <= current_time and end_time >= current_time:
        try:
            saveDataInMongo(index='NIFTY')
        except:
            pass
        try :
            saveDataInMongo(index='BANKNIFTY')
        except:
            pass
        time.sleep(180)
    else:
        time.sleep(1800)
        