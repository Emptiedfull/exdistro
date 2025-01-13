from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import requests
app = FastAPI()


serverList = ["localhost:9001","localhost:8001","localhost:7001"]

app.mount("/static",StaticFiles(directory="static"),name="static")

@app.get("/")
async def index():
    return FileResponse("./html/index.html")

@app.get("/demo")
async def demo():
    return FileResponse("./html/demo.html")

@app.get("/get_servers")
async def servers():
    active = []
    for s in serverList:
        try:
           response = requests.get("http://" + s + "/ping")
           if response.status_code == 200:
            active.append(s)
        except Exception as e:
           print(e)
           continue
    return active

@app.get("/set/{server}/{key}/{value}")
async def setReq(server:str,key:str,value:str):
    print(server,key,value)
    if server in serverList:
        try:
            response = requests.post(f'http://{server}/api/{key}/{value}')
            print(response)
            if response.status_code != 200:
                return response.text,400
            return "succes",200
        except Exception as e: 
            print(e)

            return 400
   

@app.get("/get/{server}/{key}")
async def get(server:str,key:str):
    if server in serverList:
        try:
            res = requests.get(f"http://{server}/api/{key}")
            if res.status_code != 200:
                return res.text,400
            return res.text,200
        except Exception as e:
            print(e)
            return "failure",400