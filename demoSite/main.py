from fastapi import FastAPI
from fastapi.responses import FileResponse
app = FastAPI()

@app.get("/")
async def index():
    return FileResponse("./html/index.html")