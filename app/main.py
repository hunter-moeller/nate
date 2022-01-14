from fastapi import FastAPI
from mangum import Mangum

from free_agents import router as free_agents_router

app = FastAPI(title='Serverless Lambda FastAPI')

app.include_router(free_agents_router, prefix="/free-agents")


@app.get("/")
def root():
    return {"message": "Nate the great lives!"}


# to make it work with Amazon Lambda, we create a handler object
handler = Mangum(app=app)
