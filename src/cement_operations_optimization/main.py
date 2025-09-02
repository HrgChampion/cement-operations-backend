from fastapi import FastAPI

app = FastAPI(title="Cement Operations Optimization API")



@app.get("/")
def root():
    return {"message": "Cement Operations Optimization backend is running!"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)