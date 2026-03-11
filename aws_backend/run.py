import uvicorn

if __name__ == "__main__":
    
    uvicorn.run(
        "main:app",       # main.py → app
        host="127.0.0.1",
        port=8000,
        reload=True,       # Auto reload on code change
        reload_dirs=['.']
    )
# uvicorn main:app --reload --reload-dir .