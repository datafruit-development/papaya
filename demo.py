from fastapi import FastAPI, Request, Form
from typing import Optional

app = FastAPI()

# In-memory storage for demo purposes
items_db = {
    1: {"name": "Foo", "description": "There comes Foo"},
    2: {"name": "Bar", "description": "The bartenders"},
}

@app.get("/")
async def read_root():
    """
    Root endpoint that returns a simple greeting.
    """
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    """
    Endpoint to get an item by its ID.
    Accepts an optional query parameter 'q'.
    """
    item = items_db.get(item_id)
    if item:
        response = {"item_id": item_id, **item}
        if q:
            response.update({"q": q})
        return response
    return {"error": "Item not found"}

@app.post("/items/")
async def create_item(name: str, description: Optional[str]):
    """
    Endpoint to create a new item using form data.
    Requires 'name' and optionally accepts 'description'.
    """

    return "this is a test"

@app.get("/all_items")
async def get_all_items():
    """
    Endpoint to retrieve all items currently stored.
    """
    return items_db

# Example of how to run this app using uvicorn:
# uvicorn your_filename:app --reload
