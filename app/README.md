# developer reference


### getting the api keys setup

make sure to actually edit the .env file with the relevant keys
```
cd app
cp .env.example .env
vim .env
```

### running the backend
```
cd app/backend
uv sync
source .venv/bin/activate
uvicorn main:app --reload
```

### running the frontend
```
cd app/frontend
npm i
npm run dev
```
