FROM python:3.13-slim

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "main.py"]
```

**Step 2 — Create `requirements.txt`** in the same folder:
```
python-telegram-bot[webhooks]
yt-dlp
```

**Step 3 — Your folder should look like this:**
```
📁 your-project/
├── main.py
├── Dockerfile
├── requirements.txt
└── cookies.txt  (optional)