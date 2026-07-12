# Runbook: Deploy to Production

## Purpose
Deploy or update the market intelligence platform on Ubuntu Server.

## Preconditions
- [ ] Ubuntu Server 22.04+ with Docker Engine 24+ and Docker Compose v2
- [ ] All `.env` values populated (see `.env.example`)
- [ ] DNS/firewall configured for API port (default: 8000)
- [ ] Git repo cloned to `/opt/market-intelligence`
- [ ] Read `.claude/state.md` — no pending breaking changes

## Step-by-Step Execution

### Step 1: Server preparation
```bash
# Install Docker
curl -fsSL https://get.docker.com | bash
sudo usermod -aG docker $USER
newgrp docker

# Install Docker Compose v2
sudo apt-get install docker-compose-plugin

# Verify
docker compose version  # must be v2.x
```

### Step 2: Clone and configure
```bash
cd /opt
git clone <your-repo-url> market-intelligence
cd market-intelligence

# Copy and fill environment
cp .env.example .env
nano .env  # Fill ALL required keys
```

### Step 3: Required .env keys (MUST be set)
```
POSTGRES_PASSWORD=<strong_password>
OPENAI_API_KEY=sk-...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_ADMIN_CHAT_ID=...
NEWSAPI_KEY=...
POLYGON_API_KEY=...
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
```

### Step 4: Build and start services
```bash
# Build all images
docker compose build --no-cache

# Start dependencies first
docker compose up -d postgres redis
sleep 15  # wait for postgres init

# Run migrations
docker compose run --rm api python -m src.db.migrate

# Start all services
docker compose up -d

# Verify all running
docker compose ps
```

### Step 5: Verify deployment
```bash
# API health
curl http://localhost:8000/health

# Check worker started
docker compose logs worker --tail=20

# Check bot connected
docker compose logs telegram-bot --tail=10

# Send test command to bot
# (in Telegram: /market)
```

### Step 6: Set up log rotation
```bash
# Create logrotate config
sudo tee /etc/logrotate.d/market-intelligence << 'EOF'
/var/lib/docker/containers/*/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
}
EOF
```

### Step 7: Set up systemd service (optional, auto-start on reboot)
```bash
sudo tee /etc/systemd/system/market-intelligence.service << 'EOF'
[Unit]
Description=Market Intelligence Platform
After=docker.service
Requires=docker.service

[Service]
WorkingDirectory=/opt/market-intelligence
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable market-intelligence
sudo systemctl start market-intelligence
```

## Updating Production

```bash
cd /opt/market-intelligence
git pull origin main

# If code changes only (no schema change):
docker compose build api worker telegram-bot
docker compose up -d --no-deps api worker telegram-bot

# If schema changes:
docker compose run --rm api python -m src.db.migrate
docker compose up -d
```

## Rollback Strategy
```bash
git log --oneline -10  # find previous working commit
git checkout <commit-hash>
docker compose build
docker compose up -d
```

## Monitoring Commands
```bash
# Resource usage
docker stats

# Service logs
docker compose logs -f worker

# Database connections
docker compose exec postgres psql -U mktintel -c "SELECT count(*) FROM pg_stat_activity;"

# Redis memory
docker compose exec redis redis-cli info memory | grep used_memory_human
```

## Update Instructions
After deployment:
- `.claude/state.md` — update "Last Deployment" date and version
- `.claude/changelog.md` — log deployment
