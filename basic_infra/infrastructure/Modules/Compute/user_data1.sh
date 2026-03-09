#!/usr/bin/env bash

# Enable strict mode shell
# -e exit immidiately if any command fails
# -u error if any undefined var is used
# -o pipefail (fail if any command in a pipeline fails)
# This prevents silent, partially configured servers
set -euo pipefail

# Terraform evaluates templatefile() locally. Reads user_data.sh.tftpl. Replaces:
# ${app_name}, ${ecr_repo_url}, ${tv_webhook_secret}
# Produces a fully rendered bash script. Terraform sends that rendered script to AWS. It becomes the EC2 instance’s user data
# EC2 executes it automatically
# Runs once, rus as boot, runs on first boot only.
# Runs only on instance creation, if we change user_data.sh.tftpl or run terraform apply. Terraform will NOT re-run it unless:
# The EC2 instance is replaced, or
# We explicitly force it (e.g. taint / replace)
# So terraform apply will not rerun script but terraform apply -replace=aws_intance.web will

# Variables injected from Terraform templatefile(), baked into the EC2 instance at boot time
APP_NAME="${app_name}"
ECR_REPO_URL="${ecr_repo_url}"
TV_WEBHOOK_SECRET="${tv_webhook_secret}"
APCA_API_BASE_URL="${apca_api_base_url}"
APCA_API_KEY_ID="${apca_api_key_id}"
APCA_API_SECRET_KEY="${apca_api_secret_key}"


# Redis app config
# Redis server running on same machine (localhost -EC2-) on Redis port (6379). Confirm on EC2 with 
# sudo ss -lntp | grep 6379 OR redis-cli ping. Expected output: PONG
REDIS_URL="redis://127.0.0.1:6379/0"
# Limits the length of specific Redis lists e.g. tv:15m:AAPL:date, tv:15m:AAPL:signal, tv:15m:AAPL:open
TV_MAXLEN="500" 
# Custom directory to store Redis persistent data.
REDIS_DATA_DIR="/var/lib/redis-data" 

# IMAGE TAG FILE (updated by CI/CD)
# Overwrite content from standard input into /etc/trading... (cat writes to file instead of stout and reads from heredoc instead of file)
# systemd loads EnvironmentFile=/etc/trading-mage-image.env, so systemd read IMAGE_TAG=bootstrap and make it an env var
# So inside the service, $IMAGE_TAG becomes "bootstrap"
# In CI/CD pipeline, first boot -> instance has no image tag yet. So bootstrap script sets IMAGE_TAG=bootstrap. Later, GHA deploy
#updates it IMAGE_TAG=sha256abcdef. Then systemd restarts container with new image. 
# Heredoc equiv to echo "IMAGE_TAG=bootstrap" > /etc/trading-mage-image.env, but heredoc scales well when writing multiple files
cat >/etc/trading-mage-image.env <<EOF
IMAGE_TAG=bootstrap
EOF
chown root:root /etc/trading-mage-image.env # Change ownership to root from defualt ubuntu to prevent accidental editing by a regular user
chmod 600 /etc/trading-mage-image.env

# APP ENV FILE (secrets/config)
cat >/etc/trading-mage.env <<EOF
TV_WEBHOOK_SECRET=$TV_WEBHOOK_SECRET
APCA_API_BASE_URL=$APCA_API_BASE_URL
APCA_API_KEY_ID=$APCA_API_KEY_ID
APCA_API_SECRET_KEY=$APCA_API_SECRET_KEY
REDIS_URL=$REDIS_URL
TV_MAXLEN=$TV_MAXLEN
EOF

chown root:root /etc/trading-mage.env
chmod 600 /etc/trading-mage.env

# SYSTEM UPDATE + BASE PACKAGES
# Refreshes Ubuntu’s package index. Required before installing anything
apt-get update -y # y flag automatically answers "yes" to all prompts
#Installs: ca-certificates → TLS trust store, curl → download files, gnupg → verify Docker’s GPG key, nginx → reverse proxy in front of FastAPI
apt-get install -y ca-certificates curl gnupg nginx util-linux

# DOCKER
# DOCKER INSTALLATION (OFFICIAL METHOD)
# Creates a secure directory for apt signing keys. Required by modern Docker install instructions
install -m 0755 -d /etc/apt/keyrings
# Downloads Docker’s official GPG key. Converts it to binary format for apt
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# Allows apt to read the key
chmod a+r /etc/apt/keyrings/docker.gpg

# Adds Docker’s official repository to apt. Automatically matches Ubuntu version (Jammy 22.04)
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
	> /etc/apt/sources.list.d/docker.list

apt-get update -y
# Docker engine, Docker CLI, Container runtime, Buildx plugin (useful later)
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin

#Add ECR cred helper so no awscli is needed
# ECR DOCKER CREDENTIAL HELPER (so we don't need awscli on the host)
apt-get install -y amazon-ecr-credential-helper

# Configure Docker (root) to use ecr-login for your registry
REGISTRY_HOST="$(echo "$ECR_REPO_URL" | cut -d/ -f1)"  # e.g. 0807...dkr.ecr.us-west-1.amazonaws.com

mkdir -p /root/.docker
cat >/root/.docker/config.json <<JSON
{
  "credHelpers": {
    "$REGISTRY_HOST": "ecr-login"
  }
}
JSON
chmod 600 /root/.docker/config.json

#Starts Docker immediately, Enables it on every reboot
systemctl enable --now docker


# REDIS EBS DATA DISK SETUP
###############
# Assumes a separate EBS vol is attached for REDIS data. 
# We look for a non-root whole disk that is currently unmounted
mkdir -p "$REDIS_DATA_DIR" # -p flag creates the entire path if if doesn't already exist

# On EC2, we often have multiple disks e.g /dev/nvme0n1 (root disk), /dev/nvme1n1 (extra EBS volume)
# The root filesystem is usually a partition: /dev/nvme0n1p1
# But when scanning disks you want the disk, not the partition. So the segment below converts /dev/nvme0n1p1 to /dev/nvme0n1.
# In automation, this ensures the script never formats the root disk.
# So, we are going to detect the root filesystem partition, find the disk containig that partition, and store the disk path so the script can avoid modifying it.
# Detect which device is mounted as the root filesystem (/). Takes the root partition (like /dev/nvme0n1p1)
# and converts it into the parent disk device (like /dev/nvme0n1).
# Infrastructure scripts often need to know which disk is the root disk.
# Because when attaching additional disks (like EBS volumes), we must avoid accidentally formatting the root disk.
# Example scenario:
# /dev/nvme0n1p1   root filesystem
# /dev/nvme1n1     new EBS volume
ROOT_SOURCE="$(findmnt -n -o SOURCE / || true)" # Run the command inside the parentheses and store its output in the variable ROOT_SOURCE.
ROOT_DISK="" # Initialized as an empty variable. The script will populate it later if it successfully detects the root disk.
if [ -n "$ROOT_SOURCE" ]; then # Checks if root source exists ie ROOT_SOURCE is not an empty string.
	ROOT_PARENT="$(lsblk -no PKNAME "$ROOT_SOURCE" 2>/dev/null || true)" # Determines the parent disk. lsblk lists block devices.
	if [ -n "$ROOT_PARENT" ]; then # Checks if parent disk was found ie is non empty e.g ROOT_PARENT=nvme0n1
		ROOT_DISK="/dev/$ROOT_PARENT" # Construct the full disk path e.g ROOT_DISK=/dev/nvme0n1.
	fi
fi

# The script will now look for an extra block device (like an attached EBS volume) that is not the root disk and not currently mounted, 
# so it can use that disk as persistent storage for Redis data.
# Starts by Waiting for an additional EBS volume to appear on the EC2 instance.
# Identifies that disk safely, without touching the root disk.
# Formats it if needed.
# Mount it at our Redis data directory /var/lib/redis-data.
# Adds it to /etc/fstab so it remounts on reboot.
# Set ownership so the Redis container can write to it.
REDIS_DEV=""
for i in $(seq 1 30); do # Try 30 times to find the extra disk. Attached EBS vosl do not always appear instantly at boot.
	REDIS_DEV="$(
		lsblk -dpno NAME,TYPE,MOUNTPOINT | awk '
			$2 == "disk" && $3 == "" { print $1 }
		' | grep -vx "$${ROOT_DISK:-/dev/null}" | head -n 1 || true
	)"

	if [ -n "$REDIS_DEV" ]; then
		echo "Found Redis EBS device: $REDIS_DEV"
		break
	fi

	echo "Waiting for Redis EBS volume to appear... ($i/30)"
	sleep 2 # Wait 2 seconds after every failed attempt. 
done 

if [ -z "$REDIS_DEV" ]; then
	echo "ERROR: Redis EBS volume not found"
	lsblk
	exit 1
fi

# Format only if it doesn't already have a filesystem
if ! blkid "$REDIS_DEV" >/dev/null 2>&1; then # If blkid fails, then the device likely has no filesystem. So, format it.
	echo "Formatting Redis device $REDIS_DEV as ext4"
	mkfs.ext4 -F "$REDIS_DEV" 
else
	echo "Redis device $REDIS_DEV already has a filesystem"
fi

# UUIDs are preferred over raw device names because device names can change across reboots.
UUID="$(blkid -s UUID -o value "$REDIS_DEV")"
if [ -z "$UUID" ]; then
	echo "ERROR: Could not determine UUID for $REDIS_DEV"
	exit 1
fi

if ! grep -q "$UUID" /etc/fstab; then # If UUID is determined, persist the mount in /etc/fstab entry (add one if it doesn't already exist).
	# nofail means if the disk is missing at boot, don’t fail the whole boot process
	echo "UUID=$UUID $REDIS_DATA_DIR ext4 defaults,nofail 0 2" >> /etc/fstab
fi

mount -a # Mount everything from fstab

# Redis container runs as a uid 999 in the official image
# Change owner and group recursively to UID 999, GID 999. That matches the Redis process inside the official container image.
chown -R 999:999 "$REDIS_DATA_DIR"
chmod 750 "$REDIS_DATA_DIR"


# NGINX REVERSE PROXY CONFIGURATION
# Creates an Nginx config file for app and writes it to standard Nginx site config dir. Uses app_name as filename (tv-webhook). 
# Cat reads from stdin and writes (overwrites) to the fielpath. The heredoc supplies that input
# Listens on port 80 for HTTP traffic. Accepts traffic for any hostname (server_name _;) This is a common pattern when we only have one service on the machine.
# So all the following requests will match: http://EC2_PUBLIC_IP, http://example.com, http://anything.
# Forwards incoming webhook requests to FastAPI listening at port 8000. FastAPI is not exposed publicly. Nginx is the only public entry point.
# Health endpoint location /health {return 200 "ok\n";}: This defines a health check endpoint. When someone requests: http://server/health Nginx responds directly: HTTP 200 ok
# This bypasses our application. This is useful for: load balancer health checks, monitoring systems, uptime checks. Example test: curl http://localhost/health Response: ok
# Webhook endpoint: location /webhook/tradingview { This defines a route handled by Nginx. Requests like: POST /webhook/tradingview will match this rule. This is exactly the endpoint your TradingView webhook sends alerts to.
# Reverse proxy to our FastAPI app: proxy_pass http://127.0.0.1:8000; This forwards the request to your FastAPI application. Our architecture looks like this:
# Internet -> Nginx (port 80) -> FastAPI container (port 8000) So the request flow becomes: POST /webhook/tradingview -> Nginx receives request -> Nginx forwards request ->FastAPI on localhost:8000
# Forwarding important headers: These lines preserve information about the original request. Host header proxy_set_header Host $host; Passes the original hostname.
# Example: Host: example.com Client IP proxy_set_header X-Real-IP $remote_addr; This forwards the actual client IP address. Without this, FastAPI would think the request came from: 127.0.0.1
# because Nginx is forwarding it. Proxy chain: proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for; This records the full chain of proxies.
# Example header: X-Forwarded-For: 1.2.3.4 Our app can read this to identify the real client. Protocol proxy_set_header X-Forwarded-Proto $scheme;
# This tells the backend whether the original request was: http or https Even though the backend always receives HTTP from Nginx. 
# End of server block} Closes the server configuration.
# Resulting architecture: Our deployed system looks like this: TradingView -> HTTP POST -> EC2 port 80 -> Nginx reverse proxy -> 127.0.0.1:8000 -> Docker container -> FastAPI app
# This design is very common because Nginx: handles public HTTP traffic, shields the application, allows TLS later, manages routing and buffering
cat >/etc/nginx/sites-available/"$APP_NAME" <<'NGINX' #NGINX quoted so as to not expand shell vars inside the block since Nginx uses vars like $host,$remote_addr,$scheme. Without it, Bash would try to substitute them.
server {
	listen 80;
	server_name _;

	location /health {
		proxy_pass http://127.0.0.1:8000;
		proxy_set_header Host $host;
		proxy_set_header X-Real-IP $remote_addr;
		proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
		proxy_set_header X-Forwarded-Proto $scheme;
	}

	location /webhook/tradingview {
		proxy_pass http://127.0.0.1:8000;
		proxy_set_header Host $host;
		proxy_set_header X-Real-IP $remote_addr;
		proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
		proxy_set_header X-Forwarded-Proto $scheme;
	}
}
NGINX

# ENABLE THE NGINX SITE
rm -f /etc/nginx/sites-enabled/default # Removes Ubuntu’s default site
ln -sf /etc/nginx/sites-available/"$APP_NAME" /etc/nginx/sites-enabled/"$APP_NAME" # Enables our site via symlink
nginx -t # Validates nginx config. Script will fail if config is invalid (good)
systemctl restart nginx # Restarts nginx with your config
systemctl enable nginx # Enables nginx on reboot

# CREATE A SYSTEMD SERVICE FOR REDIS and FASTAPI CONTAINER (CI/CD WILL PULL + RESTART)
# systemd is the init system and service manager used by most modern Linux distributions (including Ubuntu, which our EC2 instance runs).
# Its job is to start, stop, and supervise system services. Think of it as the operating system’s process orchestrator. It's the process with PID = 1
# systemd manages services, which are long-running processes. nginx.service(web server), docker.service(container runtime), redis-server.service(Redis dB), trading-mage-app.service(our app)  


# REDIS SYSTEMD SERVICE
# This snippet creates a systemd service that runs Redis inside a Docker container and ensures it starts automatically, 
# restarts on failure, and stores its data on disk. Just like the Nginx example above, the first line writes a file using a heredoc.
# The [Unit] section tells systemd when and how this service should start relative to other services.
# Description: A human-readable label. You’ll see it when running: systemctl status redis
# After=network-online.target docker.service: This tells systemd: Do not start Redis until the network and Docker are ready. 
# Startup order becomes: network -> docker -> redis container
# Wants=network-online.target. This tells systemd: Try to bring the network online when starting this service. But it’s not strictly required.
# Requires=docker.service: This means: If Docker fails, Redis should also fail. It also ensures Docker starts before Redis.
# [Service] section — how the service runs [Service] Restart=always RestartSec=2 Restart=always. If the container crashes, systemd will restart it automatically.
# Example: Redis crashes -> systemd waits -> systemd starts container again. RestartSec=2: Wait 2 seconds before restarting. This prevents rapid restart loops.
# Cleaning up any old container: ExecStartPre=-/usr/bin/docker rm -f redis This command runs before the main start command. It removes any existing container named redis.
# The - at the beginning means: Ignore errors if the command fails. Example: docker rm -f redis. If the container does not exist, the service will still start normally.
# Starting the Redis container: ExecStart=/usr/bin/docker ... --dir /data
# This is the core of the service: systemd will execute this command to start Redis. Container name --name redis. The container will be called: redis
# This makes it easy to manage:
# docker logs redis ... 127.0.0.1:6379:6379 This maps Redis’s port to the host. Meaning: host:127.0.0.1:6379 → container:6379 Important detail:
# Because the host address is 127.0.0.1, Redis is not accessible from the internet. Only local services (like your FastAPI app) can connect. This is a good security practice.
# Persistent storage: -v /var/lib/redis-data:/data. This mounts a directory from the host into the container. Mapping:
# Host directory:      /var/lib/redis-data
# Container directory: /data
# Redis will write data to /data, which actually lives on the EC2 host. This ensures Redis data persists even if the container is destroyed.
# Redis image
# redis:7-alpine
# This pulls the official Redis Docker image: Redis version 7. Alpine Linux base (very small image). Redis server command redis-server --appendonly yes --dir /data
# This launches Redis inside the container. Two important options: --appendonly yes: Enables AOF persistence.
# Redis writes every operation to a log file. Benefits: data survives restarts, more durable than memory-only mode
# --dir /data: Sets Redis’s working directory. Because /data is mounted to /var/lib/redis-data, Redis writes its files to the host disk. Typical files created:
# appendonly.aof
# dump.rdb
# Stopping the container: ExecStop=/usr/bin/docker stop redis When the service stops, systemd runs: docker stop redis
# This gracefully shuts down Redis.
# Cleanup after stopping ExecStopPost=-/usr/bin/docker rm -f redis. After stopping, systemd removes the container. The - again means: Ignore errors if the container is already gone.
# This ensures the next start always creates a fresh container.
# [Install] section — startup behavior [Install] WantedBy=multi-user.target. This determines when the service runs. multi-user.target is the standard Linux normal system state (non-graphical server mode).
# Enabling the service will make it start automatically at boot. Example: systemctl enable redis. Boot flow becomes: system boots -> systemd starts services -> redis.service starts -> docker container launches
# Resulting architecture: Your Redis stack becomes: EC2 instance->systemd->redis.service->docker run redis->Redis container->/var/lib/redis-data (host disk)
# So Redis data lives on the host, while Redis itself runs inside a container.
#How you would interact with it? Check status: systemctl status redis  View logs: journalctl -u redis -f  See container logs: docker logs redis  Stop service:systemctl stop redis
cat >/etc/systemd/system/redis.service <<'SYSTEMD'
[Unit]
Description=Redis container
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
Restart=always
RestartSec=2

ExecStartPre=-/usr/bin/docker rm -f redis
ExecStart=/usr/bin/docker run --name redis \
	-p 127.0.0.1:6379:6379 \
	-v /var/lib/redis-data:/data \
	redis:7-alpine \
	redis-server --appendonly yes --dir /data
ExecStop=/usr/bin/docker stop redis
ExecStopPost=-/usr/bin/docker rm -f redis

[Install]
WantedBy=multi-user.target
SYSTEMD


# APP SYSTEMD SERVICE
# This segment creates a systemd unit file. Ensures Docker is running before starting the app. Automatically restarts if the container crashes.
# Deletes any old container with the same name. - prefix means “ignore errors”
# Runs our container: Binds FastAPI only to localhost. Injects tv_webhook_secret. Uses latest image from ECR
# Public traffic can only reach it via nginx
# Clean shutdown on stop/restart
# Enables this service at normal system boot
cat >/etc/systemd/system/$APP_NAME.service <<SYSTEMD
[Unit]
Description=$APP_NAME container
After=network-online.target docker.service redis.service
Wants=network-online.target
Requires=docker.service redis.service

[Service]
Restart=always
RestartSec=2

Environment="ECR_REPO_URL=${ecr_repo_url}"
EnvironmentFile=/etc/trading-mage-image.env

ExecStartPre=-/usr/bin/docker rm -f %p
ExecStart=/usr/bin/docker run --name %p --env-file /etc/trading-mage.env -p 127.0.0.1:8000:8000 \$${ECR_REPO_URL}:\$${IMAGE_TAG} python -m uvicorn app:app --host 0.0.0.0 --port 8000
ExecStop=/usr/bin/docker stop %p
ExecStopPost=-/usr/bin/docker rm -f %p

[Install]
WantedBy=multi-user.target
SYSTEMD

#Tests
test -s "/etc/systemd/system/redis.service"
test -s "/etc/systemd/system/$APP_NAME.service"
grep -c '^ExecStart=' "/etc/systemd/system/redis.service" | grep -q '^1$'
grep -c '^ExecStart=' "/etc/systemd/system/$APP_NAME.service" | grep -q '^1$'

# ACTIVATE SYSTEMD SERVICE
systemctl daemon-reload # Reloads systemd to recognize the new service
systemctl enable --now redis.service # Enables the service on boot
systemctl enable --now $APP_NAME.service # Enables the service on boot
# Don't start yet (no image pulled yet). CI/CD will do it after it pushes the first image.


# Key architectural takeaways
# Nginx is the public edge (port 80)
# FastAPI is private (127.0.0.1:8000)
# systemd keeps the container alive
# CI/CD controls when new code is deployed
# Secrets are injected securely via env vars
# No SSH required in production later
