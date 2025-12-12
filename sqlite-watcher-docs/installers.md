# sqlite-watcher installation guide

This document walks through running the sqlite-watcher service on Linux, macOS, and Windows. The watcher process should run beside the `.sqlite` file so it can tail the WAL and expose change batches over the embedded gRPC API.

All platforms share these basics:

- Create a token file (default `~/.seren/sqlite-watcher/token`) with restrictive permissions (owner read/write only).
- Choose a queue database path (default `~/.seren/sqlite-watcher/changes.db`). Ensure the parent directory is `0700` on Unix.
- Run `sqlite-watcher serve --queue-db <path> --listen <endpoint> --token-file <file>` to start the gRPC service. Endpoints use the `unix:/path` or `tcp:host:port` syntax.

## Linux (systemd)

1. Install binaries:

   ```bash
   sudo install -m 0755 database-replicator /usr/local/bin/database-replicator
   sudo install -m 0755 sqlite-watcher /usr/local/bin/sqlite-watcher
   ```

2. Create token + queue directories:

   ```bash
   install -d -m 0700 ~/.seren/sqlite-watcher
   openssl rand -hex 32 > ~/.seren/sqlite-watcher/token
   ```

3. Create `/etc/systemd/system/sqlite-watcher.service`:

   ```ini
   [Unit]
   Description=sqlite-watcher for /srv/app.db
   After=network-online.target

   [Service]
   User=replicator
   ExecStart=/usr/local/bin/sqlite-watcher serve \
     --queue-db /var/lib/sqlite-watcher/changes.db \
     --listen unix:/run/sqlite-watcher.sock \
     --token-file /home/replicator/.seren/sqlite-watcher/token
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```

4. Enable/start:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now sqlite-watcher.service
   ```

## macOS (launchd)

1. Copy binaries into `/usr/local/bin`.
2. Save the following to `~/Library/LaunchAgents/com.seren.sqlite-watcher.plist`:

   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
   <plist version="1.0">
   <dict>
     <key>Label</key>
     <string>com.seren.sqlite-watcher</string>
     <key>ProgramArguments</key>
     <array>
       <string>/usr/local/bin/sqlite-watcher</string>
       <string>serve</string>
       <string>--queue-db</string>
       <string>/Users/you/.seren/sqlite-watcher/changes.db</string>
       <string>--listen</string>
       <string>unix:/Users/you/.seren/sqlite-watcher/watcher.sock</string>
       <string>--token-file</string>
       <string>/Users/you/.seren/sqlite-watcher/token</string>
     </array>
     <key>RunAtLoad</key>
     <true/>
     <key>KeepAlive</key>
     <true/>
     <key>StandardOutPath</key>
     <string>/Users/you/Library/Logs/sqlite-watcher.log</string>
     <key>StandardErrorPath</key>
     <string>/Users/you/Library/Logs/sqlite-watcher.log</string>
   </dict>
   </plist>
   ```

3. Load the agent: `launchctl load ~/Library/LaunchAgents/com.seren.sqlite-watcher.plist`.

## Windows (Service)

1. Copy `database-replicator.exe` and `sqlite-watcher.exe` to a directory on `%PATH%` (e.g. `C:\Program Files\Seren`).
2. Create a token file under `%USERPROFILE%\.seren\sqlite-watcher\token`.
3. Use the built-in `sc.exe` to install a service (or NSSM if you prefer a GUI):

   ```powershell
   sc.exe create sqlite-watcher binPath= "C:\Program Files\Seren\sqlite-watcher.exe serve --queue-db C:\data\sqlite-watcher\changes.db --listen tcp:127.0.0.1:6000 --token-file %USERPROFILE%\.seren\sqlite-watcher\token" start= auto
   ```

4. Start the service with `sc.exe start sqlite-watcher`.

Remember to open the firewall only if the watcher must accept remote TCP connections. In most deployments, keep it bound to loopback or Unix sockets.

## Running sync-sqlite on a schedule

- Linux/macOS: use cron or systemd timers to run `database-replicator sync-sqlite ...` periodically.
- Windows: create a Scheduled Task pointing at `database-replicator.exe sync-sqlite ...`.

Consult the smoke test (`scripts/test-sqlite-delta.sh`) to see a minimal end-to-end example.
