# pyMC_repeater

Repeater Daemon in Python using the `pymc_core` Lib.

---

I started **pyMC_core** as a way to really get under the skin of **MeshCore** — to see how it ticked and why it behaved the way it did.
After a few late nights of tinkering, testing, and head-scratching, I shared what I’d learned with the community.
The response was honestly overwhelming — loads of encouragement, great feedback, and a few people asking if I could spin it into a lightweight **repeater daemon** that would run happily on low-power, Pi-class hardware.

That challenge shaped much of what followed:
- I went with a lightweight HTTP server (**CherryPy**) instead of a full-fat framework.
- I stuck with simple polling over WebSockets — it’s more reliable, has fewer dependencies, and is far less resource hungry.
- I kept the architecture focused on being **clear, modular, and hackable** rather than chasing performance numbers.

There’s still plenty of room for this project to grow and improve — but you’ve got to start somewhere!
My hope is that **pyMC_repeater** serves as a solid, approachable foundation that others can learn from, build on, and maybe even have a bit of fun with along the way.

> **I’d love to see these repeaters out in the wild — actually running in real networks and production setups.**
> My own testing so far has been in a very synthetic environment with little to no other users in my area,
> so feedback from real-world deployments would be incredibly valuable!

---

## Overview

The repeater daemon runs continuously as a background process, forwarding LoRa packets using `pymc_core`'s Dispatcher and packet routing.

Supported Hardware (Out of the Box)

The following hardware is currently supported out-of-the-box:

Waveshare LoRaWAN/GNSS HAT

    Hardware: Waveshare SX1262 LoRa HAT
    Platform: Raspberry Pi (or compatible single-board computer)
    Frequency: 868MHz (EU) or 915MHz (US)
    TX Power: Up to 22dBm
    SPI Bus: SPI0
    GPIO Pins: CS=21, Reset=18, Busy=20, IRQ=16

HackerGadgets uConsole

    Hardware: uConsole RTL-SDR/LoRa/GPS/RTC/USB Hub
    Platform: Clockwork uConsole (Raspberry Pi CM4/CM5)
    Frequency: 433/915MHz (configurable)
    TX Power: Up to 22dBm
    SPI Bus: SPI1
    GPIO Pins: CS=-1, Reset=25, Busy=24, IRQ=26
    Additional Setup: Requires SPI1 overlay and GPS/RTC configuration (see uConsole setup guide)

Frequency Labs meshadv-mini

    Hardware: FrequencyLabs meshadv-mini Hat
    Platform: Raspberry Pi (or compatible single-board computer)
    Frequency: 868MHz (EU) or 915MHz (US)
    TX Power: Up to 22dBm
    SPI Bus: SPI0
    GPIO Pins: CS=8, Reset=24, Busy=20, IRQ=16

Frequency Labs meshadv

    Hardware: FrequencyLabs meshadv-mini Hat
    Platform: Raspberry Pi (or compatible single-board computer)
    Frequency: 868MHz (EU) or 915MHz (US)
    TX Power: Up to 22dBm
    SPI Bus: SPI0
    GPIO Pins: CS=21, Reset=18, Busy=20, IRQ=16, TXEN=13, RXEN=12, use_dio3_tcxo=True
...

## Screenshots

### Dashboard
![Dashboard](docs/dashboard.png)
*Real-time monitoring dashboard showing packet statistics, neighbor discovery, and system status*

### Statistics
![Statistics](docs/stats.png)
*statistics and performance metrics*

## Installation

Before You Begin

Make sure SPI is switched on using raspi-config:

```bash 
sudo raspi-config
```

1. Go to Interface Options
2. Select SPI
3. Choose Enable
4. Reboot when prompted:

```bash
sudo reboot
```

After reboot, you can confirm SPI is active:
```bash 
ls /dev/spi*
```

You should see something like:
```bash 
/dev/spidev0.0  /dev/spidev0.1
```

**Clone the Repository:**
```bash
git clone https://github.com/rightup/pyMC_Repeater.git
cd pyMC_Repeater
```

**Quick Install:**
```bash
sudo bash manage.sh
```

This script will:
- Create a dedicated `repeater` service user with hardware access
- Install files to `/opt/pymc_repeater`
- Create configuration directory at `/etc/pymc_repeater`
- Setup log directory at `/var/log/pymc_repeater`
- **Launch interactive radio & hardware configuration wizard**
- Install and enable systemd service

**After Installation:**
```bash
# View live logs
sudo journalctl -u pymc-repeater -f

# Access web dashboard
http://<repeater-ip>:8000
```

**Development Install:**
```bash
pip install -e .
```

## Configuration

The configuration file is created and configured during installation at:
```
/etc/pymc_repeater/config.yaml
```

To reconfigure radio and hardware settings after installation, run:
```bash
sudo bash setup-radio-config.sh /etc/pymc_repeater or sudo bash manage.sh
sudo systemctl restart pymc-repeater

```

### MQTT Integration

**Configuration Example:**

Edit `/etc/pymc_repeater/config.yaml` and add/modify the MQTT section:

```yaml
mqtt:
  # Enable/disable MQTT publishing
  enabled: true

  # MQTT broker settings
  server: mqtt.example.com
  port: 1883
  transport: "tcp"          # switch to "websockets" for brokers like LetsMesh
  websocket_path: "/mqtt"    # optional, only used for WebSocket transports
  websocket_headers:
    X-Forwarded-For: "mesh-repeater"

  # Authentication (leave empty for no authentication)
  username: "your_username"
  password: "your_password"

  # TLS/SSL settings
  use_tls: false
  tls_verify: true

  # Topic templates
  # Supported variables: {NODE_NAME}, {PUBLIC_KEY}
  topic_status: "meshcore/{NODE_NAME}/status"
  topic_packets: "meshcore/{NODE_NAME}/packets"
  topic_raw: "meshcore/{NODE_NAME}/raw"

  # MQTT client settings
  client_id_prefix: "pymc_repeater"
  # Brokers like LetsMesh enforce a 23-char client ID cap; adjust if yours allows longer IDs
  client_id_max_length: 23
  qos: 0
  retain: false
  keepalive: 60
```

**Public MQTT Brokers:**

For connecting to public MeshCore observer networks like [LetsMesh.net](https://letsmesh.net), use these settings:

```yaml
mqtt:
  enabled: true
  server: mqtt-us-v1.letsmesh.net
  port: 443
  transport: "websockets"
  username: "your_username"
  password: "your_password"
  use_tls: true
  tls_verify: true
  topic_packets: "meshcore/{NODE_NAME}/packets"
  topic_raw: "meshcore/{NODE_NAME}/raw"

  #### LetsMesh / letsme.sh Auth Tokens

  Some public analyzers (LetsMesh Packet Analyzer, letsme.sh, meshcoretomqtt) require a JWT-style auth token derived from your repeater's public key instead of a static password. We ported the MeshCore signer directly into pyMC_Repeater (via PyNaCl), so no external Node.js tooling is required anymore:

  1. Decide which key will sign tokens. The easiest path is to reuse the repeater identity by setting `use_mesh_identity_key: true`, but you can also point at `private_key_path`, `private_key_hex`, or `private_key_env`.

  2. Enable the auth token block in `config.yaml`:

    ```yaml
    mqtt:
      enabled: true
      server: mqtt-us-v1.letsmesh.net
      port: 443
      use_tls: true
      tls_verify: true
      auth_token:
       enabled: true
       # reuse the repeater's mesh identity so you do not need to store a second key
       use_mesh_identity_key: true
       token_audience: "mqtt-us-v1.letsmesh.net"
       username_template: "v1_{PUBLIC_KEY}"
    ```

    The repeater refreshes the token every hour (configurable via `token_ttl`) without blocking forwarding.

  3. If LetsMesh provides a ready-made token (for example via an `AUTH_TOKEN` environment variable or helper script), skip signing by pointing pyMC_Repeater at the token source instead:

    ```yaml
    auth_token:
      enabled: true
      token_env_var: "AUTH_TOKEN"   # or set token_command: "/usr/local/bin/get_token {AUDIENCE}"
      username_template: "v1_{PUBLIC_KEY}"
    ```

  Additional knobs such as `private_key_path`, `token_email`, and `token_owner` are available in `config.yaml.example` for sites that expect richer JWT claims. Owner/email metadata is only sent when TLS verification is enabled to avoid leaking identity data over plaintext connections.
```
## Upgrading

To upgrade an existing installation to the latest version:

```bash
# Navigate to your pyMC_Repeater directory
cd pyMC_Repeater

# Run the upgrade script
sudo bash manage.sh
```

The upgrade script will:
- Pull the latest code from the main branch
- Update all application files
- Upgrade Python dependencies if needed
- Restart the service automatically
- Preserve your existing configuration




## Uninstallation

```bash
sudo bash manage.sh
```

This script will:
- Stop and disable the systemd service
- Remove the installation directory
- Optionally remove configuration, logs, and user data
- Optionally remove the service user account

The script will prompt you for each optional removal step.


## Roadmap / Planned Features

- [ ] **Public Map Integration** - Submit repeater location and details to public map for discovery
- [ ] **Remote Administration over LoRa** - Manage repeater configuration remotely via LoRa mesh
- [ ] **Trace Request Handling** - Respond to trace/diagnostic requests from mesh network


## Contributing

I welcome contributions! To contribute to pyMC_repeater:

1. **Fork the repository** and clone your fork
2. **Create a feature branch** from the `dev` branch:
   ```bash
   git checkout -b feature/your-feature-name dev
   ```
3. **Make your changes** and test with **real** hardware
4. **Commit with clear messages**:
   ```bash
   git commit -m "feat: description of changes"
   ```
5. **Push to your fork** and submit a **Pull Request to the `dev` branch**
   - Include a clear description of the changes
   - Reference any related issues

### Development Setup

```bash
# Install in development mode with dev tools (black, pytest, isort, mypy, etc)
pip install -e ".[dev]"

# Setup pre-commit hooks for code quality
pip install pre-commit
pre-commit install

# Manually run pre-commit checks on all files
pre-commit run --all-files
```

**Note:** Hardware support (LoRa radio drivers) is included in the base installation automatically via `pymc_core[hardware]`.

Pre-commit hooks will automatically:
- Format code with Black
- Sort imports with isort
- Lint with flake8
- Fix trailing whitespace and other file issues



## Support

- [Core Lib Documentation](https://rightup.github.io/pyMC_core/)
- [Meshcore Discord](https://discord.gg/fThwBrRc3Q)




## License

This project is licensed under the MIT License - see the LICENSE file for details.




