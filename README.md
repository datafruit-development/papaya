<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/lychee-development/papaya/blob/main/docs/logo_dark.svg?raw=true">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/lychee-development/papaya/blob/main/docs/logo_light.svg?raw=true">
    <img alt="Papaya Logo" src="https://github.com/lychee-development/papaya/blob/main/docs/logo_light.svg?raw=true" width="300" style="display: block; margin: 0 auto;">
  </picture>
</p>

---

# Papaya is an intelligent debugging tool designed specifically for Apache Spark jobs.
It seamlessly integrates error monitoring, real-time notifications, and automated LLM-powered debugging to help debug your complicated Spark workflows.


* **Real-time Monitoring**: Continuously listens to your Apache Spark jobs and instantly detects errors and anomalies.
* **Discord Alerts**: Sends immediate notifications directly to your Discord channel whenever issues arise.
* **Intelligent Debugging**: Optionally analyzes your Spark errors and suggest automatic code fixes.
* **Automated Pull Requests**: Papaya automatically submits GitHub pull requests containing proposed code changes.

---

## 🛠️ Installation

Install Papaya via pip:

```bash
pip install papaya-debugger
```

---

## 🌟 Quick Start

Run Papaya by specifying the Spark UI URL:

```bash
papaya http://localhost:4040
```

To enable Discord notifications (requires `DISCORD_TOKEN` environment variable):

```bash
papaya http://localhost:4040 --discord-cid 123456789
```

To enable GitHub integration (requires `GH_APP_TOKEN` environment variable):

```bash
papaya http://localhost:4040 --github-repo myorg/myrepo
```

You can also adjust the polling interval (default is 0.5 seconds):

```bash
papaya http://localhost:4040 --poll 2.0
```

> **Note:** You must set the `GEMINI_API_KEY` environment variable for Papaya to function.

---

## 📝 CLI Options & Environment Variables

**Positional Arguments:**
- `SPARK_UI_URL` (required): Spark UI URL of the active job to monitor (e.g., `http://localhost:4040`)

**Optional Arguments:**
- `--discord-cid <int>`: Discord channel ID to send messages to (requires `DISCORD_TOKEN` env var)
- `--github-repo <OWNER/REPO>`: GitHub repository of the Spark job (requires `GH_APP_TOKEN` env var)
- `--poll <SECONDS>`: Polling interval in seconds (default: 0.5)

**Environment Variables:**
- `GEMINI_API_KEY`: **Required** for Papaya to function (used for LLM-powered debugging).
- `DISCORD_TOKEN`: Required if using `--discord-cid` to send notifications to Discord.
- `GH_APP_TOKEN`: Required if using `--github-repo` to link to a GitHub repository.

---

## 📚 Documentation

Docs coming soon :)

---

## 🌐 Contributing

Contributions are welcome! Please open issues and pull requests on GitHub.

---
