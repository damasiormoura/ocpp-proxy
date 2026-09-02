# CI and deployment

Two workflows, both on the self-hosted runner host:

| Workflow | Trigger | What it does |
|---|---|---|
| `.github/workflows/ci.yml` | every push and PR | fmt, clippy, tests, release build, Home Assistant template checks |
| `.github/workflows/deploy.yml` | manual only | renders the templates, rebuilds, installs onto the proxy LXC and Home Assistant |

## Why the toolchain comes from a container

The runner host has Docker but no Rust. Rather than installing a toolchain onto
it — one more thing to drift, and shared with unrelated repositories — both
workflows run `cargo` inside `rust:1-bookworm`, with the cargo registry in a
named Docker volume so it survives between runs.

`bookworm`, not `trixie`, deliberately: its glibc is older than the deployment
target's, so a binary built there runs there. A trixie build on an older target
would not.

## Why deployment is manual

A deploy restarts the proxy. While it is down the charger has no path to the
Central System, and where the SIM lives in the host's dongle there is no second
route. It is a two-second outage, but a real one, so it happens when somebody
asks rather than on every merge.

The job refuses outright if a charging session is open, because that outage
would land mid-transaction. Override with the `allow_open_transaction` input if
you mean it.

## Setup — one time

These three steps need repository-owner access and cannot be done from inside
the repository.

### 1. Register a runner for this repository

The existing runners are scoped to other repositories and will not pick up jobs
here. Add a third under the same convention, with an `ocpp-proxy` label to match
the `runs-on` in both workflows:

```bash
# Settings -> Actions -> Runners -> New self-hosted runner, to get a token.
ssh proxmox 'pct exec <RUNNER_LXC> -- sudo -u runner bash -lc "
  mkdir -p /opt/actions-runner/ocpp-proxy && cd /opt/actions-runner/ocpp-proxy &&
  curl -sLo r.tar.gz https://github.com/actions/runner/releases/latest/download/actions-runner-linux-x64.tar.gz &&
  tar xzf r.tar.gz &&
  ./config.sh --url https://github.com/<owner>/ocpp-proxy \
              --token <REGISTRATION_TOKEN> \
              --name gha-runner-ocpp-proxy \
              --labels self-hosted,ocpp-proxy --unattended"'
ssh proxmox 'pct exec <RUNNER_LXC> -- bash -lc "
  cd /opt/actions-runner/ocpp-proxy && ./svc.sh install runner && ./svc.sh start"'
```

The `runner` user must be in the `docker` group. It already is on the existing
host.

### 2. Give the runner a deploy key

The deploy job reaches the Proxmox host over SSH and works on the container and
the Home Assistant VM through `pct` and `qm`, so the container itself needs no
SSH server. Mirror the naming of the existing per-repository keys:

```bash
ssh proxmox 'pct exec <RUNNER_LXC> -- sudo -u runner \
  ssh-keygen -t ed25519 -N "" -C ocpp-proxy-deploy \
             -f /home/runner/.ssh/deploy_ocpp_proxy'
# then append the printed public key to the Proxmox host's authorized_keys
```

Restrict it in `authorized_keys` if you want: the job only needs `pct` and `qm`.

### 3. Add the `DEPLOY_LOCAL_ENV` secret

Settings → Secrets and variables → Actions → New repository secret, named
`DEPLOY_LOCAL_ENV`, whose value is the entire contents of your `deploy/local.env`
— the same file `render.sh` reads locally. The job writes it, renders from it,
and deletes it in an `always()` step.

This is where the Charge Point ID, the Central System address and the broker
address live. They are deployment parameters, not repository content; see
`deploy/local.env.example`.

Note it is a *secret* rather than a *variable* so the values are masked in logs.
It holds identifiers, not credentials — MQTT credentials stay in
`/etc/ocpp-proxy/secrets.env` on the target and never pass through CI.

## What the deploy job checks

Failing any of these fails the job:

1. **A session is not open** — unless overridden.
2. **Tests pass** — re-run here rather than trusting a green run on another
   commit, because this installs a binary onto the thing that bills the
   electricity.
3. **The deployed binary hashes to the one just built** — a running binary
   cannot be overwritten (`Text file busy`), and a failed push followed by a
   restart silently reuses the old one. That looks exactly like a successful
   deploy that changed nothing, and it has happened here before.
4. **The proxy reports a serving state afterwards** — `healthy`, `idle` or
   `degraded`. `idle` passes: it means the listener is bound with no charger yet
   connected, and the charger takes about twenty seconds to come back.
5. **`ha core check` passes** before Home Assistant is restarted, and it is only
   restarted if the package actually changed. The dashboard is a YAML-mode
   Lovelace file and needs no restart at all.

## Running a deploy by hand

`render.sh` is the same script the workflow uses, so a manual deploy and a CI
deploy install identical bytes:

```bash
cp deploy/local.env.example deploy/local.env   # then fill it in
./deploy/render.sh                              # output in deploy/.rendered/
```

Both `deploy/local.env` and `deploy/.rendered/` are gitignored.
