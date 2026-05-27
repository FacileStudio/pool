# Pool

Client library for the Nook Pool event bus. Dual implementation: TypeScript (`ts/`) and Go (`go/`).

## Architecture

Pool connects Facile Suite apps to Nook via WebSocket for real-time event sync. The client handles:
- Registration with Nook (HTTP POST)
- WebSocket connection with auto-reconnect (exponential backoff + jitter)
- Event emission and listening
- Offset tracking for replay on reconnect
- nook.yaml config file auto-detection

## Conventions

- TypeScript source in `ts/src/`, compiled output committed to `ts/dist/`
- Go source in `go/`
- Both implementations follow the same protocol and API shape
- Consumers install from GitHub: `bun add github:FacileStudio/pool#ts` or `go get github.com/FacileStudio/pool/go`

## Protocol

Messages are JSON over WebSocket:
- Client sends: `event`, `ack`, `subscribe`, `ping`
- Server sends: `welcome`, `event`, `subscribed`, `pong`, `error`

Each event has a per-channel monotonic `offset` for replay on reconnect.

## Development

```bash
cd ts && bun install && bun run build
cd go && go mod tidy
```
