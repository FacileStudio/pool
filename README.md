# @facile/pool

Client library for connecting Facile Suite apps to the Nook Pool event bus.

Available in TypeScript and Go.

## TypeScript

```bash
bun add github:FacileStudio/pool#ts
```

```typescript
import { PoolClient } from "@facile/pool";

const pool = new PoolClient({
  config: {
    app: "opus",
    instance: "http://localhost:3400",
    secret: "shared-secret",
    events: {
      emit: ["project.created", "project.updated", "project.deleted"],
      listen: ["project.created", "project.updated", "project.deleted"],
    },
  },
});

await pool.connect();

pool.emit("project.created", {
  app: "opus",
  object: "project",
  action: "created",
  facile_id: "fac_abc123",
  payload: { facile_id: "fac_abc123", name: "Acme" },
  timestamp: new Date().toISOString(),
  idempotency_key: "opus_proj_created_fac_abc123",
});

pool.listen("project.created", (payload, meta) => {
  console.log(`Project created by ${meta.sender}:`, payload);
});
```

### nook.yaml

Place a `nook.yaml` at your project root for auto-detection:

```yaml
app: opus
instance: http://localhost:3400
secret: shared-secret
events:
  emit:
    - project.created
    - project.updated
    - project.deleted
  listen:
    - project.created
    - project.updated
    - project.deleted
```

Then init without inline config:

```typescript
const pool = new PoolClient({});
```

## Go

```bash
go get github.com/FacileStudio/pool/go
```

```go
import pool "github.com/FacileStudio/pool/go"

client := pool.NewClient(&pool.Config{
    App:      "sablier",
    Instance: "http://localhost:3400",
    Secret:   "shared-secret",
    Events: pool.EventConfig{
        Emit:   []string{"project.created", "project.updated"},
        Listen: []string{"project.created", "project.updated"},
    },
})

client.Connect(ctx)
defer client.Disconnect()

client.Emit("project.created", payload)
client.Listen("project.created", func(payload json.RawMessage, meta pool.EventMeta) {
    // handle
})
```
