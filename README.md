# Replicated log, iteration 1

## Run

in terminal 1:

```bash
docker-compose up --build
```

in terminal 2:

```bash
curl http://localhost:5000/messages -H "Content-Type: application/json" -d '{"message":"hello blocking replication"}'
```

Result:
