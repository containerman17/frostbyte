# FrostByte

## Notes

- e2e is originally 2Xj6iZeySuuUTZR5jYjhQ7yQ84q8AtDWnAJ2vS9BmmXBNJhYD1

## Restore E2E Database

```bash
# Download and extract database backup
curl -o database_e2e_backup.tar.gz https://pub-bdd9bf0f9525419495f511e25d842b66.r2.dev/database_e2e_backup.tar.gz
tar -xzf database_e2e_backup.tar.gz
rm database_e2e_backup.tar.gz
```

Now run it `npx tsx ./specs/spec.test.ts`
