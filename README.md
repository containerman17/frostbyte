# FrostByte

## Notes

- e2e is originally 2c1BN4N9qEhNmW4yCpeLK24SfjFQLyS1Z7FtgRDaYxZWFUUKxf

## Restore E2E Database

```bash
# Download and extract database backup
curl -o database_e2e_numine.tar.gz https://pub-bdd9bf0f9525419495f511e25d842b66.r2.dev/database_e2e_numine.tar.gz
tar -xzf database_e2e_numine.tar.gz
rm database_e2e_numine.tar.gz
```

Now run it `npm run test:specs`
