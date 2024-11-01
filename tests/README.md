# Testing

You can run tests on both local and GitHub actions.  
`flake8` is used for linting and `pytest` is used for unit tests.

### Local Environment
To run tests locally, you need to have `docker` and `docker-compose` installed.  
`docker-compose` runs a MySQL container and a Redis container for testing.

On root directory, run the following command:
```bash
docker-compose -f tests/docker-compose.yml up -d
export PYTHONPATH="$(pwd)/src/"
python -m pytest tests
```

### GitHub Actions
GitHub Actions runs tests on pull requests and pushes to the main branch.  
