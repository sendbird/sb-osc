# Usage Guide

SB-OSC is designed to be deployed as a containerized application.  
It can be run on both Kubernetes and Docker environments.  

For Kubernetes deployment refer to [charts](./charts) directory, and for Docker deployment refer to [compose](./compose) directory.  

### Building Docker Image
You can build Docker image using Dockerfile in the root directory.  
```bash
docker build -t sb-osc .
```

### Troubleshooting
Issues and solutions that may occur when using SB-OSC can be found in [troubleshooting.md](../doc/troubleshooting.md).
