---
sidebar_position: 2
---

# Run Azurite

## What is azurite?
The Azurite open-source emulator provides a free local environment for testing your Azure blob, queue storage, and table storage applications. When you're satisfied with how your application is working locally, switch to using an Azure Storage account in the cloud. The emulator provides cross-platform support on Windows, Linux, and macOS.

## Starting blob storage service

### Pull azurite docker image

Use the below docker command to pull the latest docker image:

```bash
docker pull mcr.microsoft.com/azure-storage/azurite
```

### Start the blob storage service

Use the below command to run the azurite with blob storage service:

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

> This will start blob storage service and listens on port 10000

## Connect to storage explorer

1. Start azure storage explorer
2. Select resource: Local storage emulator
3. Remove queues port and tables port and leave everything as default
4. Click next and then connect

> Your containers will be visible under blob containers