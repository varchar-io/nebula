---
sidebar_position: 1
---

# Tutorial Intro

Let's discover **Nebula in less than 2 minutes**.

## Getting Started

Get started by **running nebula on your localbox**.

Nebula repo contains pre-built binaries built from both Ubuntu 20.04 LTS and MacOS (M1) Big Sur.
(We don't offer Windows built yet)

All binaries are static linked, it should work fine on your linux box and Mac laptop. Otherwise you need need to rebuild nebula.

At nebula repo root, you can

### Start up one node instance locally

```shell
./run.sh
```

This script will detect current user's operation system and launch the service processes by choosing the right version of pre-built binaries.
After this command, you can open browser, navigate to this address: 
```
  http://localhost:8088/
``` 
Now, explore Nebula through `nebula UI` with a preload dataset, if everything goes well, you should be able to see this:
![Init UI](/img/init.png)

Try to put keys and metrics in the `fields` and see how it visualize the result of your query!

### Try to build binaries 

```shell
./build.sh
```

This script will launch either `build.linux.sh` or `build.mac.sh`, both of them install pre-required softwares and launch the build process managed by `cmake`.
For prerequisites installation - it uses `brew` on MacOs while using `apt` on Linux. (Tested version: `Ubuntu 20.04 LTS` and `MacOs Big Sur + M1 Chip`).
Note: there maybe changes needed if you build nebula on other untested platforms.


### Docker images
(to be updated)

### Kubenetes
(to be updated)
