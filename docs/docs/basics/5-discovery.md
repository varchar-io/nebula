---
layout: default
---

# Service discovery
A normal Nebula cluster has 3 layers
- Web Tier: this tier is responsible for user interface, product experience and API providers.
- Nebula Server: nebula brain with all knowledge of the data state and working nodes states.
- Nebula Node: nebula worker to host some data and execute queries on top of them.

`Service discovery` is the way to allow each tier to understand host details of other tiers
so that they can communicate with each other.

Today, we're mostly targeting a single server deployment, so we usually run an embeded meta server and discovery server
inside the single Nebula Server host. 
However, this is not a high availability setup, to support that, we're planning to support server pool and run a standalone meta server and discovery server out of the server pool which provides static address to serve meta data and server discovery.

# Attractive option not chosen - Use Web Tier
Using web tier as discover server home is attractive, because every setup will have a web tier unique address - DNS name, no matter it's http://localhost or https://xyz.com, by opening an API endpoint to allow nodes to register themselves is straightforward and easy to manage. 
However, I did a final decision to not take it on due to
1. Security Hole: service discovery is kinda cluster internal activity, expose server info like IP and PORT through a public interface is not safe, if malformed info is injected by bad intension, it may cause the confusion of the server and eventually fail on too many bad hosts.
2. Flexibility: web Tier is indeed a pluggable tier, it could be completely customized and free form for different user experience even though Nebula itself provides a neat built-in web tier, it doesn't mean binding these cluster behavior to it is good, instead it may block a web tier from being fully customized.

# Static Server
A discovery server can work because all nodes know where to register themselves. As touched above already, in a single-server deployment, this single nebula server acts as discovery server as well, we requires it to be static so that node has it's address to call the discovery interface.

In the future, a high-availability setup will have a server pool, in that case, we will have a standalone meta server which acts as discovery server living out of the server pool.

# Cloud + VPC + Static IP
In today's major cloud providers, no matter it's AWS, GCP, or AZURE, they are all providing reserving an internal static IP within the VPC network, so we will consider that as a support for the option we have chosen - running discoery server inside Nebula Server. Here is a [link](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-internal-ip-address) describing how to do so in GCP for reference. 