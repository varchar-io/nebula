"use strict";(self.webpackChunknebula=self.webpackChunknebula||[]).push([[389],{2803:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return n},contentTitle:function(){return l},metadata:function(){return d},toc:function(){return c},default:function(){return v}});var i=r(7462),s=r(3366),a=(r(7294),r(3905)),o=["components"],n={layout:"default"},l="Service discovery",d={unversionedId:"basics/discovery",id:"basics/discovery",isDocsHomePage:!1,title:"Service discovery",description:"A normal Nebula cluster has 3 layers",source:"@site/docs/basics/5-discovery.md",sourceDirName:"basics",slug:"/basics/discovery",permalink:"/docs/basics/discovery",tags:[],version:"current",sidebarPosition:5,frontMatter:{layout:"default"},sidebar:"defaultSidebar",previous:{title:"SDK: javascript",permalink:"/docs/basics/sdk"}},c=[],u={toc:c};function v(e){var t=e.components,r=(0,s.Z)(e,o);return(0,a.kt)("wrapper",(0,i.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"service-discovery"},"Service discovery"),(0,a.kt)("p",null,"A normal Nebula cluster has 3 layers"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Web Tier: this tier is responsible for user interface, product experience and API providers."),(0,a.kt)("li",{parentName:"ul"},"Nebula Server: nebula brain with all knowledge of the data state and working nodes states."),(0,a.kt)("li",{parentName:"ul"},"Nebula Node: nebula worker to host some data and execute queries on top of them.")),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"Service discovery")," is the way to allow each tier to understand host details of other tiers\nso that they can communicate with each other."),(0,a.kt)("p",null,"Today, we're mostly targeting a single server deployment, so we usually run an embeded meta server and discovery server\ninside the single Nebula Server host.\nHowever, this is not a high availability setup, to support that, we're planning to support server pool and run a standalone meta server and discovery server out of the server pool which provides static address to serve meta data and server discovery."),(0,a.kt)("h1",{id:"attractive-option-not-chosen---use-web-tier"},"Attractive option not chosen - Use Web Tier"),(0,a.kt)("p",null,"Using web tier as discover server home is attractive, because every setup will have a web tier unique address - DNS name, no matter it's http://localhost or ",(0,a.kt)("a",{parentName:"p",href:"https://xyz.com"},"https://xyz.com"),", by opening an API endpoint to allow nodes to register themselves is straightforward and easy to manage.\nHowever, I did a final decision to not take it on due to"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Security Hole: service discovery is kinda cluster internal activity, expose server info like IP and PORT through a public interface is not safe, if malformed info is injected by bad intension, it may cause the confusion of the server and eventually fail on too many bad hosts."),(0,a.kt)("li",{parentName:"ol"},"Flexibility: web Tier is indeed a pluggable tier, it could be completely customized and free form for different user experience even though Nebula itself provides a neat built-in web tier, it doesn't mean binding these cluster behavior to it is good, instead it may block a web tier from being fully customized.")),(0,a.kt)("h1",{id:"static-server"},"Static Server"),(0,a.kt)("p",null,"A discovery server can work because all nodes know where to register themselves. As touched above already, in a single-server deployment, this single nebula server acts as discovery server as well, we requires it to be static so that node has it's address to call the discovery interface."),(0,a.kt)("p",null,"In the future, a high-availability setup will have a server pool, in that case, we will have a standalone meta server which acts as discovery server living out of the server pool."),(0,a.kt)("h1",{id:"cloud--vpc--static-ip"},"Cloud + VPC + Static IP"),(0,a.kt)("p",null,"In today's major cloud providers, no matter it's AWS, GCP, or AZURE, they are all providing reserving an internal static IP within the VPC network, so we will consider that as a support for the option we have chosen - running discoery server inside Nebula Server. Here is a ",(0,a.kt)("a",{parentName:"p",href:"https://cloud.google.com/compute/docs/ip-addresses/reserve-static-internal-ip-address"},"link")," describing how to do so in GCP for reference."))}v.isMDXComponent=!0}}]);