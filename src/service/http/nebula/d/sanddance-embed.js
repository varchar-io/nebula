var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
var SandDance = SandDanceExplorer.SandDance;
var VegaDeckGl = SandDance.VegaDeckGl;
var SandDanceEmbed;
(function (SandDanceEmbed) {
    SandDanceEmbed.requests = [];
    function load(data, insight) {
        return new Promise(function (resolve) {
            var innerLoad = function () {
                var getPartialInsight;
                if (insight) {
                    //TODO make sure that insight columns exist in dataset
                    getPartialInsight = function (columns) { return insight; };
                }
                SandDanceEmbed.sandDanceExplorer.load(data, getPartialInsight).then(resolve);
            };
            var create = function () {
                SandDanceExplorer.use(Fabric, vega, deck, deck, luma);
                var explorerProps = {
                    logoClickUrl: 'https://microsoft.github.io/SandDance/',
                    mounted: function (explorer) {
                        SandDanceEmbed.sandDanceExplorer = explorer;
                        innerLoad();
                    }
                };
                ReactDOM.render(React.createElement(SandDanceExplorer.Explorer, explorerProps), document.getElementById('app'));
            };
            if (SandDanceEmbed.sandDanceExplorer) {
                innerLoad();
            }
            else {
                create();
            }
        });
    }
    SandDanceEmbed.load = load;
    function respondToRequest(requestWithSource) {
        SandDanceEmbed.requests.push(requestWithSource);
        var copy = __assign({}, requestWithSource);
        delete copy.source;
        var request = __assign({}, copy);
        var response;
        switch (request.action) {
            case 'init': {
                response = {
                    request: request
                };
                break;
            }
            case 'load': {
                var request_load = request;
                load(request_load.data, request_load.insight).then(function () {
                    response = {
                        request: request
                    };
                    requestWithSource.source.postMessage(response, '*');
                });
                //don't keep a copy of the array
                delete request_load.data;
                break;
            }
            case 'getData': {
                response = {
                    request: request,
                    data: SandDanceEmbed.sandDanceExplorer.state.dataContent.data
                };
                break;
            }
            case 'getInsight': {
                response = {
                    request: request,
                    insight: SandDanceEmbed.sandDanceExplorer.viewer.getInsight()
                };
                break;
            }
        }
        if (response) {
            requestWithSource.source.postMessage(response, '*');
        }
    }
    SandDanceEmbed.respondToRequest = respondToRequest;
    window.addEventListener('message', function (e) {
        var payload = e.data;
        if (!payload)
            return;
        if (Array.isArray(payload)) {
            var data = payload;
            var requestLoadFromArray = {
                action: 'load',
                data: data,
                insight: null
            };
            payload = requestLoadFromArray;
        }
        else {
            var dataWithInsight = payload;
            if (Array.isArray(dataWithInsight.data)) {
                var requestLoadFromDataWithInsight = __assign({ action: 'load' }, dataWithInsight);
                payload = requestLoadFromDataWithInsight;
            }
        }
        var request = payload;
        if (!request)
            return;
        var requestWithSource = __assign({}, request, { source: e.source });
        respondToRequest(requestWithSource);
    });
    if (window.opener) {
        var request = {
            action: 'init',
            ts: new Date()
        };
        respondToRequest(__assign({}, request, { source: window.opener }));
    }
})(SandDanceEmbed || (SandDanceEmbed = {}));
