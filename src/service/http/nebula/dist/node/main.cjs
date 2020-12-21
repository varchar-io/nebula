(function(e, a) { for(var i in a) e[i] = a[i]; }(exports, /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, { enumerable: true, get: getter });
/******/ 		}
/******/ 	};
/******/
/******/ 	// define __esModule on exports
/******/ 	__webpack_require__.r = function(exports) {
/******/ 		if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 			Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 		}
/******/ 		Object.defineProperty(exports, '__esModule', { value: true });
/******/ 	};
/******/
/******/ 	// create a fake namespace object
/******/ 	// mode & 1: value is a module id, require it
/******/ 	// mode & 2: merge all properties of value into the ns
/******/ 	// mode & 4: return value when already ns object
/******/ 	// mode & 8|1: behave like require
/******/ 	__webpack_require__.t = function(value, mode) {
/******/ 		if(mode & 1) value = __webpack_require__(value);
/******/ 		if(mode & 8) return value;
/******/ 		if((mode & 4) && typeof value === 'object' && value && value.__esModule) return value;
/******/ 		var ns = Object.create(null);
/******/ 		__webpack_require__.r(ns);
/******/ 		Object.defineProperty(ns, 'default', { enumerable: true, value: value });
/******/ 		if(mode & 2 && typeof value != 'string') for(var key in value) __webpack_require__.d(ns, key, function(key) { return value[key]; }.bind(null, key));
/******/ 		return ns;
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 6);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

// source: nebula.proto
/**
 * @fileoverview
 * @enhanceable
 * @suppress {messageConventions} JS Compiler reports an error if a variable or
 *     field starts with 'MSG_' and isn't a translatable message.
 * @public
 */
// GENERATED CODE -- DO NOT EDIT!
/* eslint-disable */
// @ts-nocheck

var jspb = __webpack_require__(7);
var goog = jspb;
var global = Function('return this')();

goog.exportSymbol('proto.nebula.service.CustomColumn', null, global);
goog.exportSymbol('proto.nebula.service.CustomType', null, global);
goog.exportSymbol('proto.nebula.service.DataType', null, global);
goog.exportSymbol('proto.nebula.service.EchoRequest', null, global);
goog.exportSymbol('proto.nebula.service.EchoResponse', null, global);
goog.exportSymbol('proto.nebula.service.ListTables', null, global);
goog.exportSymbol('proto.nebula.service.LoadError', null, global);
goog.exportSymbol('proto.nebula.service.LoadRequest', null, global);
goog.exportSymbol('proto.nebula.service.LoadResponse', null, global);
goog.exportSymbol('proto.nebula.service.LoadType', null, global);
goog.exportSymbol('proto.nebula.service.Metric', null, global);
goog.exportSymbol('proto.nebula.service.Operation', null, global);
goog.exportSymbol('proto.nebula.service.Order', null, global);
goog.exportSymbol('proto.nebula.service.OrderType', null, global);
goog.exportSymbol('proto.nebula.service.PingResponse', null, global);
goog.exportSymbol('proto.nebula.service.Predicate', null, global);
goog.exportSymbol('proto.nebula.service.PredicateAnd', null, global);
goog.exportSymbol('proto.nebula.service.PredicateOr', null, global);
goog.exportSymbol('proto.nebula.service.QueryRequest', null, global);
goog.exportSymbol('proto.nebula.service.QueryRequest.FilterCase', null, global);
goog.exportSymbol('proto.nebula.service.QueryResponse', null, global);
goog.exportSymbol('proto.nebula.service.Rollup', null, global);
goog.exportSymbol('proto.nebula.service.ServiceInfo', null, global);
goog.exportSymbol('proto.nebula.service.ServiceTier', null, global);
goog.exportSymbol('proto.nebula.service.Statistics', null, global);
goog.exportSymbol('proto.nebula.service.TableList', null, global);
goog.exportSymbol('proto.nebula.service.TableStateRequest', null, global);
goog.exportSymbol('proto.nebula.service.TableStateResponse', null, global);
goog.exportSymbol('proto.nebula.service.UrlData', null, global);
goog.exportSymbol('proto.nebula.service.ZipFormat', null, global);
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.EchoRequest = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.EchoRequest, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.EchoRequest.displayName = 'proto.nebula.service.EchoRequest';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.EchoResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.EchoResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.EchoResponse.displayName = 'proto.nebula.service.EchoResponse';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.UrlData = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.UrlData, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.UrlData.displayName = 'proto.nebula.service.UrlData';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.TableStateRequest = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.TableStateRequest, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.TableStateRequest.displayName = 'proto.nebula.service.TableStateRequest';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.ListTables = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.ListTables, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.ListTables.displayName = 'proto.nebula.service.ListTables';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.TableList = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.TableList.repeatedFields_, null);
};
goog.inherits(proto.nebula.service.TableList, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.TableList.displayName = 'proto.nebula.service.TableList';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.TableStateResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.TableStateResponse.repeatedFields_, null);
};
goog.inherits(proto.nebula.service.TableStateResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.TableStateResponse.displayName = 'proto.nebula.service.TableStateResponse';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.Predicate = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.Predicate.repeatedFields_, null);
};
goog.inherits(proto.nebula.service.Predicate, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.Predicate.displayName = 'proto.nebula.service.Predicate';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.PredicateAnd = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.PredicateAnd.repeatedFields_, null);
};
goog.inherits(proto.nebula.service.PredicateAnd, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.PredicateAnd.displayName = 'proto.nebula.service.PredicateAnd';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.PredicateOr = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.PredicateOr.repeatedFields_, null);
};
goog.inherits(proto.nebula.service.PredicateOr, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.PredicateOr.displayName = 'proto.nebula.service.PredicateOr';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.Metric = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.Metric, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.Metric.displayName = 'proto.nebula.service.Metric';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.Order = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.Order, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.Order.displayName = 'proto.nebula.service.Order';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.CustomColumn = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.CustomColumn, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.CustomColumn.displayName = 'proto.nebula.service.CustomColumn';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.QueryRequest = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.nebula.service.QueryRequest.repeatedFields_, proto.nebula.service.QueryRequest.oneofGroups_);
};
goog.inherits(proto.nebula.service.QueryRequest, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.QueryRequest.displayName = 'proto.nebula.service.QueryRequest';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.Statistics = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.Statistics, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.Statistics.displayName = 'proto.nebula.service.Statistics';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.QueryResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.QueryResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.QueryResponse.displayName = 'proto.nebula.service.QueryResponse';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.LoadRequest = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.LoadRequest, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.LoadRequest.displayName = 'proto.nebula.service.LoadRequest';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.LoadResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.LoadResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.LoadResponse.displayName = 'proto.nebula.service.LoadResponse';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.ServiceInfo = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.ServiceInfo, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.ServiceInfo.displayName = 'proto.nebula.service.ServiceInfo';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.nebula.service.PingResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.nebula.service.PingResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.nebula.service.PingResponse.displayName = 'proto.nebula.service.PingResponse';
}



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.EchoRequest.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.EchoRequest.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.EchoRequest} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.EchoRequest.toObject = function(includeInstance, msg) {
  var f, obj = {
    name: jspb.Message.getFieldWithDefault(msg, 1, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.EchoRequest}
 */
proto.nebula.service.EchoRequest.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.EchoRequest;
  return proto.nebula.service.EchoRequest.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.EchoRequest} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.EchoRequest}
 */
proto.nebula.service.EchoRequest.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setName(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.EchoRequest.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.EchoRequest.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.EchoRequest} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.EchoRequest.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getName();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
};


/**
 * optional string name = 1;
 * @return {string}
 */
proto.nebula.service.EchoRequest.prototype.getName = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.EchoRequest} returns this
 */
proto.nebula.service.EchoRequest.prototype.setName = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.EchoResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.EchoResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.EchoResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.EchoResponse.toObject = function(includeInstance, msg) {
  var f, obj = {
    message: jspb.Message.getFieldWithDefault(msg, 1, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.EchoResponse}
 */
proto.nebula.service.EchoResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.EchoResponse;
  return proto.nebula.service.EchoResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.EchoResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.EchoResponse}
 */
proto.nebula.service.EchoResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setMessage(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.EchoResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.EchoResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.EchoResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.EchoResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getMessage();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
};


/**
 * optional string message = 1;
 * @return {string}
 */
proto.nebula.service.EchoResponse.prototype.getMessage = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.EchoResponse} returns this
 */
proto.nebula.service.EchoResponse.prototype.setMessage = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.UrlData.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.UrlData.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.UrlData} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.UrlData.toObject = function(includeInstance, msg) {
  var f, obj = {
    code: jspb.Message.getFieldWithDefault(msg, 1, ""),
    raw: jspb.Message.getFieldWithDefault(msg, 2, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.UrlData}
 */
proto.nebula.service.UrlData.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.UrlData;
  return proto.nebula.service.UrlData.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.UrlData} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.UrlData}
 */
proto.nebula.service.UrlData.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setCode(value);
      break;
    case 2:
      var value = /** @type {string} */ (reader.readString());
      msg.setRaw(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.UrlData.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.UrlData.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.UrlData} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.UrlData.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getCode();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getRaw();
  if (f.length > 0) {
    writer.writeString(
      2,
      f
    );
  }
};


/**
 * optional string code = 1;
 * @return {string}
 */
proto.nebula.service.UrlData.prototype.getCode = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.UrlData} returns this
 */
proto.nebula.service.UrlData.prototype.setCode = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional string raw = 2;
 * @return {string}
 */
proto.nebula.service.UrlData.prototype.getRaw = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 2, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.UrlData} returns this
 */
proto.nebula.service.UrlData.prototype.setRaw = function(value) {
  return jspb.Message.setProto3StringField(this, 2, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.TableStateRequest.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.TableStateRequest.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.TableStateRequest} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableStateRequest.toObject = function(includeInstance, msg) {
  var f, obj = {
    table: jspb.Message.getFieldWithDefault(msg, 1, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.TableStateRequest}
 */
proto.nebula.service.TableStateRequest.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.TableStateRequest;
  return proto.nebula.service.TableStateRequest.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.TableStateRequest} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.TableStateRequest}
 */
proto.nebula.service.TableStateRequest.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setTable(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.TableStateRequest.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.TableStateRequest.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.TableStateRequest} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableStateRequest.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getTable();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
};


/**
 * optional string table = 1;
 * @return {string}
 */
proto.nebula.service.TableStateRequest.prototype.getTable = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.TableStateRequest} returns this
 */
proto.nebula.service.TableStateRequest.prototype.setTable = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.ListTables.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.ListTables.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.ListTables} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.ListTables.toObject = function(includeInstance, msg) {
  var f, obj = {
    limit: jspb.Message.getFieldWithDefault(msg, 1, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.ListTables}
 */
proto.nebula.service.ListTables.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.ListTables;
  return proto.nebula.service.ListTables.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.ListTables} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.ListTables}
 */
proto.nebula.service.ListTables.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setLimit(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.ListTables.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.ListTables.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.ListTables} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.ListTables.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getLimit();
  if (f !== 0) {
    writer.writeUint32(
      1,
      f
    );
  }
};


/**
 * optional uint32 limit = 1;
 * @return {number}
 */
proto.nebula.service.ListTables.prototype.getLimit = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.ListTables} returns this
 */
proto.nebula.service.ListTables.prototype.setLimit = function(value) {
  return jspb.Message.setProto3IntField(this, 1, value);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.TableList.repeatedFields_ = [1];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.TableList.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.TableList.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.TableList} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableList.toObject = function(includeInstance, msg) {
  var f, obj = {
    tableList: (f = jspb.Message.getRepeatedField(msg, 1)) == null ? undefined : f
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.TableList}
 */
proto.nebula.service.TableList.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.TableList;
  return proto.nebula.service.TableList.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.TableList} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.TableList}
 */
proto.nebula.service.TableList.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.addTable(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.TableList.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.TableList.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.TableList} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableList.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getTableList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      1,
      f
    );
  }
};


/**
 * repeated string table = 1;
 * @return {!Array<string>}
 */
proto.nebula.service.TableList.prototype.getTableList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 1));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.TableList} returns this
 */
proto.nebula.service.TableList.prototype.setTableList = function(value) {
  return jspb.Message.setField(this, 1, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.TableList} returns this
 */
proto.nebula.service.TableList.prototype.addTable = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 1, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.TableList} returns this
 */
proto.nebula.service.TableList.prototype.clearTableList = function() {
  return this.setTableList([]);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.TableStateResponse.repeatedFields_ = [6,7,8];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.TableStateResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.TableStateResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.TableStateResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableStateResponse.toObject = function(includeInstance, msg) {
  var f, obj = {
    blockcount: jspb.Message.getFieldWithDefault(msg, 1, 0),
    rowcount: jspb.Message.getFieldWithDefault(msg, 2, 0),
    memsize: jspb.Message.getFieldWithDefault(msg, 3, 0),
    mintime: jspb.Message.getFieldWithDefault(msg, 4, 0),
    maxtime: jspb.Message.getFieldWithDefault(msg, 5, 0),
    dimensionList: (f = jspb.Message.getRepeatedField(msg, 6)) == null ? undefined : f,
    metricList: (f = jspb.Message.getRepeatedField(msg, 7)) == null ? undefined : f,
    histsList: (f = jspb.Message.getRepeatedField(msg, 8)) == null ? undefined : f
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.TableStateResponse}
 */
proto.nebula.service.TableStateResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.TableStateResponse;
  return proto.nebula.service.TableStateResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.TableStateResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.TableStateResponse}
 */
proto.nebula.service.TableStateResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {number} */ (reader.readInt32());
      msg.setBlockcount(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setRowcount(value);
      break;
    case 3:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setMemsize(value);
      break;
    case 4:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setMintime(value);
      break;
    case 5:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setMaxtime(value);
      break;
    case 6:
      var value = /** @type {string} */ (reader.readString());
      msg.addDimension(value);
      break;
    case 7:
      var value = /** @type {string} */ (reader.readString());
      msg.addMetric(value);
      break;
    case 8:
      var value = /** @type {string} */ (reader.readString());
      msg.addHists(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.TableStateResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.TableStateResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.TableStateResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.TableStateResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getBlockcount();
  if (f !== 0) {
    writer.writeInt32(
      1,
      f
    );
  }
  f = message.getRowcount();
  if (f !== 0) {
    writer.writeInt64(
      2,
      f
    );
  }
  f = message.getMemsize();
  if (f !== 0) {
    writer.writeInt64(
      3,
      f
    );
  }
  f = message.getMintime();
  if (f !== 0) {
    writer.writeUint64(
      4,
      f
    );
  }
  f = message.getMaxtime();
  if (f !== 0) {
    writer.writeUint64(
      5,
      f
    );
  }
  f = message.getDimensionList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      6,
      f
    );
  }
  f = message.getMetricList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      7,
      f
    );
  }
  f = message.getHistsList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      8,
      f
    );
  }
};


/**
 * optional int32 blockCount = 1;
 * @return {number}
 */
proto.nebula.service.TableStateResponse.prototype.getBlockcount = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setBlockcount = function(value) {
  return jspb.Message.setProto3IntField(this, 1, value);
};


/**
 * optional int64 rowCount = 2;
 * @return {number}
 */
proto.nebula.service.TableStateResponse.prototype.getRowcount = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setRowcount = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * optional int64 memSize = 3;
 * @return {number}
 */
proto.nebula.service.TableStateResponse.prototype.getMemsize = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 3, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setMemsize = function(value) {
  return jspb.Message.setProto3IntField(this, 3, value);
};


/**
 * optional uint64 minTime = 4;
 * @return {number}
 */
proto.nebula.service.TableStateResponse.prototype.getMintime = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 4, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setMintime = function(value) {
  return jspb.Message.setProto3IntField(this, 4, value);
};


/**
 * optional uint64 maxTime = 5;
 * @return {number}
 */
proto.nebula.service.TableStateResponse.prototype.getMaxtime = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 5, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setMaxtime = function(value) {
  return jspb.Message.setProto3IntField(this, 5, value);
};


/**
 * repeated string dimension = 6;
 * @return {!Array<string>}
 */
proto.nebula.service.TableStateResponse.prototype.getDimensionList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 6));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setDimensionList = function(value) {
  return jspb.Message.setField(this, 6, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.addDimension = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 6, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.clearDimensionList = function() {
  return this.setDimensionList([]);
};


/**
 * repeated string metric = 7;
 * @return {!Array<string>}
 */
proto.nebula.service.TableStateResponse.prototype.getMetricList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 7));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setMetricList = function(value) {
  return jspb.Message.setField(this, 7, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.addMetric = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 7, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.clearMetricList = function() {
  return this.setMetricList([]);
};


/**
 * repeated string hists = 8;
 * @return {!Array<string>}
 */
proto.nebula.service.TableStateResponse.prototype.getHistsList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 8));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.setHistsList = function(value) {
  return jspb.Message.setField(this, 8, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.addHists = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 8, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.TableStateResponse} returns this
 */
proto.nebula.service.TableStateResponse.prototype.clearHistsList = function() {
  return this.setHistsList([]);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.Predicate.repeatedFields_ = [3,4,5];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.Predicate.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.Predicate.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.Predicate} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Predicate.toObject = function(includeInstance, msg) {
  var f, obj = {
    column: jspb.Message.getFieldWithDefault(msg, 1, ""),
    op: jspb.Message.getFieldWithDefault(msg, 2, 0),
    valueList: (f = jspb.Message.getRepeatedField(msg, 3)) == null ? undefined : f,
    nValueList: (f = jspb.Message.getRepeatedField(msg, 4)) == null ? undefined : f,
    dValueList: (f = jspb.Message.getRepeatedFloatingPointField(msg, 5)) == null ? undefined : f,
    zip: msg.getZip_asB64(),
    zipformat: jspb.Message.getFieldWithDefault(msg, 7, 0),
    zipcount: jspb.Message.getFieldWithDefault(msg, 8, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.Predicate}
 */
proto.nebula.service.Predicate.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.Predicate;
  return proto.nebula.service.Predicate.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.Predicate} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.Predicate}
 */
proto.nebula.service.Predicate.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setColumn(value);
      break;
    case 2:
      var value = /** @type {!proto.nebula.service.Operation} */ (reader.readEnum());
      msg.setOp(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.addValue(value);
      break;
    case 4:
      var value = /** @type {!Array<string>} */ (reader.readPackedInt64String());
      msg.setNValueList(value);
      break;
    case 5:
      var value = /** @type {!Array<number>} */ (reader.readPackedDouble());
      msg.setDValueList(value);
      break;
    case 6:
      var value = /** @type {!Uint8Array} */ (reader.readBytes());
      msg.setZip(value);
      break;
    case 7:
      var value = /** @type {!proto.nebula.service.ZipFormat} */ (reader.readEnum());
      msg.setZipformat(value);
      break;
    case 8:
      var value = /** @type {number} */ (reader.readInt32());
      msg.setZipcount(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.Predicate.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.Predicate.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.Predicate} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Predicate.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getColumn();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getOp();
  if (f !== 0.0) {
    writer.writeEnum(
      2,
      f
    );
  }
  f = message.getValueList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      3,
      f
    );
  }
  f = message.getNValueList();
  if (f.length > 0) {
    writer.writePackedInt64String(
      4,
      f
    );
  }
  f = message.getDValueList();
  if (f.length > 0) {
    writer.writePackedDouble(
      5,
      f
    );
  }
  f = message.getZip_asU8();
  if (f.length > 0) {
    writer.writeBytes(
      6,
      f
    );
  }
  f = message.getZipformat();
  if (f !== 0.0) {
    writer.writeEnum(
      7,
      f
    );
  }
  f = message.getZipcount();
  if (f !== 0) {
    writer.writeInt32(
      8,
      f
    );
  }
};


/**
 * optional string column = 1;
 * @return {string}
 */
proto.nebula.service.Predicate.prototype.getColumn = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setColumn = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional Operation op = 2;
 * @return {!proto.nebula.service.Operation}
 */
proto.nebula.service.Predicate.prototype.getOp = function() {
  return /** @type {!proto.nebula.service.Operation} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {!proto.nebula.service.Operation} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setOp = function(value) {
  return jspb.Message.setProto3EnumField(this, 2, value);
};


/**
 * repeated string value = 3;
 * @return {!Array<string>}
 */
proto.nebula.service.Predicate.prototype.getValueList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 3));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setValueList = function(value) {
  return jspb.Message.setField(this, 3, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.addValue = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 3, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.clearValueList = function() {
  return this.setValueList([]);
};


/**
 * repeated int64 n_value = 4;
 * @return {!Array<string>}
 */
proto.nebula.service.Predicate.prototype.getNValueList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 4));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setNValueList = function(value) {
  return jspb.Message.setField(this, 4, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.addNValue = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 4, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.clearNValueList = function() {
  return this.setNValueList([]);
};


/**
 * repeated double d_value = 5;
 * @return {!Array<number>}
 */
proto.nebula.service.Predicate.prototype.getDValueList = function() {
  return /** @type {!Array<number>} */ (jspb.Message.getRepeatedFloatingPointField(this, 5));
};


/**
 * @param {!Array<number>} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setDValueList = function(value) {
  return jspb.Message.setField(this, 5, value || []);
};


/**
 * @param {number} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.addDValue = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 5, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.clearDValueList = function() {
  return this.setDValueList([]);
};


/**
 * optional bytes zip = 6;
 * @return {!(string|Uint8Array)}
 */
proto.nebula.service.Predicate.prototype.getZip = function() {
  return /** @type {!(string|Uint8Array)} */ (jspb.Message.getFieldWithDefault(this, 6, ""));
};


/**
 * optional bytes zip = 6;
 * This is a type-conversion wrapper around `getZip()`
 * @return {string}
 */
proto.nebula.service.Predicate.prototype.getZip_asB64 = function() {
  return /** @type {string} */ (jspb.Message.bytesAsB64(
      this.getZip()));
};


/**
 * optional bytes zip = 6;
 * Note that Uint8Array is not supported on all browsers.
 * @see http://caniuse.com/Uint8Array
 * This is a type-conversion wrapper around `getZip()`
 * @return {!Uint8Array}
 */
proto.nebula.service.Predicate.prototype.getZip_asU8 = function() {
  return /** @type {!Uint8Array} */ (jspb.Message.bytesAsU8(
      this.getZip()));
};


/**
 * @param {!(string|Uint8Array)} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setZip = function(value) {
  return jspb.Message.setProto3BytesField(this, 6, value);
};


/**
 * optional ZipFormat ZipFormat = 7;
 * @return {!proto.nebula.service.ZipFormat}
 */
proto.nebula.service.Predicate.prototype.getZipformat = function() {
  return /** @type {!proto.nebula.service.ZipFormat} */ (jspb.Message.getFieldWithDefault(this, 7, 0));
};


/**
 * @param {!proto.nebula.service.ZipFormat} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setZipformat = function(value) {
  return jspb.Message.setProto3EnumField(this, 7, value);
};


/**
 * optional int32 zipCount = 8;
 * @return {number}
 */
proto.nebula.service.Predicate.prototype.getZipcount = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 8, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Predicate} returns this
 */
proto.nebula.service.Predicate.prototype.setZipcount = function(value) {
  return jspb.Message.setProto3IntField(this, 8, value);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.PredicateAnd.repeatedFields_ = [1];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.PredicateAnd.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.PredicateAnd.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.PredicateAnd} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PredicateAnd.toObject = function(includeInstance, msg) {
  var f, obj = {
    expressionList: jspb.Message.toObjectList(msg.getExpressionList(),
    proto.nebula.service.Predicate.toObject, includeInstance)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.PredicateAnd}
 */
proto.nebula.service.PredicateAnd.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.PredicateAnd;
  return proto.nebula.service.PredicateAnd.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.PredicateAnd} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.PredicateAnd}
 */
proto.nebula.service.PredicateAnd.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = new proto.nebula.service.Predicate;
      reader.readMessage(value,proto.nebula.service.Predicate.deserializeBinaryFromReader);
      msg.addExpression(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.PredicateAnd.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.PredicateAnd.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.PredicateAnd} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PredicateAnd.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getExpressionList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      1,
      f,
      proto.nebula.service.Predicate.serializeBinaryToWriter
    );
  }
};


/**
 * repeated Predicate expression = 1;
 * @return {!Array<!proto.nebula.service.Predicate>}
 */
proto.nebula.service.PredicateAnd.prototype.getExpressionList = function() {
  return /** @type{!Array<!proto.nebula.service.Predicate>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto.nebula.service.Predicate, 1));
};


/**
 * @param {!Array<!proto.nebula.service.Predicate>} value
 * @return {!proto.nebula.service.PredicateAnd} returns this
*/
proto.nebula.service.PredicateAnd.prototype.setExpressionList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 1, value);
};


/**
 * @param {!proto.nebula.service.Predicate=} opt_value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Predicate}
 */
proto.nebula.service.PredicateAnd.prototype.addExpression = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 1, opt_value, proto.nebula.service.Predicate, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.PredicateAnd} returns this
 */
proto.nebula.service.PredicateAnd.prototype.clearExpressionList = function() {
  return this.setExpressionList([]);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.PredicateOr.repeatedFields_ = [1];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.PredicateOr.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.PredicateOr.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.PredicateOr} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PredicateOr.toObject = function(includeInstance, msg) {
  var f, obj = {
    expressionList: jspb.Message.toObjectList(msg.getExpressionList(),
    proto.nebula.service.Predicate.toObject, includeInstance)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.PredicateOr}
 */
proto.nebula.service.PredicateOr.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.PredicateOr;
  return proto.nebula.service.PredicateOr.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.PredicateOr} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.PredicateOr}
 */
proto.nebula.service.PredicateOr.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = new proto.nebula.service.Predicate;
      reader.readMessage(value,proto.nebula.service.Predicate.deserializeBinaryFromReader);
      msg.addExpression(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.PredicateOr.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.PredicateOr.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.PredicateOr} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PredicateOr.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getExpressionList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      1,
      f,
      proto.nebula.service.Predicate.serializeBinaryToWriter
    );
  }
};


/**
 * repeated Predicate expression = 1;
 * @return {!Array<!proto.nebula.service.Predicate>}
 */
proto.nebula.service.PredicateOr.prototype.getExpressionList = function() {
  return /** @type{!Array<!proto.nebula.service.Predicate>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto.nebula.service.Predicate, 1));
};


/**
 * @param {!Array<!proto.nebula.service.Predicate>} value
 * @return {!proto.nebula.service.PredicateOr} returns this
*/
proto.nebula.service.PredicateOr.prototype.setExpressionList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 1, value);
};


/**
 * @param {!proto.nebula.service.Predicate=} opt_value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Predicate}
 */
proto.nebula.service.PredicateOr.prototype.addExpression = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 1, opt_value, proto.nebula.service.Predicate, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.PredicateOr} returns this
 */
proto.nebula.service.PredicateOr.prototype.clearExpressionList = function() {
  return this.setExpressionList([]);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.Metric.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.Metric.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.Metric} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Metric.toObject = function(includeInstance, msg) {
  var f, obj = {
    column: jspb.Message.getFieldWithDefault(msg, 1, ""),
    method: jspb.Message.getFieldWithDefault(msg, 2, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.Metric}
 */
proto.nebula.service.Metric.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.Metric;
  return proto.nebula.service.Metric.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.Metric} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.Metric}
 */
proto.nebula.service.Metric.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setColumn(value);
      break;
    case 2:
      var value = /** @type {!proto.nebula.service.Rollup} */ (reader.readEnum());
      msg.setMethod(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.Metric.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.Metric.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.Metric} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Metric.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getColumn();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getMethod();
  if (f !== 0.0) {
    writer.writeEnum(
      2,
      f
    );
  }
};


/**
 * optional string column = 1;
 * @return {string}
 */
proto.nebula.service.Metric.prototype.getColumn = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.Metric} returns this
 */
proto.nebula.service.Metric.prototype.setColumn = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional Rollup method = 2;
 * @return {!proto.nebula.service.Rollup}
 */
proto.nebula.service.Metric.prototype.getMethod = function() {
  return /** @type {!proto.nebula.service.Rollup} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {!proto.nebula.service.Rollup} value
 * @return {!proto.nebula.service.Metric} returns this
 */
proto.nebula.service.Metric.prototype.setMethod = function(value) {
  return jspb.Message.setProto3EnumField(this, 2, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.Order.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.Order.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.Order} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Order.toObject = function(includeInstance, msg) {
  var f, obj = {
    column: jspb.Message.getFieldWithDefault(msg, 1, ""),
    type: jspb.Message.getFieldWithDefault(msg, 2, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.Order}
 */
proto.nebula.service.Order.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.Order;
  return proto.nebula.service.Order.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.Order} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.Order}
 */
proto.nebula.service.Order.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setColumn(value);
      break;
    case 2:
      var value = /** @type {!proto.nebula.service.OrderType} */ (reader.readEnum());
      msg.setType(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.Order.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.Order.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.Order} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Order.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getColumn();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getType();
  if (f !== 0.0) {
    writer.writeEnum(
      2,
      f
    );
  }
};


/**
 * optional string column = 1;
 * @return {string}
 */
proto.nebula.service.Order.prototype.getColumn = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.Order} returns this
 */
proto.nebula.service.Order.prototype.setColumn = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional OrderType type = 2;
 * @return {!proto.nebula.service.OrderType}
 */
proto.nebula.service.Order.prototype.getType = function() {
  return /** @type {!proto.nebula.service.OrderType} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {!proto.nebula.service.OrderType} value
 * @return {!proto.nebula.service.Order} returns this
 */
proto.nebula.service.Order.prototype.setType = function(value) {
  return jspb.Message.setProto3EnumField(this, 2, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.CustomColumn.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.CustomColumn.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.CustomColumn} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.CustomColumn.toObject = function(includeInstance, msg) {
  var f, obj = {
    column: jspb.Message.getFieldWithDefault(msg, 1, ""),
    type: jspb.Message.getFieldWithDefault(msg, 2, 0),
    expr: jspb.Message.getFieldWithDefault(msg, 3, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.CustomColumn}
 */
proto.nebula.service.CustomColumn.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.CustomColumn;
  return proto.nebula.service.CustomColumn.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.CustomColumn} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.CustomColumn}
 */
proto.nebula.service.CustomColumn.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setColumn(value);
      break;
    case 2:
      var value = /** @type {!proto.nebula.service.CustomType} */ (reader.readEnum());
      msg.setType(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.setExpr(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.CustomColumn.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.CustomColumn.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.CustomColumn} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.CustomColumn.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getColumn();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getType();
  if (f !== 0.0) {
    writer.writeEnum(
      2,
      f
    );
  }
  f = message.getExpr();
  if (f.length > 0) {
    writer.writeString(
      3,
      f
    );
  }
};


/**
 * optional string column = 1;
 * @return {string}
 */
proto.nebula.service.CustomColumn.prototype.getColumn = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.CustomColumn} returns this
 */
proto.nebula.service.CustomColumn.prototype.setColumn = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional CustomType type = 2;
 * @return {!proto.nebula.service.CustomType}
 */
proto.nebula.service.CustomColumn.prototype.getType = function() {
  return /** @type {!proto.nebula.service.CustomType} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {!proto.nebula.service.CustomType} value
 * @return {!proto.nebula.service.CustomColumn} returns this
 */
proto.nebula.service.CustomColumn.prototype.setType = function(value) {
  return jspb.Message.setProto3EnumField(this, 2, value);
};


/**
 * optional string expr = 3;
 * @return {string}
 */
proto.nebula.service.CustomColumn.prototype.getExpr = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.CustomColumn} returns this
 */
proto.nebula.service.CustomColumn.prototype.setExpr = function(value) {
  return jspb.Message.setProto3StringField(this, 3, value);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.nebula.service.QueryRequest.repeatedFields_ = [7,8,12];

/**
 * Oneof group definitions for this message. Each group defines the field
 * numbers belonging to that group. When of these fields' value is set, all
 * other fields in the group are cleared. During deserialization, if multiple
 * fields are encountered for a group, only the last value seen will be kept.
 * @private {!Array<!Array<number>>}
 * @const
 */
proto.nebula.service.QueryRequest.oneofGroups_ = [[2,3]];

/**
 * @enum {number}
 */
proto.nebula.service.QueryRequest.FilterCase = {
  FILTER_NOT_SET: 0,
  FILTERA: 2,
  FILTERO: 3
};

/**
 * @return {proto.nebula.service.QueryRequest.FilterCase}
 */
proto.nebula.service.QueryRequest.prototype.getFilterCase = function() {
  return /** @type {proto.nebula.service.QueryRequest.FilterCase} */(jspb.Message.computeOneofCase(this, proto.nebula.service.QueryRequest.oneofGroups_[0]));
};



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.QueryRequest.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.QueryRequest.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.QueryRequest} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.QueryRequest.toObject = function(includeInstance, msg) {
  var f, obj = {
    table: jspb.Message.getFieldWithDefault(msg, 1, ""),
    filtera: (f = msg.getFiltera()) && proto.nebula.service.PredicateAnd.toObject(includeInstance, f),
    filtero: (f = msg.getFiltero()) && proto.nebula.service.PredicateOr.toObject(includeInstance, f),
    start: jspb.Message.getFieldWithDefault(msg, 4, 0),
    end: jspb.Message.getFieldWithDefault(msg, 5, 0),
    window: jspb.Message.getFieldWithDefault(msg, 6, 0),
    dimensionList: (f = jspb.Message.getRepeatedField(msg, 7)) == null ? undefined : f,
    metricList: jspb.Message.toObjectList(msg.getMetricList(),
    proto.nebula.service.Metric.toObject, includeInstance),
    top: jspb.Message.getFieldWithDefault(msg, 9, 0),
    order: (f = msg.getOrder()) && proto.nebula.service.Order.toObject(includeInstance, f),
    timeline: jspb.Message.getBooleanFieldWithDefault(msg, 11, false),
    customList: jspb.Message.toObjectList(msg.getCustomList(),
    proto.nebula.service.CustomColumn.toObject, includeInstance)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.QueryRequest}
 */
proto.nebula.service.QueryRequest.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.QueryRequest;
  return proto.nebula.service.QueryRequest.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.QueryRequest} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.QueryRequest}
 */
proto.nebula.service.QueryRequest.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setTable(value);
      break;
    case 2:
      var value = new proto.nebula.service.PredicateAnd;
      reader.readMessage(value,proto.nebula.service.PredicateAnd.deserializeBinaryFromReader);
      msg.setFiltera(value);
      break;
    case 3:
      var value = new proto.nebula.service.PredicateOr;
      reader.readMessage(value,proto.nebula.service.PredicateOr.deserializeBinaryFromReader);
      msg.setFiltero(value);
      break;
    case 4:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setStart(value);
      break;
    case 5:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setEnd(value);
      break;
    case 6:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setWindow(value);
      break;
    case 7:
      var value = /** @type {string} */ (reader.readString());
      msg.addDimension(value);
      break;
    case 8:
      var value = new proto.nebula.service.Metric;
      reader.readMessage(value,proto.nebula.service.Metric.deserializeBinaryFromReader);
      msg.addMetric(value);
      break;
    case 9:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setTop(value);
      break;
    case 10:
      var value = new proto.nebula.service.Order;
      reader.readMessage(value,proto.nebula.service.Order.deserializeBinaryFromReader);
      msg.setOrder(value);
      break;
    case 11:
      var value = /** @type {boolean} */ (reader.readBool());
      msg.setTimeline(value);
      break;
    case 12:
      var value = new proto.nebula.service.CustomColumn;
      reader.readMessage(value,proto.nebula.service.CustomColumn.deserializeBinaryFromReader);
      msg.addCustom(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.QueryRequest.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.QueryRequest.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.QueryRequest} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.QueryRequest.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getTable();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getFiltera();
  if (f != null) {
    writer.writeMessage(
      2,
      f,
      proto.nebula.service.PredicateAnd.serializeBinaryToWriter
    );
  }
  f = message.getFiltero();
  if (f != null) {
    writer.writeMessage(
      3,
      f,
      proto.nebula.service.PredicateOr.serializeBinaryToWriter
    );
  }
  f = message.getStart();
  if (f !== 0) {
    writer.writeUint64(
      4,
      f
    );
  }
  f = message.getEnd();
  if (f !== 0) {
    writer.writeUint64(
      5,
      f
    );
  }
  f = message.getWindow();
  if (f !== 0) {
    writer.writeUint32(
      6,
      f
    );
  }
  f = message.getDimensionList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      7,
      f
    );
  }
  f = message.getMetricList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      8,
      f,
      proto.nebula.service.Metric.serializeBinaryToWriter
    );
  }
  f = message.getTop();
  if (f !== 0) {
    writer.writeUint32(
      9,
      f
    );
  }
  f = message.getOrder();
  if (f != null) {
    writer.writeMessage(
      10,
      f,
      proto.nebula.service.Order.serializeBinaryToWriter
    );
  }
  f = message.getTimeline();
  if (f) {
    writer.writeBool(
      11,
      f
    );
  }
  f = message.getCustomList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      12,
      f,
      proto.nebula.service.CustomColumn.serializeBinaryToWriter
    );
  }
};


/**
 * optional string table = 1;
 * @return {string}
 */
proto.nebula.service.QueryRequest.prototype.getTable = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setTable = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional PredicateAnd filterA = 2;
 * @return {?proto.nebula.service.PredicateAnd}
 */
proto.nebula.service.QueryRequest.prototype.getFiltera = function() {
  return /** @type{?proto.nebula.service.PredicateAnd} */ (
    jspb.Message.getWrapperField(this, proto.nebula.service.PredicateAnd, 2));
};


/**
 * @param {?proto.nebula.service.PredicateAnd|undefined} value
 * @return {!proto.nebula.service.QueryRequest} returns this
*/
proto.nebula.service.QueryRequest.prototype.setFiltera = function(value) {
  return jspb.Message.setOneofWrapperField(this, 2, proto.nebula.service.QueryRequest.oneofGroups_[0], value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearFiltera = function() {
  return this.setFiltera(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.nebula.service.QueryRequest.prototype.hasFiltera = function() {
  return jspb.Message.getField(this, 2) != null;
};


/**
 * optional PredicateOr filterO = 3;
 * @return {?proto.nebula.service.PredicateOr}
 */
proto.nebula.service.QueryRequest.prototype.getFiltero = function() {
  return /** @type{?proto.nebula.service.PredicateOr} */ (
    jspb.Message.getWrapperField(this, proto.nebula.service.PredicateOr, 3));
};


/**
 * @param {?proto.nebula.service.PredicateOr|undefined} value
 * @return {!proto.nebula.service.QueryRequest} returns this
*/
proto.nebula.service.QueryRequest.prototype.setFiltero = function(value) {
  return jspb.Message.setOneofWrapperField(this, 3, proto.nebula.service.QueryRequest.oneofGroups_[0], value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearFiltero = function() {
  return this.setFiltero(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.nebula.service.QueryRequest.prototype.hasFiltero = function() {
  return jspb.Message.getField(this, 3) != null;
};


/**
 * optional uint64 start = 4;
 * @return {number}
 */
proto.nebula.service.QueryRequest.prototype.getStart = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 4, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setStart = function(value) {
  return jspb.Message.setProto3IntField(this, 4, value);
};


/**
 * optional uint64 end = 5;
 * @return {number}
 */
proto.nebula.service.QueryRequest.prototype.getEnd = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 5, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setEnd = function(value) {
  return jspb.Message.setProto3IntField(this, 5, value);
};


/**
 * optional uint32 window = 6;
 * @return {number}
 */
proto.nebula.service.QueryRequest.prototype.getWindow = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 6, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setWindow = function(value) {
  return jspb.Message.setProto3IntField(this, 6, value);
};


/**
 * repeated string dimension = 7;
 * @return {!Array<string>}
 */
proto.nebula.service.QueryRequest.prototype.getDimensionList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 7));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setDimensionList = function(value) {
  return jspb.Message.setField(this, 7, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.addDimension = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 7, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearDimensionList = function() {
  return this.setDimensionList([]);
};


/**
 * repeated Metric metric = 8;
 * @return {!Array<!proto.nebula.service.Metric>}
 */
proto.nebula.service.QueryRequest.prototype.getMetricList = function() {
  return /** @type{!Array<!proto.nebula.service.Metric>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto.nebula.service.Metric, 8));
};


/**
 * @param {!Array<!proto.nebula.service.Metric>} value
 * @return {!proto.nebula.service.QueryRequest} returns this
*/
proto.nebula.service.QueryRequest.prototype.setMetricList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 8, value);
};


/**
 * @param {!proto.nebula.service.Metric=} opt_value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.Metric}
 */
proto.nebula.service.QueryRequest.prototype.addMetric = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 8, opt_value, proto.nebula.service.Metric, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearMetricList = function() {
  return this.setMetricList([]);
};


/**
 * optional uint32 top = 9;
 * @return {number}
 */
proto.nebula.service.QueryRequest.prototype.getTop = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 9, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setTop = function(value) {
  return jspb.Message.setProto3IntField(this, 9, value);
};


/**
 * optional Order order = 10;
 * @return {?proto.nebula.service.Order}
 */
proto.nebula.service.QueryRequest.prototype.getOrder = function() {
  return /** @type{?proto.nebula.service.Order} */ (
    jspb.Message.getWrapperField(this, proto.nebula.service.Order, 10));
};


/**
 * @param {?proto.nebula.service.Order|undefined} value
 * @return {!proto.nebula.service.QueryRequest} returns this
*/
proto.nebula.service.QueryRequest.prototype.setOrder = function(value) {
  return jspb.Message.setWrapperField(this, 10, value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearOrder = function() {
  return this.setOrder(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.nebula.service.QueryRequest.prototype.hasOrder = function() {
  return jspb.Message.getField(this, 10) != null;
};


/**
 * optional bool timeline = 11;
 * @return {boolean}
 */
proto.nebula.service.QueryRequest.prototype.getTimeline = function() {
  return /** @type {boolean} */ (jspb.Message.getBooleanFieldWithDefault(this, 11, false));
};


/**
 * @param {boolean} value
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.setTimeline = function(value) {
  return jspb.Message.setProto3BooleanField(this, 11, value);
};


/**
 * repeated CustomColumn custom = 12;
 * @return {!Array<!proto.nebula.service.CustomColumn>}
 */
proto.nebula.service.QueryRequest.prototype.getCustomList = function() {
  return /** @type{!Array<!proto.nebula.service.CustomColumn>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto.nebula.service.CustomColumn, 12));
};


/**
 * @param {!Array<!proto.nebula.service.CustomColumn>} value
 * @return {!proto.nebula.service.QueryRequest} returns this
*/
proto.nebula.service.QueryRequest.prototype.setCustomList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 12, value);
};


/**
 * @param {!proto.nebula.service.CustomColumn=} opt_value
 * @param {number=} opt_index
 * @return {!proto.nebula.service.CustomColumn}
 */
proto.nebula.service.QueryRequest.prototype.addCustom = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 12, opt_value, proto.nebula.service.CustomColumn, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.nebula.service.QueryRequest} returns this
 */
proto.nebula.service.QueryRequest.prototype.clearCustomList = function() {
  return this.setCustomList([]);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.Statistics.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.Statistics.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.Statistics} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Statistics.toObject = function(includeInstance, msg) {
  var f, obj = {
    querytimems: jspb.Message.getFieldWithDefault(msg, 1, 0),
    rowsscanned: jspb.Message.getFieldWithDefault(msg, 2, 0),
    blocksscanned: jspb.Message.getFieldWithDefault(msg, 3, 0),
    rowsreturn: jspb.Message.getFieldWithDefault(msg, 4, 0),
    error: jspb.Message.getFieldWithDefault(msg, 5, 0),
    message: jspb.Message.getFieldWithDefault(msg, 6, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.Statistics}
 */
proto.nebula.service.Statistics.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.Statistics;
  return proto.nebula.service.Statistics.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.Statistics} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.Statistics}
 */
proto.nebula.service.Statistics.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setQuerytimems(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setRowsscanned(value);
      break;
    case 3:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setBlocksscanned(value);
      break;
    case 4:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setRowsreturn(value);
      break;
    case 5:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setError(value);
      break;
    case 6:
      var value = /** @type {string} */ (reader.readString());
      msg.setMessage(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.Statistics.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.Statistics.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.Statistics} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.Statistics.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getQuerytimems();
  if (f !== 0) {
    writer.writeUint32(
      1,
      f
    );
  }
  f = message.getRowsscanned();
  if (f !== 0) {
    writer.writeUint64(
      2,
      f
    );
  }
  f = message.getBlocksscanned();
  if (f !== 0) {
    writer.writeUint64(
      3,
      f
    );
  }
  f = message.getRowsreturn();
  if (f !== 0) {
    writer.writeUint64(
      4,
      f
    );
  }
  f = message.getError();
  if (f !== 0) {
    writer.writeUint32(
      5,
      f
    );
  }
  f = message.getMessage();
  if (f.length > 0) {
    writer.writeString(
      6,
      f
    );
  }
};


/**
 * optional uint32 queryTimeMs = 1;
 * @return {number}
 */
proto.nebula.service.Statistics.prototype.getQuerytimems = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setQuerytimems = function(value) {
  return jspb.Message.setProto3IntField(this, 1, value);
};


/**
 * optional uint64 rowsScanned = 2;
 * @return {number}
 */
proto.nebula.service.Statistics.prototype.getRowsscanned = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setRowsscanned = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * optional uint64 blocksScanned = 3;
 * @return {number}
 */
proto.nebula.service.Statistics.prototype.getBlocksscanned = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 3, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setBlocksscanned = function(value) {
  return jspb.Message.setProto3IntField(this, 3, value);
};


/**
 * optional uint64 rowsReturn = 4;
 * @return {number}
 */
proto.nebula.service.Statistics.prototype.getRowsreturn = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 4, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setRowsreturn = function(value) {
  return jspb.Message.setProto3IntField(this, 4, value);
};


/**
 * optional uint32 error = 5;
 * @return {number}
 */
proto.nebula.service.Statistics.prototype.getError = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 5, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setError = function(value) {
  return jspb.Message.setProto3IntField(this, 5, value);
};


/**
 * optional string message = 6;
 * @return {string}
 */
proto.nebula.service.Statistics.prototype.getMessage = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 6, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.Statistics} returns this
 */
proto.nebula.service.Statistics.prototype.setMessage = function(value) {
  return jspb.Message.setProto3StringField(this, 6, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.QueryResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.QueryResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.QueryResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.QueryResponse.toObject = function(includeInstance, msg) {
  var f, obj = {
    stats: (f = msg.getStats()) && proto.nebula.service.Statistics.toObject(includeInstance, f),
    type: jspb.Message.getFieldWithDefault(msg, 2, 0),
    data: msg.getData_asB64()
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.QueryResponse}
 */
proto.nebula.service.QueryResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.QueryResponse;
  return proto.nebula.service.QueryResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.QueryResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.QueryResponse}
 */
proto.nebula.service.QueryResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = new proto.nebula.service.Statistics;
      reader.readMessage(value,proto.nebula.service.Statistics.deserializeBinaryFromReader);
      msg.setStats(value);
      break;
    case 2:
      var value = /** @type {!proto.nebula.service.DataType} */ (reader.readEnum());
      msg.setType(value);
      break;
    case 3:
      var value = /** @type {!Uint8Array} */ (reader.readBytes());
      msg.setData(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.QueryResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.QueryResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.QueryResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.QueryResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getStats();
  if (f != null) {
    writer.writeMessage(
      1,
      f,
      proto.nebula.service.Statistics.serializeBinaryToWriter
    );
  }
  f = message.getType();
  if (f !== 0.0) {
    writer.writeEnum(
      2,
      f
    );
  }
  f = message.getData_asU8();
  if (f.length > 0) {
    writer.writeBytes(
      3,
      f
    );
  }
};


/**
 * optional Statistics stats = 1;
 * @return {?proto.nebula.service.Statistics}
 */
proto.nebula.service.QueryResponse.prototype.getStats = function() {
  return /** @type{?proto.nebula.service.Statistics} */ (
    jspb.Message.getWrapperField(this, proto.nebula.service.Statistics, 1));
};


/**
 * @param {?proto.nebula.service.Statistics|undefined} value
 * @return {!proto.nebula.service.QueryResponse} returns this
*/
proto.nebula.service.QueryResponse.prototype.setStats = function(value) {
  return jspb.Message.setWrapperField(this, 1, value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.nebula.service.QueryResponse} returns this
 */
proto.nebula.service.QueryResponse.prototype.clearStats = function() {
  return this.setStats(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.nebula.service.QueryResponse.prototype.hasStats = function() {
  return jspb.Message.getField(this, 1) != null;
};


/**
 * optional DataType type = 2;
 * @return {!proto.nebula.service.DataType}
 */
proto.nebula.service.QueryResponse.prototype.getType = function() {
  return /** @type {!proto.nebula.service.DataType} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {!proto.nebula.service.DataType} value
 * @return {!proto.nebula.service.QueryResponse} returns this
 */
proto.nebula.service.QueryResponse.prototype.setType = function(value) {
  return jspb.Message.setProto3EnumField(this, 2, value);
};


/**
 * optional bytes data = 3;
 * @return {!(string|Uint8Array)}
 */
proto.nebula.service.QueryResponse.prototype.getData = function() {
  return /** @type {!(string|Uint8Array)} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * optional bytes data = 3;
 * This is a type-conversion wrapper around `getData()`
 * @return {string}
 */
proto.nebula.service.QueryResponse.prototype.getData_asB64 = function() {
  return /** @type {string} */ (jspb.Message.bytesAsB64(
      this.getData()));
};


/**
 * optional bytes data = 3;
 * Note that Uint8Array is not supported on all browsers.
 * @see http://caniuse.com/Uint8Array
 * This is a type-conversion wrapper around `getData()`
 * @return {!Uint8Array}
 */
proto.nebula.service.QueryResponse.prototype.getData_asU8 = function() {
  return /** @type {!Uint8Array} */ (jspb.Message.bytesAsU8(
      this.getData()));
};


/**
 * @param {!(string|Uint8Array)} value
 * @return {!proto.nebula.service.QueryResponse} returns this
 */
proto.nebula.service.QueryResponse.prototype.setData = function(value) {
  return jspb.Message.setProto3BytesField(this, 3, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.LoadRequest.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.LoadRequest.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.LoadRequest} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.LoadRequest.toObject = function(includeInstance, msg) {
  var f, obj = {
    type: jspb.Message.getFieldWithDefault(msg, 1, 0),
    table: jspb.Message.getFieldWithDefault(msg, 2, ""),
    json: jspb.Message.getFieldWithDefault(msg, 3, ""),
    ttl: jspb.Message.getFieldWithDefault(msg, 4, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.LoadRequest}
 */
proto.nebula.service.LoadRequest.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.LoadRequest;
  return proto.nebula.service.LoadRequest.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.LoadRequest} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.LoadRequest}
 */
proto.nebula.service.LoadRequest.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {!proto.nebula.service.LoadType} */ (reader.readEnum());
      msg.setType(value);
      break;
    case 2:
      var value = /** @type {string} */ (reader.readString());
      msg.setTable(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.setJson(value);
      break;
    case 4:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setTtl(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.LoadRequest.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.LoadRequest.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.LoadRequest} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.LoadRequest.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getType();
  if (f !== 0.0) {
    writer.writeEnum(
      1,
      f
    );
  }
  f = message.getTable();
  if (f.length > 0) {
    writer.writeString(
      2,
      f
    );
  }
  f = message.getJson();
  if (f.length > 0) {
    writer.writeString(
      3,
      f
    );
  }
  f = message.getTtl();
  if (f !== 0) {
    writer.writeUint32(
      4,
      f
    );
  }
};


/**
 * optional LoadType type = 1;
 * @return {!proto.nebula.service.LoadType}
 */
proto.nebula.service.LoadRequest.prototype.getType = function() {
  return /** @type {!proto.nebula.service.LoadType} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {!proto.nebula.service.LoadType} value
 * @return {!proto.nebula.service.LoadRequest} returns this
 */
proto.nebula.service.LoadRequest.prototype.setType = function(value) {
  return jspb.Message.setProto3EnumField(this, 1, value);
};


/**
 * optional string table = 2;
 * @return {string}
 */
proto.nebula.service.LoadRequest.prototype.getTable = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 2, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.LoadRequest} returns this
 */
proto.nebula.service.LoadRequest.prototype.setTable = function(value) {
  return jspb.Message.setProto3StringField(this, 2, value);
};


/**
 * optional string json = 3;
 * @return {string}
 */
proto.nebula.service.LoadRequest.prototype.getJson = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.LoadRequest} returns this
 */
proto.nebula.service.LoadRequest.prototype.setJson = function(value) {
  return jspb.Message.setProto3StringField(this, 3, value);
};


/**
 * optional uint32 ttl = 4;
 * @return {number}
 */
proto.nebula.service.LoadRequest.prototype.getTtl = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 4, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.LoadRequest} returns this
 */
proto.nebula.service.LoadRequest.prototype.setTtl = function(value) {
  return jspb.Message.setProto3IntField(this, 4, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.LoadResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.LoadResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.LoadResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.LoadResponse.toObject = function(includeInstance, msg) {
  var f, obj = {
    error: jspb.Message.getFieldWithDefault(msg, 1, 0),
    loadtimems: jspb.Message.getFieldWithDefault(msg, 2, 0),
    table: jspb.Message.getFieldWithDefault(msg, 3, ""),
    message: jspb.Message.getFieldWithDefault(msg, 4, "")
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.LoadResponse}
 */
proto.nebula.service.LoadResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.LoadResponse;
  return proto.nebula.service.LoadResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.LoadResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.LoadResponse}
 */
proto.nebula.service.LoadResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {!proto.nebula.service.LoadError} */ (reader.readEnum());
      msg.setError(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setLoadtimems(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.setTable(value);
      break;
    case 4:
      var value = /** @type {string} */ (reader.readString());
      msg.setMessage(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.LoadResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.LoadResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.LoadResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.LoadResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getError();
  if (f !== 0.0) {
    writer.writeEnum(
      1,
      f
    );
  }
  f = message.getLoadtimems();
  if (f !== 0) {
    writer.writeUint32(
      2,
      f
    );
  }
  f = message.getTable();
  if (f.length > 0) {
    writer.writeString(
      3,
      f
    );
  }
  f = message.getMessage();
  if (f.length > 0) {
    writer.writeString(
      4,
      f
    );
  }
};


/**
 * optional LoadError error = 1;
 * @return {!proto.nebula.service.LoadError}
 */
proto.nebula.service.LoadResponse.prototype.getError = function() {
  return /** @type {!proto.nebula.service.LoadError} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {!proto.nebula.service.LoadError} value
 * @return {!proto.nebula.service.LoadResponse} returns this
 */
proto.nebula.service.LoadResponse.prototype.setError = function(value) {
  return jspb.Message.setProto3EnumField(this, 1, value);
};


/**
 * optional uint32 loadTimeMs = 2;
 * @return {number}
 */
proto.nebula.service.LoadResponse.prototype.getLoadtimems = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.LoadResponse} returns this
 */
proto.nebula.service.LoadResponse.prototype.setLoadtimems = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * optional string table = 3;
 * @return {string}
 */
proto.nebula.service.LoadResponse.prototype.getTable = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.LoadResponse} returns this
 */
proto.nebula.service.LoadResponse.prototype.setTable = function(value) {
  return jspb.Message.setProto3StringField(this, 3, value);
};


/**
 * optional string message = 4;
 * @return {string}
 */
proto.nebula.service.LoadResponse.prototype.getMessage = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 4, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.LoadResponse} returns this
 */
proto.nebula.service.LoadResponse.prototype.setMessage = function(value) {
  return jspb.Message.setProto3StringField(this, 4, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.ServiceInfo.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.ServiceInfo.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.ServiceInfo} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.ServiceInfo.toObject = function(includeInstance, msg) {
  var f, obj = {
    ipv4: jspb.Message.getFieldWithDefault(msg, 1, ""),
    port: jspb.Message.getFieldWithDefault(msg, 2, 0),
    host: jspb.Message.getFieldWithDefault(msg, 3, ""),
    tier: jspb.Message.getFieldWithDefault(msg, 4, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.ServiceInfo}
 */
proto.nebula.service.ServiceInfo.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.ServiceInfo;
  return proto.nebula.service.ServiceInfo.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.ServiceInfo} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.ServiceInfo}
 */
proto.nebula.service.ServiceInfo.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setIpv4(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readUint32());
      msg.setPort(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.setHost(value);
      break;
    case 4:
      var value = /** @type {!proto.nebula.service.ServiceTier} */ (reader.readEnum());
      msg.setTier(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.ServiceInfo.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.ServiceInfo.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.ServiceInfo} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.ServiceInfo.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getIpv4();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getPort();
  if (f !== 0) {
    writer.writeUint32(
      2,
      f
    );
  }
  f = message.getHost();
  if (f.length > 0) {
    writer.writeString(
      3,
      f
    );
  }
  f = message.getTier();
  if (f !== 0.0) {
    writer.writeEnum(
      4,
      f
    );
  }
};


/**
 * optional string ipv4 = 1;
 * @return {string}
 */
proto.nebula.service.ServiceInfo.prototype.getIpv4 = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.ServiceInfo} returns this
 */
proto.nebula.service.ServiceInfo.prototype.setIpv4 = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional uint32 port = 2;
 * @return {number}
 */
proto.nebula.service.ServiceInfo.prototype.getPort = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.nebula.service.ServiceInfo} returns this
 */
proto.nebula.service.ServiceInfo.prototype.setPort = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * optional string host = 3;
 * @return {string}
 */
proto.nebula.service.ServiceInfo.prototype.getHost = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * @param {string} value
 * @return {!proto.nebula.service.ServiceInfo} returns this
 */
proto.nebula.service.ServiceInfo.prototype.setHost = function(value) {
  return jspb.Message.setProto3StringField(this, 3, value);
};


/**
 * optional ServiceTier tier = 4;
 * @return {!proto.nebula.service.ServiceTier}
 */
proto.nebula.service.ServiceInfo.prototype.getTier = function() {
  return /** @type {!proto.nebula.service.ServiceTier} */ (jspb.Message.getFieldWithDefault(this, 4, 0));
};


/**
 * @param {!proto.nebula.service.ServiceTier} value
 * @return {!proto.nebula.service.ServiceInfo} returns this
 */
proto.nebula.service.ServiceInfo.prototype.setTier = function(value) {
  return jspb.Message.setProto3EnumField(this, 4, value);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.nebula.service.PingResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.nebula.service.PingResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.nebula.service.PingResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PingResponse.toObject = function(includeInstance, msg) {
  var f, obj = {

  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.nebula.service.PingResponse}
 */
proto.nebula.service.PingResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.nebula.service.PingResponse;
  return proto.nebula.service.PingResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.nebula.service.PingResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.nebula.service.PingResponse}
 */
proto.nebula.service.PingResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.nebula.service.PingResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.nebula.service.PingResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.nebula.service.PingResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.nebula.service.PingResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
};


/**
 * @enum {number}
 */
proto.nebula.service.Operation = {
  EQ: 0,
  NEQ: 1,
  MORE: 2,
  LESS: 3,
  LIKE: 4,
  ILIKE: 5
};

/**
 * @enum {number}
 */
proto.nebula.service.ZipFormat = {
  DELTA: 0
};

/**
 * @enum {number}
 */
proto.nebula.service.Rollup = {
  COUNT: 0,
  SUM: 1,
  MIN: 2,
  MAX: 3,
  AVG: 4,
  P10: 5,
  P25: 6,
  P50: 7,
  P75: 8,
  P90: 9,
  P99: 10,
  P99_9: 11,
  P99_99: 12,
  TREEMERGE: 13,
  CARD_EST: 14
};

/**
 * @enum {number}
 */
proto.nebula.service.OrderType = {
  ASC: 0,
  DESC: 1,
  NONE: 2
};

/**
 * @enum {number}
 */
proto.nebula.service.CustomType = {
  INT: 0,
  LONG: 1,
  FLOAT: 2,
  DOUBLE: 3,
  STRING: 4
};

/**
 * @enum {number}
 */
proto.nebula.service.DataType = {
  NATIVE: 0,
  JSON: 1
};

/**
 * @enum {number}
 */
proto.nebula.service.LoadType = {
  CONFIG: 0,
  GOOGLE_SHEET: 1,
  DEMAND: 2
};

/**
 * @enum {number}
 */
proto.nebula.service.LoadError = {
  SUCCESS: 0,
  TEMPLATE_NOT_FOUND: 1,
  MISSING_PARAM: 2,
  MISSING_BUCKET_VALUE: 3,
  ANAUTHORIZED: 4,
  EMPTY_RESULT: 5,
  NOT_SUPPORTED: 6
};

/**
 * @enum {number}
 */
proto.nebula.service.ServiceTier = {
  WEB: 0,
  SERVER: 1,
  NODE: 2
};

goog.object.extend(exports, proto.nebula.service);


/***/ }),
/* 1 */
/***/ (function(module, exports) {

module.exports = require("grpc");

/***/ }),
/* 2 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
// GENERATED CODE -- DO NOT EDIT!

// Original file comments:
//
// Copyright 2017-present Shawn Cao
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

var grpc = __webpack_require__(1);
var nebula_pb = __webpack_require__(0);

function serialize_nebula_service_EchoRequest(arg) {
  if (!(arg instanceof nebula_pb.EchoRequest)) {
    throw new Error('Expected argument of type nebula.service.EchoRequest');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_EchoRequest(buffer_arg) {
  return nebula_pb.EchoRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_EchoResponse(arg) {
  if (!(arg instanceof nebula_pb.EchoResponse)) {
    throw new Error('Expected argument of type nebula.service.EchoResponse');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_EchoResponse(buffer_arg) {
  return nebula_pb.EchoResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_ListTables(arg) {
  if (!(arg instanceof nebula_pb.ListTables)) {
    throw new Error('Expected argument of type nebula.service.ListTables');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_ListTables(buffer_arg) {
  return nebula_pb.ListTables.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_LoadRequest(arg) {
  if (!(arg instanceof nebula_pb.LoadRequest)) {
    throw new Error('Expected argument of type nebula.service.LoadRequest');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_LoadRequest(buffer_arg) {
  return nebula_pb.LoadRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_LoadResponse(arg) {
  if (!(arg instanceof nebula_pb.LoadResponse)) {
    throw new Error('Expected argument of type nebula.service.LoadResponse');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_LoadResponse(buffer_arg) {
  return nebula_pb.LoadResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_PingResponse(arg) {
  if (!(arg instanceof nebula_pb.PingResponse)) {
    throw new Error('Expected argument of type nebula.service.PingResponse');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_PingResponse(buffer_arg) {
  return nebula_pb.PingResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_QueryRequest(arg) {
  if (!(arg instanceof nebula_pb.QueryRequest)) {
    throw new Error('Expected argument of type nebula.service.QueryRequest');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_QueryRequest(buffer_arg) {
  return nebula_pb.QueryRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_QueryResponse(arg) {
  if (!(arg instanceof nebula_pb.QueryResponse)) {
    throw new Error('Expected argument of type nebula.service.QueryResponse');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_QueryResponse(buffer_arg) {
  return nebula_pb.QueryResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_ServiceInfo(arg) {
  if (!(arg instanceof nebula_pb.ServiceInfo)) {
    throw new Error('Expected argument of type nebula.service.ServiceInfo');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_ServiceInfo(buffer_arg) {
  return nebula_pb.ServiceInfo.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_TableList(arg) {
  if (!(arg instanceof nebula_pb.TableList)) {
    throw new Error('Expected argument of type nebula.service.TableList');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_TableList(buffer_arg) {
  return nebula_pb.TableList.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_TableStateRequest(arg) {
  if (!(arg instanceof nebula_pb.TableStateRequest)) {
    throw new Error('Expected argument of type nebula.service.TableStateRequest');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_TableStateRequest(buffer_arg) {
  return nebula_pb.TableStateRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_TableStateResponse(arg) {
  if (!(arg instanceof nebula_pb.TableStateResponse)) {
    throw new Error('Expected argument of type nebula.service.TableStateResponse');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_TableStateResponse(buffer_arg) {
  return nebula_pb.TableStateResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_nebula_service_UrlData(arg) {
  if (!(arg instanceof nebula_pb.UrlData)) {
    throw new Error('Expected argument of type nebula.service.UrlData');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_nebula_service_UrlData(buffer_arg) {
  return nebula_pb.UrlData.deserializeBinary(new Uint8Array(buffer_arg));
}


// all service methods are captilized compared to other regular methods
var V1Service = exports.V1Service = {
  // Sends a greeting
  echo: {
    path: '/nebula.service.V1/Echo',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.EchoRequest,
    responseType: nebula_pb.EchoResponse,
    requestSerialize: serialize_nebula_service_EchoRequest,
    requestDeserialize: deserialize_nebula_service_EchoRequest,
    responseSerialize: serialize_nebula_service_EchoResponse,
    responseDeserialize: deserialize_nebula_service_EchoResponse,
  },
  // Get all available data sources
  tables: {
    path: '/nebula.service.V1/Tables',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.ListTables,
    responseType: nebula_pb.TableList,
    requestSerialize: serialize_nebula_service_ListTables,
    requestDeserialize: deserialize_nebula_service_ListTables,
    responseSerialize: serialize_nebula_service_TableList,
    responseDeserialize: deserialize_nebula_service_TableList,
  },
  // Get table state for given table
  state: {
    path: '/nebula.service.V1/State',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.TableStateRequest,
    responseType: nebula_pb.TableStateResponse,
    requestSerialize: serialize_nebula_service_TableStateRequest,
    requestDeserialize: deserialize_nebula_service_TableStateRequest,
    responseSerialize: serialize_nebula_service_TableStateResponse,
    responseDeserialize: deserialize_nebula_service_TableStateResponse,
  },
  // Query Nebula to get result
  query: {
    path: '/nebula.service.V1/Query',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.QueryRequest,
    responseType: nebula_pb.QueryResponse,
    requestSerialize: serialize_nebula_service_QueryRequest,
    requestDeserialize: deserialize_nebula_service_QueryRequest,
    responseSerialize: serialize_nebula_service_QueryResponse,
    responseDeserialize: deserialize_nebula_service_QueryResponse,
  },
  // on demand loading specified data with parameters
  // template defined in cluster configuration
  load: {
    path: '/nebula.service.V1/Load',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.LoadRequest,
    responseType: nebula_pb.LoadResponse,
    requestSerialize: serialize_nebula_service_LoadRequest,
    requestDeserialize: deserialize_nebula_service_LoadRequest,
    responseSerialize: serialize_nebula_service_LoadResponse,
    responseDeserialize: deserialize_nebula_service_LoadResponse,
  },
  // shut down all work nodes - used for perf profiling
  nuclear: {
    path: '/nebula.service.V1/Nuclear',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.EchoRequest,
    responseType: nebula_pb.EchoResponse,
    requestSerialize: serialize_nebula_service_EchoRequest,
    requestDeserialize: deserialize_nebula_service_EchoRequest,
    responseSerialize: serialize_nebula_service_EchoResponse,
    responseDeserialize: deserialize_nebula_service_EchoResponse,
  },
  // URL service - code for raw or raw for code
  url: {
    path: '/nebula.service.V1/Url',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.UrlData,
    responseType: nebula_pb.UrlData,
    requestSerialize: serialize_nebula_service_UrlData,
    requestDeserialize: deserialize_nebula_service_UrlData,
    responseSerialize: serialize_nebula_service_UrlData,
    responseDeserialize: deserialize_nebula_service_UrlData,
  },
  // Discovery service - register node
  ping: {
    path: '/nebula.service.V1/Ping',
    requestStream: false,
    responseStream: false,
    requestType: nebula_pb.ServiceInfo,
    responseType: nebula_pb.PingResponse,
    requestSerialize: serialize_nebula_service_ServiceInfo,
    requestDeserialize: deserialize_nebula_service_ServiceInfo,
    responseSerialize: serialize_nebula_service_PingResponse,
    responseDeserialize: deserialize_nebula_service_PingResponse,
  },
};

exports.V1Client = grpc.makeGenericClientConstructor(V1Service);


/***/ }),
/* 3 */
/***/ (function(module, exports) {

module.exports = require("serve-static");

/***/ }),
/* 4 */
/***/ (function(module, exports) {

module.exports = require("finalhandler");

/***/ }),
/* 5 */
/***/ (function(module, exports) {

module.exports = require("json-bigint");

/***/ }),
/* 6 */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
__webpack_require__.r(__webpack_exports__);
/* harmony import */ var nebula_pb__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(0);
/* harmony import */ var nebula_pb__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(nebula_pb__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var nebula_node_rpc__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(2);
/* harmony import */ var nebula_node_rpc__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(nebula_node_rpc__WEBPACK_IMPORTED_MODULE_1__);
/* harmony import */ var serve_static__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3);
/* harmony import */ var serve_static__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(serve_static__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var finalhandler__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(4);
/* harmony import */ var finalhandler__WEBPACK_IMPORTED_MODULE_3___default = /*#__PURE__*/__webpack_require__.n(finalhandler__WEBPACK_IMPORTED_MODULE_3__);
/* harmony import */ var grpc__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(1);
/* harmony import */ var grpc__WEBPACK_IMPORTED_MODULE_4___default = /*#__PURE__*/__webpack_require__.n(grpc__WEBPACK_IMPORTED_MODULE_4__);
/* harmony import */ var json_bigint__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(5);
/* harmony import */ var json_bigint__WEBPACK_IMPORTED_MODULE_5___default = /*#__PURE__*/__webpack_require__.n(json_bigint__WEBPACK_IMPORTED_MODULE_5__);









const qc = (service) => {
    // set the maximum message size as 20MB
    return new nebula_node_rpc__WEBPACK_IMPORTED_MODULE_1__["V1Client"](service, grpc__WEBPACK_IMPORTED_MODULE_4___default.a.credentials.createInsecure(), {
        "nebula": "node",
        'grpc.max_receive_message_length': 64 * 1024 * 1024,
        'grpc.max_send_message_length': 64 * 1024 * 1024
    });
};

// static handler
// serving static files from current folder
const shandler = serve_static__WEBPACK_IMPORTED_MODULE_2___default()(".", {
    'index': ['index.html'],
    'dotfiles': 'deny',
    'fallthrough': false,
    'immutable': true
});

const static_res = (req, res) => {
    shandler(req, res, finalhandler__WEBPACK_IMPORTED_MODULE_3___default()(req, res));
};

/* harmony default export */ __webpack_exports__["default"] = ({
    V1Client: nebula_node_rpc__WEBPACK_IMPORTED_MODULE_1__["V1Client"],
    EchoRequest: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["EchoRequest"],
    EchoResponse: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["EchoResponse"],
    ListTables: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["ListTables"],
    TableStateRequest: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["TableStateRequest"],
    TableStateResponse: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["TableStateResponse"],
    Operation: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Operation"],
    Predicate: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Predicate"],
    PredicateAnd: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["PredicateAnd"],
    PredicateOr: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["PredicateOr"],
    Rollup: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Rollup"],
    Metric: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Metric"],
    Order: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Order"],
    OrderType: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["OrderType"],
    CustomType: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["CustomType"],
    CustomColumn: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["CustomColumn"],
    QueryRequest: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["QueryRequest"],
    Statistics: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["Statistics"],
    DataType: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["DataType"],
    QueryResponse: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["QueryResponse"],
    LoadError: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["LoadError"],
    LoadType: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["LoadType"],
    LoadRequest: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["LoadRequest"],
    LoadResponse: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["LoadResponse"],
    UrlData: nebula_pb__WEBPACK_IMPORTED_MODULE_0__["UrlData"],
    static_res,
    qc,
    grpc: (grpc__WEBPACK_IMPORTED_MODULE_4___default()),
    jsonb: (json_bigint__WEBPACK_IMPORTED_MODULE_5___default())
});

/***/ }),
/* 7 */
/***/ (function(module, exports) {

module.exports = require("google-protobuf");

/***/ })
/******/ ])["default"]));