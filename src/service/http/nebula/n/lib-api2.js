// unlike lib-api, lib-api2 will not export grpc or other dependencies
// lib-api2 will only export the service interface, which is the same as lib-api

import {
  EchoRequest,
  EchoResponse,
  ListTables,
  TableStateRequest,
  TableStateResponse,
  Operation,
  Predicate,
  PredicateAnd,
  PredicateOr,
  Rollup,
  Metric,
  Order,
  OrderType,
  CustomType,
  CustomColumn,
  QueryRequest,
  Statistics,
  DataType,
  QueryResponse,
  LoadError,
  LoadType,
  LoadRequest,
  LoadResponse,
  UrlData
} from 'nebula-pb';

import {
  V1Client
} from 'nebula-node-rpc';

export default {
  // nebula server
  V1Client,
  EchoRequest,
  EchoResponse,
  ListTables,
  TableStateRequest,
  TableStateResponse,
  Operation,
  Predicate,
  PredicateAnd,
  PredicateOr,
  Rollup,
  Metric,
  Order,
  OrderType,
  CustomType,
  CustomColumn,
  QueryRequest,
  Statistics,
  DataType,
  QueryResponse,
  LoadError,
  LoadType,
  LoadRequest,
  LoadResponse,
  UrlData,
};