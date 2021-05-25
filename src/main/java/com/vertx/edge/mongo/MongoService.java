/*
 * Vert.x Edge, open source.
 * Copyright (C) 2020-2021 Vert.x Edge
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.vertx.edge.mongo;

import com.vertx.edge.annotations.ServiceProvider;
import com.vertx.edge.deploy.service.RecordService;
import com.vertx.edge.deploy.service.secret.Secret;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.MongoDataSource;
import lombok.extern.log4j.Log4j2;

/**
 * @author Luiz Schmidt
 */
@Log4j2
@ServiceProvider(name = MongoService.SERVICE)
public class MongoService implements RecordService {

  private static final long DEFAULT_CONNECTION_TIMEOUT = 1000;
  private static final long DEFAULT_SELECTIONDB_TIMEOUT = 5000;
  private static final long DEFAULT_READ_TIMEOUT = 5000;

  public static final String SERVICE = "mongo-service";

  public static Future<MongoClient> mongo(ServiceDiscovery discovery) {
    Promise<MongoClient> promise = Promise.promise();

    MongoDataSource.getMongoClient(discovery, new JsonObject().put("name", SERVICE)).onSuccess(promise::complete)
        .onFailure(cause -> promise.fail(RecordService.buildErrorMessage(SERVICE, cause)));

    return promise.future();
  }

  public Future<Record> newRecord(Vertx vertx, JsonObject config) {
    Promise<Record> promise = Promise.promise();
    buildMongoOptions(vertx, config)
        .onSuccess(opts -> promise.complete(MongoDataSource.createRecord(SERVICE, new JsonObject(), opts)))
        .onFailure(promise::fail);
    return promise.future();
  }

  private static Future<JsonObject> buildMongoOptions(Vertx vertx, JsonObject config) {
    if (config.containsKey("connection_string"))
      log.warn("If the connection string is used the mongo client will ignore any driver configuration options.");

    config.put("serverSelectionTimeoutMS", config.getLong("serverSelectionTimeoutMS", DEFAULT_SELECTIONDB_TIMEOUT));
    config.put("socketTimeoutMS", config.getLong("socketTimeoutMS", DEFAULT_READ_TIMEOUT));
    config.put("connectTimeoutMS", config.getLong("connectTimeoutMS", DEFAULT_CONNECTION_TIMEOUT));

    return Secret.getUsernameAndPassword(vertx, config).compose(v -> Future.succeededFuture(config.mergeIn(v)));
  }
}
