package com.vertx.commons.mongo;

import com.vertx.commons.annotations.ServiceProvider;
import com.vertx.commons.deploy.service.RecordService;
import com.vertx.commons.deploy.service.secret.Secret;

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
    Promise<JsonObject> promise = Promise.promise();
    if (config.containsKey("connection_string"))
      log.warn("If the connection string is used the mongo client will ignore any driver configuration options.");

    config.put("serverSelectionTimeoutMS", config.getLong("serverSelectionTimeoutMS", DEFAULT_SELECTIONDB_TIMEOUT));
    config.put("socketTimeoutMS", config.getLong("socketTimeoutMS", DEFAULT_READ_TIMEOUT));
    config.put("connectTimeoutMS", config.getLong("connectTimeoutMS", DEFAULT_CONNECTION_TIMEOUT));

    Secret.clear(config);
    Secret.getUsernameAndPassword(vertx, config).onComplete(res -> {
      if (res.succeeded()) {
        promise.complete(config.mergeIn(res.result()));
      } else {
        promise.fail(res.cause());
      }
    });

    return promise.future();
  }
}
