/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.rest.resources;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.PredicatedTransformation;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.tools.*;
import org.apache.kafka.connect.transforms.Transformation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;

@Path("/connector-metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorMetricsResource implements ConnectResource {

    private final Herder herder;
    private final List<PluginInfo> connectorPlugins;

    private long requestTimeoutMs;



    public ConnectorMetricsResource(Herder herder) {
        this.herder = herder;
        this.connectorPlugins = new ArrayList<>();
        this.requestTimeoutMs = DEFAULT_REST_REQUEST_TIMEOUT_MS;

        // TODO: improve once plugins are allowed to be added/removed during runtime.
        addConnectorPlugins(herder.plugins().predicates(), Collections.emptySet());
        addConnectorPlugins(herder.plugins().converters(), Collections.emptySet());
        addConnectorPlugins(herder.plugins().headerConverters(), Collections.emptySet());
    }

    private <T> void addConnectorPlugins(Collection<PluginDesc<T>> plugins, Collection<Class<? extends T>> excludes) {
        plugins.stream()
                .filter(p -> !excludes.contains(p.pluginClass()))
                .map(PluginInfo::new)
                .forEach(connectorPlugins::add);
    }

    @Override
    public void requestTimeout(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    @GET
    @Path("/")
    @Operation(summary = "List all connector metrics installed")
    public List<Object> listConnectorMetrics(
            @DefaultValue("true") @QueryParam("connectorsOnly") @Parameter(description = "Whether to list only connectors instead of all plugins") boolean connectorsOnly
    ) {
        synchronized (this) {
            if (connectorsOnly) {
                return Collections.unmodifiableList(connectorPlugins.stream()
                        .filter(p -> PluginType.SINK.toString().equals(p.type()) || PluginType.SOURCE.toString().equals(p.type()))
                        .collect(Collectors.toList()));
            } else {
                return Collections.unmodifiableList(connectorPlugins);
            }
        }
    }

    @GET
    @Path("/{connectorName}")
    @Operation(summary = "Get the metrics definition for the specified connector name")
    public List<Object> getConnectorMetrics(final @PathParam("connectorName") String connectorName) {
        synchronized (this) {
            return Collections.singletonList(connectorName);
        }
    }

}
