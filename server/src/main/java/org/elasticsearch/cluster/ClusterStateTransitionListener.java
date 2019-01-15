/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.List;

public class ClusterStateTransitionListener extends AbstractComponent implements ClusterStateListener {

    private final NodeClient client;

    @Inject
    public ClusterStateTransitionListener(Settings settings, NodeClient client, ClusterService clusterService) {
        super(settings);
        this.client = client;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterAllocationExplainRequest request = null;
        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedMaster() == false;
        boolean nodesChanged = false;
        boolean routingTableChanged = false;
        boolean metaDataChanged = false;
        ClusterState prevState = event.previousState();
        ClusterState currState = event.state();
       
        if (event.localNodeMaster()) {
            //logger.info("ClusterChangedEvent is [{}]", event);
            logger.info("Local Node is master");
            if (masterChanged) {
                logger.info("Master has changed was earlier {}", event.nodesDelta().previousMasterNode());
            }
            if (event.nodesAdded()) {
                logger.info("Nodes have been added {}", event.nodesDelta().addedNodes());
            }
            if (event.nodesAdded()) {
                logger.info("Nodes have been removed {}", event.nodesDelta().removedNodes());
            }
            if (event.routingTableChanged()) {
                logger.info("Routing table has changed");
                List<ShardRouting> unassignedPrev = prevState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
                List<ShardRouting> unassignedCurr = prevState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
                unassignedCurr.removeAll(unassignedPrev);
                logger.info("Shards unassigned in the current state are [{}]", unassignedCurr.toString());
                
            }
            if (event.metaDataChanged()) {
                logger.info("Metadata have changed");
            }
            List<String> newIndices = event.indicesCreated();
            if (!CollectionUtils.isEmpty(newIndices.toArray())) {
                logger.info("Indices have been created {}", newIndices );
            }
            if (!CollectionUtils.isEmpty(event.indicesDeleted().toArray())) {
                logger.info("Indices have been deleted");
            }
            if (nodesChanged || routingTableChanged || metaDataChanged) {
                if (!event.state().getRoutingNodes().unassigned().isEmpty()) {
                    request = new ClusterAllocationExplainRequest();
                    executeAllocationExplain(request);
                }
            }
        }
    }

    private void executeAllocationExplain(ClusterAllocationExplainRequest request) {
        client.admin().cluster().allocationExplain(request, new ActionListener<ClusterAllocationExplainResponse>() {
            @Override
            public void onResponse(ClusterAllocationExplainResponse response) {
                //TODO
            }
            @Override
            public void onFailure(Exception e) {
                //TODO
            }
        });
    }
}
