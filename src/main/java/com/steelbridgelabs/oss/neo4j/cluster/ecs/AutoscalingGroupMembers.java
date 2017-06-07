/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package com.steelbridgelabs.oss.neo4j.cluster.ecs;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.ContainerInstance;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesRequest;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesResult;
import com.amazonaws.services.ecs.model.ListContainerInstancesRequest;
import com.amazonaws.services.ecs.model.ListContainerInstancesResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Rogelio J. Baucells
 */
public class AutoscalingGroupMembers {

    public static void main(String[] args) {
        // check arguments
        if (args.length == 1) {
            // create aws ec2 client
            AmazonEC2 client = AmazonEC2ClientBuilder.defaultClient();
            // create request
            DescribeInstancesRequest request = new DescribeInstancesRequest();
            // use instance ids associated to cluster
            request.withInstanceIds(ec2Instances(args[0]));
            // next token
            String token = null;
            // instance dns
            List<String> dns = new ArrayList<>();
            do {
                // set next token
                request.setNextToken(token);
                // describe instances
                DescribeInstancesResult result = client.describeInstances(request);
                // loop reservations
                result.getReservations().forEach(reservation -> {
                    // append private dns
                    dns.addAll(reservation.getInstances().stream().map(Instance::getPrivateDnsName).collect(Collectors.toList()));
                });
                // check we have more data to retrieve
                token = result.getNextToken();
            }
            while (token != null);
            // dump initial cluster members
            System.out.print(dns.stream().map(entry -> entry + ":5000").collect(Collectors.joining(",")));
        }
        else {
            // show information
            System.err.println("Invalid ECS cluster name: java -jar neo4j-cluster-ecs-tools.jar My-Cluster-Name");
        }
    }

    private static List<String> containerInstanceArns(AmazonECS client, String cluster) {
        // create request
        ListContainerInstancesRequest request = new ListContainerInstancesRequest();
        // specify cluster name
        request.withCluster(cluster);
        // next token
        String token = null;
        // container instance arns
        List<String> list = new ArrayList<>();
        do {
            // set next token
            request.setNextToken(token);
            // describe instances
            ListContainerInstancesResult result = client.listContainerInstances(request);
            // get container instance arns
            list.addAll(result.getContainerInstanceArns());
            // check we have more data to retrieve
            token = result.getNextToken();
        }
        while (token != null);
        // return arns
        return list;
    }

    private static List<String> ec2Instances(String cluster) {
        // aws ecs client
        AmazonECS client = AmazonECSClientBuilder.defaultClient();
        // create request
        DescribeContainerInstancesRequest request = new DescribeContainerInstancesRequest();
        // cluster name
        request.withCluster(cluster);
        // container instances to describe
        request.withContainerInstances(containerInstanceArns(client, cluster));
        // describe instances
        DescribeContainerInstancesResult result = client.describeContainerInstances(request);
        // return ec2 instance ids
        return result.getContainerInstances().stream().map(ContainerInstance::getEc2InstanceId).collect(Collectors.toList());
    }
}
