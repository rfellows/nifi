/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { RouterModule, Routes } from '@angular/router';
import { Cluster } from './cluster.component';
import { NgModule } from '@angular/core';
import { ClusterNodeListing } from '../ui/cluster-node-listing/cluster-node-listing.component';
import { ClusterSystemListing } from '../ui/cluster-system-listing/cluster-system-listing.component';
import { ClusterJvmListing } from '../ui/cluster-jvm-listing/cluster-jvm-listing.component';

const routes: Routes = [
    {
        path: '',
        component: Cluster,
        children: [
            { path: '', pathMatch: 'full', redirectTo: 'nodes' },
            {
                path: 'nodes',
                component: ClusterNodeListing,
                children: [
                    {
                        path: ':id',
                        component: ClusterNodeListing
                    }
                ]
            },
            {
                path: 'system',
                component: ClusterSystemListing,
                children: [
                    {
                        path: ':id',
                        component: ClusterSystemListing
                    }
                ]
            },
            {
                path: 'jvm',
                component: ClusterJvmListing,
                children: [
                    {
                        path: ':id',
                        component: ClusterJvmListing
                    }
                ]
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class ClusterRoutingModule {}
