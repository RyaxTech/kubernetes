/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package imagelayerslocality

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/internal/cache"
)

func TestImageLocalityPriority(t *testing.T) {
	const mb int64 = 1024 * 1024
	testLayers := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/base100:latest",
			},
		},
	}

	nodeLayers1 := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"gcr.io/base100:latest",
				},
				SizeBytes: int64(100 * mb),
				Layers: map[string]int64{
					"layerA": 15 * mb,
					"layerB": 10 * mb,
					"layerC": 10 * mb,
					"layerD": 65 * mb,
				},
			},
		},
	}

	nodeLayers2 := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"gcr.io/base:latest",
				},
				SizeBytes: int64(25 * mb),
				Layers: map[string]int64{
					"layerA": 15 * mb,
					"layerB": 10 * mb,
				},
			},
			{
				Names: []string{
					"gcr.io/other:latest",
				},
				SizeBytes: int64(25 * mb),
				Layers: map[string]int64{
					"layerX": 15 * mb,
					"layerY": 10 * mb,
				},
			},
		},
	}

	nodeWithNoImages := v1.NodeStatus{}

	tests := []struct {
		pod          *v1.Pod
		pods         []*v1.Pod
		nodes        []*v1.Node
		expectedList framework.NodeScoreList
		name         string
	}{
		{
			// Pod: gcr.io/base50
			// Layers:

			// Node1
			// Image: gcr.io/base100:latest 100MB
			// Layers: A:15MB, B:10MB, C:10M D:65M
			// Score: 100 * (15MB + 10MB + 10MB + 65MB) / 100MB = 100

			// Node2
			// Image: gcr.io/base:latest 25MB, gcr.io/other:latest 25MB
			// Layers: A:15MB, B:10MB ...
			// Score: 100 * (15MB + 10MB) / 100MB = 25

			// Node3
			// Score: 0
			pod:          &v1.Pod{Spec: testLayers},
			nodes:        []*v1.Node{makeImageNode("machine1", nodeLayers1), makeImageNode("machine2", nodeLayers2), makeImageNode("machine3", nodeWithNoImages)},
			expectedList: []framework.NodeScore{{Name: "machine1", Score: 100}, {Name: "machine2", Score: 25}, {Name: "machine3", Score: 0}},
			name:         "pod with multiple small images",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cache.NewSnapshot(nil, test.nodes)

			state := framework.NewCycleState()
			fh, _ := runtime.NewFramework(nil, nil, runtime.WithSnapshotSharedLister(snapshot))

			p, _ := New(nil, fh)
			var gotList framework.NodeScoreList
			for _, n := range test.nodes {
				nodeName := n.ObjectMeta.Name
				score, status := p.(framework.ScorePlugin).Score(context.Background(), state, test.pod, nodeName)
				if !status.IsSuccess() {
					t.Errorf("unexpected error: %v", status)
				}
				gotList = append(gotList, framework.NodeScore{Name: nodeName, Score: score})
			}

			if diff := cmp.Diff(test.expectedList, gotList); diff != "" {
				t.Errorf("Unexpected node score list (-want, +got):\n%s", diff)
			}
		})
	}
}

func makeImageNode(node string, status v1.NodeStatus) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Status:     status,
	}
}
