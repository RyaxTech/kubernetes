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
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// ImageLayersLocality is a score plugin that favors nodes that have the bigest total layers requested pod container's images layers.
type ImageLayersLocality struct {
	handle framework.Handle
}

var _ framework.ScorePlugin = &ImageLayersLocality{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.ImageLayersLocality

// Name returns name of the plugin. It is used in logs, etc.
func (pl *ImageLayersLocality) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (pl *ImageLayersLocality) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("getting node %q from Snapshot: %w", nodeName, err))
	}

	nodeInfos, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	score := sumImageScores(nodeInfo, pod.Spec.Containers, nodeInfos)

	return int64(score * 100), nil
}

// ScoreExtensions of the Score plugin.
func (pl *ImageLayersLocality) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &ImageLayersLocality{handle: h}, nil
}

// sumImageScores returns the sum of image scores of all the containers that are already on the node.
// Each image receives a raw score of its size, scaled by scaledImageScore. Note
// that the init containers are not considered for it's rare for users to deploy
// huge init containers.
func sumImageScores(nodeInfo *framework.NodeInfo, containers []v1.Container, nodeInfos []*framework.NodeInfo) float64 {
	var sum float64
	for _, container := range containers {
		imageName := normalizedImageName(container.Image)
		imageLayers, imageSize := getImageLayersAndSize(nodeInfos, imageName)
		for _, state := range nodeInfo.ImageStates {
			sum += scaledImageScore(state, nodeInfo.Node().Name, imageLayers, imageSize)
		}
	}
	return sum / float64(len(containers))
}

// scaledImageScore returns a score for the given state of an image regarding the layers present on the node.
func scaledImageScore(imageState *framework.ImageStateSummary, nodeName string, imageLayers []string, imageSize int64) float64 {
	// Compute layers score
	var sum int64
	for _, layer := range imageLayers {
		if imageState.LayersOnNodes[layer].Has(nodeName) {
			sum += imageState.LayersSize[layer]
		}
	}
	layerScore := float64(sum) / float64(imageSize)
	return layerScore
}

func getImageLayersAndSize(nodeInfos []*framework.NodeInfo, imageName string) ([]string, int64) {
	imageLayers := make([]string, 0)
	for _, node := range nodeInfos {
		if image, ok := node.ImageStates[imageName]; ok {
			for layer := range image.LayersSize {
				imageLayers = append(imageLayers, layer)
			}
			return imageLayers, image.Size
		}
	}
	return imageLayers, -1
}

// normalizedImageName returns the CRI compliant name for a given image.
// TODO: cover the corner cases of missed matches, e.g,
// 1. Using Docker as runtime and docker.io/library/test:tag in pod spec, but only test:tag will present in node status
// 2. Using the implicit registry, i.e., test:tag or library/test:tag in pod spec but only docker.io/library/test:tag
// in node status; note that if users consistently use one registry format, this should not happen.
func normalizedImageName(name string) string {
	if strings.LastIndex(name, ":") <= strings.LastIndex(name, "/") {
		name = name + ":latest"
	}
	return name
}
