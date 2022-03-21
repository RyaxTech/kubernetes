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
	"k8s.io/klog/v2"
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
	if klog.V(10).Enabled() {
		for _, node := range nodeInfos {
			for imageName, image := range node.ImageStates {
				klog.InfoS("ImageLayersPlugin debug info", "node", node.Node().Name, "image", imageName, "container-image", pod.Spec.Containers[0].Image, "nodeImageStatus", image)
			}
		}
	}

	score := sumImageScores(nodeInfo, pod.Spec.Containers, nodeInfos)

	return int64(score * 100), framework.NewStatus(framework.Success, "Score computed successfully")
}

// ScoreExtensions of the Score plugin.
func (pl *ImageLayersLocality) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &ImageLayersLocality{handle: h}, nil
}

// Returns the sum of image scores of all the containers that are already on the node.
// Each image receives a score for sum of the size of each layer presnet on the node, nomalized by the total image size.
// Note that the init containers are not considered for it's rare for users to deploy
// huge init containers.
func sumImageScores(nodeInfo *framework.NodeInfo, containers []v1.Container, nodeInfos []*framework.NodeInfo) float64 {
	var layerScores float64 = 0
	for _, container := range containers {
		var sum int64 = 0
		imageName := normalizedImageName(container.Image)
		imageLayers, imageSize := getImageLayersAndSize(nodeInfos, imageName)
		presentLayers := make(map[string]int64)

		if klog.V(10).Enabled() {
			klog.InfoS("ImageLayersPlugin debug info", "imageNormalized", imageName, "imageLayers", imageLayers, "imageSize", imageSize)
		}
		for _, state := range nodeInfo.ImageStates {
			getPresentLayers(state, nodeInfo.Node().Name, imageLayers, imageSize, presentLayers)
		}
		for _, size := range presentLayers {
			sum += size
		}
		if klog.V(10).Enabled() {
			klog.InfoS("ImageLayersPlugin LAYERS", "presentLayers", presentLayers, "sumLayers", sum, "imageSize", imageSize, "node", nodeInfo.Node().Name, "image", imageName)
		}
		layerScore := float64(sum) / float64(imageSize)
		// Avoid score higher than expected when common layers then the sum of the layers
		if layerScore > 1 {
			layerScore = 1
		}
		layerScores += layerScore
		if klog.V(10).Enabled() {
			klog.InfoS("ImageLayersPlugin", "SCORE", layerScore, "node", nodeInfo.Node().Name, "image", imageName)
		}
	}
	return layerScores / float64(len(containers))
}

// getPresentLayers fills the presentLayers map for one image regarding the layers present on the node.
func getPresentLayers(imageState *framework.ImageStateSummary, nodeName string, imageLayers []string, imageSize int64, presentLayers map[string]int64) {
	// Compute layers score
	var sum int64 = 0
	for _, layer := range imageLayers {
		if imageState.LayersOnNodes[layer].Has(nodeName) {
			presentLayers[layer] = imageState.LayersSize[layer]
			if klog.V(10).Enabled() {
				klog.InfoS("ImageLayersPlugin LAYER FOUND!", "layer", layer, "sum", sum, "node", nodeName)
			}
		}
	}
}

func getImageLayersAndSize(nodeInfos []*framework.NodeInfo, imageName string) ([]string, int64) {
	imageLayers := make([]string, 0)
	var totalSize int64 = 0
	for _, node := range nodeInfos {
		if image, ok := node.ImageStates[imageName]; ok {
			for layer, size := range image.LayersSize {
				imageLayers = append(imageLayers, layer)
				totalSize += size
			}
			return imageLayers, totalSize
		}
	}
	return imageLayers, 1
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
