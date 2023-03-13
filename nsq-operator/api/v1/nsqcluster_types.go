/*
Copyright 2023 yebaoping.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NsqClusterSpec defines the desired state of NsqCluster
type NsqClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	NsqLookupD NsqLookupDTemplate `json:"nsqlookupd"`

	NsqD NsqDTemplate `json:"nsqd"`

	NsqAdmin NsqAdminTemplate `json:"nsqadmin,omitempty"`
}

type NsqLookupDTemplate struct {
	Replicas int `json:"replicas,omitempty"`
}

type NsqDTemplate struct {
	Replicas int `json:"replicas,omitempty"`
}

type NsqAdminTemplate struct {
}

// NsqClusterStatus defines the observed state of NsqCluster
type NsqClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// NsqCluster is the Schema for the nsqclusters API
type NsqCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NsqClusterSpec   `json:"spec,omitempty"`
	Status NsqClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// NsqClusterList contains a list of NsqCluster
type NsqClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NsqCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NsqCluster{}, &NsqClusterList{})
}
