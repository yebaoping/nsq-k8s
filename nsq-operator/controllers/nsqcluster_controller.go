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

package controllers

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"time"

	apiv1 "github.com/yebaoping/nsq-operator/api/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
)

// NsqClusterReconciler reconciles a NsqCluster object
type NsqClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	record.EventRecorder
	Log logr.Logger
}

//+kubebuilder:rbac:groups=apps.yebaoping.cn,resources=nsqclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps.yebaoping.cn,resources=nsqclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps.yebaoping.cn,resources=nsqclusters/finalizers,verbs=update

//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services/status,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NsqCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *NsqClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("nsqclusters reconcile", "request", req.String())

	nsqCluster := &apiv1.NsqCluster{}
	if err := r.Get(ctx, req.NamespacedName, nsqCluster); err != nil {
		// 已删除
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		r.Log.Error(err, "Get NsqCluster error")
		r.EventRecorder.Event(nsqCluster, corev1.EventTypeWarning, err.Error(), "Get NsqCluster error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if res, err := r.reconcileNsqLookupD(ctx, nsqCluster, req); err != nil {
		return res, err
	}
	if res, err := r.reconcileNsqD(ctx, nsqCluster, req); err != nil {
		return res, err
	}
	if res, err := r.reconcileNsqAdmin(ctx, nsqCluster, req); err != nil {
		return res, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NsqClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.NsqCluster{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(createEvent event.CreateEvent) bool {
				return true
			},
			DeleteFunc: func(event event.DeleteEvent) bool {
				r.Log.Info("Resource deleted", "NamespacedName", event.Object.GetNamespace()+"/"+event.Object.GetName())

				return false
			},
			UpdateFunc: func(event event.UpdateEvent) bool {
				if event.ObjectNew.GetResourceVersion() == event.ObjectOld.GetResourceVersion() {
					return false
				}

				newObj, ok := event.ObjectNew.(*apiv1.NsqCluster)
				if !ok {
					return false
				}
				oldObj, ok := event.ObjectOld.(*apiv1.NsqCluster)
				if !ok {
					return false
				}
				if reflect.DeepEqual(newObj.Spec, oldObj.Spec) {
					return false
				}

				r.Log.Info("Resource update", "NamespacedName", event.ObjectNew.GetNamespace()+"/"+event.ObjectNew.GetName(),
					"old_spec", oldObj.Spec, "new_spec", newObj.Spec)

				return true
			},
		})).
		Owns(&corev1.Service{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(event event.CreateEvent) bool {
				return false
			},
			DeleteFunc: func(event event.DeleteEvent) bool {
				r.Log.Info("Service deleted", "NamespacedName", event.Object.GetNamespace()+"/"+event.Object.GetName())

				return true
			},
			UpdateFunc: func(event event.UpdateEvent) bool {
				if event.ObjectNew.GetResourceVersion() == event.ObjectOld.GetResourceVersion() {
					return false
				}

				newObj, ok := event.ObjectNew.(*corev1.Service)
				if !ok {
					return false
				}
				oldObj, ok := event.ObjectOld.(*corev1.Service)
				if !ok {
					return false
				}
				if reflect.DeepEqual(newObj.Spec, oldObj.Spec) {
					return false
				}

				r.Log.Info("Service update", "NamespacedName", event.ObjectNew.GetNamespace()+"/"+event.ObjectNew.GetName(),
					"old_spec", oldObj.Spec, "new_spec", newObj.Spec)

				return true
			},
		})).
		Owns(&appsv1.StatefulSet{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(event event.CreateEvent) bool {
				return false
			},
			DeleteFunc: func(event event.DeleteEvent) bool {
				r.Log.Info("StatefulSet deleted", "NamespacedName", event.Object.GetNamespace()+"/"+event.Object.GetName())

				return true
			},
			UpdateFunc: func(event event.UpdateEvent) bool {
				if event.ObjectNew.GetResourceVersion() == event.ObjectOld.GetResourceVersion() {
					return false
				}

				newObj, ok := event.ObjectNew.(*appsv1.StatefulSet)
				if !ok {
					return false
				}
				oldObj, ok := event.ObjectOld.(*appsv1.StatefulSet)
				if !ok {
					return false
				}
				if reflect.DeepEqual(newObj.Spec, oldObj.Spec) {
					return false
				}

				r.Log.Info("StatefulSet update", "NamespacedName", event.ObjectNew.GetNamespace()+"/"+event.ObjectNew.GetName(),
					"old_spec", oldObj.Spec, "new_spec", newObj.Spec)

				return true
			},
		})).
		Owns(&appsv1.Deployment{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(event event.CreateEvent) bool {
				return false
			},
			DeleteFunc: func(event event.DeleteEvent) bool {
				r.Log.Info("Deployment deleted", "NamespacedName", event.Object.GetNamespace()+"/"+event.Object.GetName())

				return true
			},
			UpdateFunc: func(event event.UpdateEvent) bool {
				if event.ObjectNew.GetResourceVersion() == event.ObjectOld.GetResourceVersion() {
					return false
				}

				newObj, ok := event.ObjectNew.(*appsv1.Deployment)
				if !ok {
					return false
				}
				oldObj, ok := event.ObjectOld.(*appsv1.Deployment)
				if !ok {
					return false
				}
				if reflect.DeepEqual(newObj.Spec, oldObj.Spec) {
					return false
				}

				r.Log.Info("Deployment update", "NamespacedName", event.ObjectNew.GetNamespace()+"/"+event.ObjectNew.GetName(),
					"old_spec", oldObj.Spec, "new_spec", newObj.Spec)

				return true
			},
		})).
		Complete(r)
}
