package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	apiv1 "github.com/yebaoping/nsq-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const (
	NsqLookupDSvcName = "nsqcluster-nsqlookupd"
	NsqLookupDStsName = "nsqcluster-nsqlookupd"
	NsqLookupDPodName = "nsqcluster-nsqlookupd"
)

var NsqLookupDLabels = map[string]string{
	"app": "nsqcluster-nsqlookupd",
}

func (r *NsqClusterReconciler) reconcileNsqLookupD(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcileNsqLookupDSvc(ctx, app, req)
	if err != nil {
		return res, err
	}

	res, err = r.reconcileNsqLookupDSts(ctx, app, req)
	if err != nil {
		return res, err
	}

	// 更新主资源Status
	app.Status.NsqLookupD.Services = make([]string, app.Spec.NsqLookupD.Replicas)
	for i := 0; i < app.Spec.NsqLookupD.Replicas; i++ {
		app.Status.NsqLookupD.Services[i] = fmt.Sprintf("%s-%d.%s", NsqLookupDStsName, i, NsqLookupDSvcName)
	}
	if err := r.Status().Update(ctx, app); err != nil {
		r.Log.Error(err, "reconcileNsqLookupD update status error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqLookupDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqLookupDSvcName}, svc)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqLookupDSvc(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqlookupd service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqLookupDSvc(ctx, app, req, svc)
}

func (r *NsqClusterReconciler) createNsqLookupDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqLookupDSvcName,
			Namespace: req.Namespace,
			Labels:    NsqLookupDLabels,
		},
	}
	newSvc.Spec = generateNsqLookupDSvcSpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newSvc, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqLookupD set controller reference service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newSvc); err != nil {
		r.Log.Error(err, "reconcileNsqLookupD create service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqLookupDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *corev1.Service) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqLookupDSvcSpec(ctx, app, req)
	if !nsqLookupDSvcSpecEqual(old.Spec, spec) {
		/*
			old.Spec = nsqLookupDSvcSpec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqLookupD update service error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqLookupD update service spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqLookupDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqLookupDStsName}, sts)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqLookupDSts(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqlookupd statefulset error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqLookupDSts(ctx, app, req, sts)
}

func (r *NsqClusterReconciler) createNsqLookupDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqLookupDStsName,
			Namespace: req.Namespace,
			Labels:    NsqLookupDLabels,
		},
	}

	newSts.Spec = generateNsqLookupDStsSpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newSts, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqLookupD set controller reference statefulSet error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newSts); err != nil {
		r.Log.Error(err, "reconcileNsqLookupD create statefulSet error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqLookupDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *appsv1.StatefulSet) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqLookupDStsSpec(ctx, app, req)
	if !nsqLookupDStsSpecEqual(old.Spec, spec) {
		/*
			old.Spec = spec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqLookupD update statefulset error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqLookupD update statefulset spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func generateNsqLookupDSvcSpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) corev1.ServiceSpec {
	return corev1.ServiceSpec{
		Ports: []corev1.ServicePort{
			{
				Name:       "lookupd-tcp",
				Port:       4160,
				TargetPort: intstr.FromInt(4160),
			},
			{
				Name:       "lookupd-http",
				Port:       4161,
				TargetPort: intstr.FromInt(4161),
			},
		},
		Selector:  NsqLookupDLabels,
		ClusterIP: "None", // headless service
		Type:      corev1.ServiceTypeClusterIP,
	}
}

func generateNsqLookupDStsSpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) appsv1.StatefulSetSpec {
	spec := appsv1.StatefulSetSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: NsqLookupDLabels,
		},
		ServiceName: NsqLookupDSvcName,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: NsqLookupDLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    NsqLookupDPodName,
						Image:   "nsqio/nsq",
						Command: []string{"/nsqlookupd"},
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 4160,
							},
							{
								ContainerPort: 4161,
							},
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
					},
				},
			},
		},
	}

	spec.Replicas = new(int32)
	*spec.Replicas = int32(app.Spec.NsqLookupD.Replicas)

	return spec
}

func nsqLookupDSvcSpecEqual(old corev1.ServiceSpec, tmpl corev1.ServiceSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}

func nsqLookupDStsSpecEqual(old appsv1.StatefulSetSpec, tmpl appsv1.StatefulSetSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}
