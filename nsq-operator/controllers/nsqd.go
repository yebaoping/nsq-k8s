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
	NsqDSvcName = "nsqcluster-nsqd"
	NsqDStsName = "nsqcluster-nsqd"
	NsqDPodName = "nsqcluster-nsqd"
)

var NsqDLabels = map[string]string{
	"app": "nsqcluster-nsqd",
}

func (r *NsqClusterReconciler) reconcileNsqD(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcileNsqDSvc(ctx, app, req)
	if err != nil {
		return res, err
	}

	res, err = r.reconcileNsqDSts(ctx, app, req)
	if err != nil {
		return res, err
	}

	// 更新主资源Status
	app.Status.NsqD.Services = make([]string, app.Spec.NsqD.Replicas)
	for i := 0; i < app.Spec.NsqD.Replicas; i++ {
		app.Status.NsqD.Services[i] = fmt.Sprintf("%s-%d.%s", NsqDStsName, i, NsqDSvcName)
	}
	if err := r.Status().Update(ctx, app); err != nil {
		r.Log.Error(err, "reconcileNsqD update status error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqDSvcName}, svc)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqDSvc(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqd service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqDSvc(ctx, app, req, svc)
}

func (r *NsqClusterReconciler) createNsqDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqDSvcName,
			Namespace: req.Namespace,
			Labels:    NsqDLabels,
		},
	}
	newSvc.Spec = generateNsqDSvcSpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newSvc, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqD set controller reference service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newSvc); err != nil {
		r.Log.Error(err, "reconcileNsqD create service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqDSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *corev1.Service) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqDSvcSpec(ctx, app, req)
	if !nsqDSvcSpecEqual(old.Spec, spec) {
		/*
			old.Spec = nsqdSvcSpec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqD update service error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqD update service spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqDStsName}, sts)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqDSts(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqd statefulset error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqDSts(ctx, app, req, sts)
}

func (r *NsqClusterReconciler) createNsqDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqDStsName,
			Namespace: req.Namespace,
			Labels:    NsqDLabels,
		},
	}
	newSts.Spec = generateNsqDStsSpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newSts, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqD set controller reference statefulset error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newSts); err != nil {
		r.Log.Error(err, "reconcileNsqD create statefulset error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqDSts(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *appsv1.StatefulSet) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqDStsSpec(ctx, app, req)
	if !nsqDStsSpecEqual(old.Spec, spec) {
		/*
			old.Spec = spec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqD update statefulset error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqD update statefulset spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func generateNsqDSvcSpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) corev1.ServiceSpec {
	return corev1.ServiceSpec{
		Ports: []corev1.ServicePort{
			{
				Name:       "broadcast-tcp",
				Port:       4150,
				TargetPort: intstr.FromInt(4150),
			},
			{
				Name:       "http-address",
				Port:       4151,
				TargetPort: intstr.FromInt(4151),
			},
		},
		Selector:  NsqDLabels,
		ClusterIP: "None", // headless service
		Type:      corev1.ServiceTypeClusterIP,
	}
}

func generateNsqDStsSpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) appsv1.StatefulSetSpec {
	spec := appsv1.StatefulSetSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: NsqDLabels,
		},
		ServiceName: NsqDSvcName,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: NsqDLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    NsqDPodName,
						Image:   "nsqio/nsq",
						Command: []string{"/nsqd"},
						Args: []string{
							fmt.Sprintf("--broadcast-address=$(NSQD_POD_NAME).%s", NsqDSvcName),
							"--broadcast-tcp-port=4150",
						},
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 4150,
							},
							{
								ContainerPort: 4151,
							},
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{
								Name: "NSQD_POD_NAME",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.name",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	spec.Replicas = new(int32)
	*spec.Replicas = int32(app.Spec.NsqD.Replicas)

	for i := 0; i < app.Spec.NsqLookupD.Replicas; i++ {
		spec.Template.Spec.Containers[0].Args = append(spec.Template.Spec.Containers[0].Args,
			fmt.Sprintf("--lookupd-tcp-address=%s-%d.%s:4160", NsqLookupDStsName, i, NsqLookupDSvcName))
	}

	return spec
}

func nsqDSvcSpecEqual(old corev1.ServiceSpec, tmpl corev1.ServiceSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}

func nsqDStsSpecEqual(old appsv1.StatefulSetSpec, tmpl appsv1.StatefulSetSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}
