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
	NsqAdminSvcName    = "nsqcluster-nsqadmin"
	NsqAdminDeployName = "nsqcluster-nsqadmin"
	NsqAdminPodName    = "nsqcluster-nsqadmin"
)

var NsqAdminLabels = map[string]string{
	"app": "nsqcluster-nsqadmin",
}

func (r *NsqClusterReconciler) reconcileNsqAdmin(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcileNsqAdminSvc(ctx, app, req)
	if err != nil {
		return res, err
	}

	res, err = r.reconcileNsqAdminDeploy(ctx, app, req)
	if err != nil {
		return res, err
	}

	// 获取pod所在node的ip
	pods := &corev1.PodList{}
	err = r.List(ctx, pods, client.InNamespace(req.Namespace), client.MatchingLabels(NsqAdminLabels))
	if err != nil {
		r.Log.Error(err, "reconcileNsqAdmin get nsqAdmin pods error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}
	if len(pods.Items) == 0 || pods.Items[0].Status.HostIP == "" {
		r.Log.Info("reconcileNsqAdmin get nsqAdmin pods empty or host ip empty,requeue after 5 seconds")

		return ctrl.Result{RequeueAfter: time.Second * 5}, fmt.Errorf("reconcileNsqAdmin get nsqAdmin pods empty or host ip empty")
	}

	app.Status.NsqAdmin.Address = fmt.Sprintf("http://%s:30001", pods.Items[0].Status.HostIP)
	if err := r.Status().Update(ctx, app); err != nil {
		r.Log.Error(err, "reconcileNsqAdmin update status error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqAdminSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqAdminSvcName}, svc)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqAdminSvc(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqadmin service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqAdminSvc(ctx, app, req, svc)
}

func (r *NsqClusterReconciler) createNsqAdminSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqAdminSvcName,
			Namespace: req.Namespace,
			Labels:    NsqAdminLabels,
		},
	}
	newSvc.Spec = generateNsqAdminSvcSpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newSvc, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqAdmin set controller reference service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newSvc); err != nil {
		r.Log.Error(err, "reconcileNsqAdmin create service error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqAdminSvc(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *corev1.Service) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqAdminSvcSpec(ctx, app, req)
	if !nsqAdminSvcSpecEqual(old.Spec, spec) {
		/*
			old.Spec = nsqAdminSvcSpec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqAdmin update service error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqAdmin update service spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) reconcileNsqAdminDeploy(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: NsqAdminDeployName}, deploy)
	if errors.IsNotFound(err) {
		// 不存在，新建
		return r.createNsqAdminDeploy(ctx, app, req)
	}
	if err != nil {
		r.Log.Error(err, "Get nsqAdmin deployment error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	// 已经存在
	return r.updateNsqAdminDeploy(ctx, app, req, deploy)
}

func (r *NsqClusterReconciler) createNsqAdminDeploy(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) (ctrl.Result, error) {
	newDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NsqAdminDeployName,
			Namespace: req.Namespace,
			Labels:    NsqAdminLabels,
		},
	}
	newDeploy.Spec = generateNsqAdminDeploySpec(ctx, app, req)

	if err := ctrl.SetControllerReference(app, newDeploy, r.Scheme); err != nil {
		r.Log.Error(err, "reconcileNsqAdmin set controller reference deployment error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	if err := r.Create(ctx, newDeploy); err != nil {
		r.Log.Error(err, "reconcileNsqAdmin create deployment error")
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *NsqClusterReconciler) updateNsqAdminDeploy(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request, old *appsv1.Deployment) (ctrl.Result, error) {
	// 对比space是否改动
	spec := generateNsqAdminDeploySpec(ctx, app, req)
	if !nsqAdminDeploySpecEqual(old.Spec, spec) {
		/*
			old.Spec = spec
			if err := r.Update(ctx, old); err != nil {
				r.Log.Error(err, "reconcileNsqAdmin update deployment error")
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		*/

		spec, _ := json.Marshal(map[string]interface{}{
			"spec": spec,
		})
		if err := r.Patch(ctx, old, client.RawPatch(types.MergePatchType, spec)); err != nil {
			r.Log.Error(err, "reconcileNsqAdmin update deployment spec error")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}
func generateNsqAdminSvcSpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) corev1.ServiceSpec {
	return corev1.ServiceSpec{
		Ports: []corev1.ServicePort{
			{
				Port:       4171,
				TargetPort: intstr.FromInt(4171),
				NodePort:   30001,
			},
		},
		Selector: NsqAdminLabels,
		Type:     corev1.ServiceTypeNodePort,
	}
}

func generateNsqAdminDeploySpec(ctx context.Context, app *apiv1.NsqCluster, req ctrl.Request) appsv1.DeploymentSpec {
	spec := appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: NsqAdminLabels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: NsqAdminLabels,
			},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{
					{
						Name:  "nsqcluster-nsqadmin-init",
						Image: "busybox",
					},
				},
				Containers: []corev1.Container{
					{
						Name:    NsqAdminPodName,
						Image:   "nsqio/nsq",
						Command: []string{"/nsqadmin"},
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 4171,
							},
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
					},
				},
			},
		},
	}

	spec.Replicas = new(int32)
	*spec.Replicas = 1

	var initContainerCmd string

	spec.Template.Spec.Containers[0].Args = make([]string, app.Spec.NsqLookupD.Replicas)
	for i := 0; i < app.Spec.NsqLookupD.Replicas; i++ {
		lookupDAddr := fmt.Sprintf("%s-%d.%s", NsqLookupDStsName, i, NsqLookupDSvcName)

		initContainerCmd += fmt.Sprintf("until ping %s -c 1; do echo waiting for %s; sleep 2; done;", lookupDAddr, lookupDAddr)

		spec.Template.Spec.Containers[0].Args[i] = fmt.Sprintf("--lookupd-http-address=%s:4161", lookupDAddr)
	}

	spec.Template.Spec.InitContainers[0].Command = []string{"sh", "-c", initContainerCmd}

	return spec
}

func nsqAdminSvcSpecEqual(old corev1.ServiceSpec, tmpl corev1.ServiceSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}

func nsqAdminDeploySpecEqual(old appsv1.DeploymentSpec, tmpl appsv1.DeploymentSpec) bool {
	return reflect.DeepEqual(old, tmpl)
}
