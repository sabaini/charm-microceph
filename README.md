# charm-microceph

This charm deploys the microceph snap.

It is expected to be related to the control plane via cross model relations. To
achieve this assuming the control plane is in a model called *k8s*.

```
juju offer microceph:ceph

juju integrate -m k8s glance:ceph admin/controller.microceph
```
