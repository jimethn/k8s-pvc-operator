import logging
import pytest
import re
from datetime import timedelta
from unittest.mock import MagicMock


def test_delete_if_needed(operator_pvc_manager, v1, appsv1, pvc):
  operator_pvc_manager.v1 = v1
  operator_pvc_manager.appsv1 = appsv1
  operator_pvc_manager.get_sts_for_pvc = MagicMock()
  operator_pvc_manager.pvc_unmounted_long_enough = MagicMock()
  sts_0_replicas = MagicMock()
  sts_0_replicas.spec.replicas = 0

  # recently deleted sts should not delete
  operator_pvc_manager.get_sts_for_pvc.return_value = None
  operator_pvc_manager.pvc_unmounted_long_enough.return_value = False
  assert operator_pvc_manager.delete_if_needed(pvc) == False
  operator_pvc_manager.v1.delete_namespaced_persistent_volume_claim.assert_not_called()

  # recently downscaled sts should not delete
  operator_pvc_manager.get_sts_for_pvc.return_value = sts_0_replicas
  operator_pvc_manager.pvc_unmounted_long_enough.return_value = False
  assert operator_pvc_manager.delete_if_needed(pvc) == False
  operator_pvc_manager.v1.delete_namespaced_persistent_volume_claim.assert_not_called()

  # long-deleted sts should delete
  operator_pvc_manager.get_sts_for_pvc.return_value = None
  operator_pvc_manager.pvc_unmounted_long_enough.return_value = True
  assert operator_pvc_manager.delete_if_needed(pvc) == True
  assert operator_pvc_manager.v1.delete_namespaced_persistent_volume_claim.call_count == 1

  # long-downscaled sts should delete
  operator_pvc_manager.get_sts_for_pvc.return_value = sts_0_replicas
  operator_pvc_manager.pvc_unmounted_long_enough.return_value = True
  assert operator_pvc_manager.delete_if_needed(pvc) == True
  assert operator_pvc_manager.v1.delete_namespaced_persistent_volume_claim.call_count == 2

def test_resize_if_needed(operator_pvc_manager, v1, appsv1, pvc):
  operator_pvc_manager.v1 = v1
  operator_pvc_manager.appsv1 = appsv1

  # invalid units should return false
  pvc.spec.resources.requests = {'storage':'40Ti'}  # we only accept Gi units
  assert operator_pvc_manager.resize_if_needed(pvc) == False

  # unspecified units should return false
  operator_pvc_manager.get_pvc_desired_size = MagicMock()
  operator_pvc_manager.get_pvc_desired_size.return_value = '12345'
  assert operator_pvc_manager.resize_if_needed(pvc) == False

  # desired matching current should return false
  pvc.spec.resources.requests['storage'] = '40Gi'
  operator_pvc_manager.get_pvc_desired_size.return_value = '40Gi'
  assert operator_pvc_manager.resize_if_needed(pvc) == False

  # current exceeding desired should return false
  pvc.spec.resources.requests['storage'] = '400Gi'
  operator_pvc_manager.get_pvc_desired_size.return_value = '40Gi'
  assert operator_pvc_manager.resize_if_needed(pvc) == False

  # desired exceeding current should scale up
  pvc.spec.resources.requests['storage'] = '40Gi'
  operator_pvc_manager.get_pvc_desired_size.return_value = '500Gi'
  assert operator_pvc_manager.resize_if_needed(pvc) == True
  operator_pvc_manager.v1.patch_namespaced_persistent_volume_claim.assert_called_once()

def test_get_sts_for_pvc(operator_pvc_manager, appsv1, pvc):
  operator_pvc_manager.appsv1 = appsv1
  sts = operator_pvc_manager.get_sts_for_pvc(pvc)
  assert sts.metadata.name == 'some-sts'

def test_get_ordinal(operator_pvc_manager):
  fake_object = MagicMock()
  fake_object.metadata.name = 'my-sts-pod-5'
  ordinal = operator_pvc_manager.get_ordinal(fake_object)
  assert ordinal == 5

  with pytest.raises(RuntimeWarning) as excinfo:
    fake_object.metadata.name = 'has-no-ordinal'
    operator_pvc_manager.get_ordinal(fake_object)
  assert excinfo.typename == 'RuntimeWarning'

def test_get_pvc_desired_size(operator_pvc_manager, sts):
  # it should pull the size from the object, with correct format
  size = operator_pvc_manager.get_pvc_desired_size(sts)
  assert re.match('^[0-9]+Gi$', size)

  # unspecified units should raise a warning
  sts.metadata.annotations['pvc-operator/storage-size'] = '12345'
  with pytest.raises(RuntimeWarning) as excinfo:
    operator_pvc_manager.get_pvc_desired_size(sts)
  assert excinfo.typename == 'RuntimeWarning'

def test_pvc_unmounted_long_enough(operator_pvc_manager, v1, cloudtrail, pvc):
  operator_pvc_manager.pvc_grace_minutes = timedelta(seconds=1)
  operator_pvc_manager.cloudtrail = cloudtrail
  operator_pvc_manager.v1 = v1
  # still attached, should return False
  operator_pvc_manager.cloudtrail.attach_first()
  long_enough = operator_pvc_manager.pvc_unmounted_long_enough(pvc)
  assert long_enough == False
  # detatched a while ago, should return True
  operator_pvc_manager.cloudtrail.detatch_first()
  long_enough = operator_pvc_manager.pvc_unmounted_long_enough(pvc)
  assert long_enough == True
  # detatched "recently" (within grace period), should return False
  operator_pvc_manager.pvc_grace_minutes = timedelta(days=10000)
  operator_pvc_manager.cloudtrail.detatch_first()
  long_enough = operator_pvc_manager.pvc_unmounted_long_enough(pvc)
  assert long_enough == False

def test_get_volume_id(operator_pvc_manager):
  v1 = MagicMock()
  operator_pvc_manager.v1 = v1
  # test gp2 spec
  gp2 = MagicMock()
  gp2.items[0].spec.aws_elastic_block_store.volume_id = 'aws://us-east-1c/vol-0a6d7a39a07212c42'
  gp2.items[0].spec.csi = None
  v1.list_persistent_volume.return_value = gp2
  assert operator_pvc_manager.get_volume_id('some-pv') == 'vol-0a6d7a39a07212c42'
  # test gp3 spec
  gp3 = MagicMock()
  gp3.items[0].spec.csi.volume_handle = 'vol-0a6d7a39a07212c42'
  gp3.items[0].spec.aws_elastic_block_store = None
  v1.list_persistent_volume.return_value = gp3
  assert operator_pvc_manager.get_volume_id('some-pv') == 'vol-0a6d7a39a07212c42'
  # test missing volume
  novol = MagicMock()
  novol.items[0].spec.csi = None
  novol.items[0].spec.aws_elastic_block_store = None
  v1.list_persistent_volume.return_value = novol
  with pytest.raises(RuntimeError) as excinfo:
    operator_pvc_manager.get_volume_id('some-pv')
  assert excinfo.typename == 'RuntimeError'

def test_ready_check(operator_pvc_manager, tmpdir, cloudtrail, v1, appsv1):
  operator_pvc_manager.cloudtrail = cloudtrail
  operator_pvc_manager.v1 = v1
  operator_pvc_manager.appsv1 = appsv1
  check_file=f"{tmpdir}/heartbeat"
  operator_pvc_manager.ready_check(check_file=check_file)
  with open(check_file) as f:
    assert f.readlines() == ['ready\n']

def test_health_check(operator_pvc_manager, tmpdir):
  check_file=f"{tmpdir}/heartbeat"
  operator_pvc_manager.health_check(check_file=check_file)
  with open(check_file) as f:
    assert f.readlines() == ['running\n']
