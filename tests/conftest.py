import logging
import pytest
from datetime import datetime
from dateutil.tz import tzlocal
from importlib import reload
from random import randint
from unittest import mock


@pytest.fixture()
def operator_pvc_manager():
  # this fixture is so mocks don't persist between tests
  from operator_pvc_manager import operator_pvc_manager
  operator_pvc_manager.logger = logging.getLogger(__name__)
  yield operator_pvc_manager
  reload(operator_pvc_manager)

@pytest.fixture()
def appsv1(sts):
  appsv1 = mock.MagicMock()
  appsv1.list_stateful_set_for_all_namespaces.return_value = True
  sts_list = mock.MagicMock()
  sts_list.items = [sts]
  appsv1.list_namespaced_stateful_set.return_value = sts_list
  return appsv1

@pytest.fixture()
def v1():
  v1 = mock.MagicMock()
  v1.list_persistent_volume_claim_for_all_namespaces.return_value = True
  pvc = mock.MagicMock()
  pvc.metadata.name = 'some-pvc'
  pvc.metadata.namespace = 'default'
  pvc.metadata.annotations.get.return_value = 'some-sts'
  v1.list_persistent_volume_claim_for_all_namespaces.items = [pvc]
  pv = mock.MagicMock()
  pv.items[0].spec.aws_elastic_block_store.volume_id = 'aws://us-east-1c/vol-0a6d7a39a07212c42'
  v1.list_persistent_volume.return_value = pv
  return v1

@pytest.fixture()
def sts():
  sts = mock.MagicMock()
  sts.metadata.name = 'some-sts'
  sts.metadata.namespace = 'default'
  sts.metadata.annotations = {'pvc-operator/storage-size': '500Gi'}
  return sts

@pytest.fixture()
def pvc():
  pvc = mock.MagicMock()
  pvc.metadata.name = 'some-sts-storage-12'
  pvc.metadata.namespace = 'default'
  pvc.metadata.annotations = {'pvc-operator/statefulset': 'some-sts'}
  return pvc

@pytest.fixture()
def cloudtrail():
  return MockCloudtrail()

class MockCloudtrail():
  """Moto doesn't provide a mock for cloudtrail, so making our own."""
  def __init__(self):
    self.mode = "random"
  
  def random_mode(self):
    """Make lookup_events return any type of event in random order. This is
    the default."""
    self.mode = "random"

  def attach_first(self):
    """Make calls to lookup_events return an AttachVolume event first."""
    self.mode = "attach"

  def detatch_first(self):
    """Make calls to lookup_events return a DetachVolume event first."""
    self.mode = "detatch"

  def lookup_events(self, MaxResults, LookupAttributes=[]):
    """Return a fake response with MaxResults elements. The order is random
    unless a mode was specified."""
    event_boilerplate = {
      'NextToken': '2xx0x1Ec5ChGGwt/RvH4ii6HbkSpkxV0Fxy08nRjHkOV1FXMFZZm6W3/L+l7xz+o',
      'ResponseMetadata': {'RequestId': 'c4fa7fcc-3a11-4ba4-8635-da0817c1ab40', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': 'c4fa7fcc-3a11-4ba4-8635-da0817c1ab40', 'content-type': 'application/x-amz-json-1.1', 'content-length': '1747', 'date': 'Fri, 24 Sep 2021 19:55:48 GMT'}, 'RetryAttempts': 0}
    }
    fake_tag_event = {'EventId': '058b6e28-fc6b-4e24-999d-3688423c9f43', 'EventName': 'CreateTags', 'ReadOnly': 'false', 'AccessKeyId': 'ACCESSKEYAWSUSER', 'EventTime': datetime(2021, 9, 24, 7, 16, 39, tzinfo=tzlocal()), 'EventSource': 'ec2.amazonaws.com', 'Username': 'Tagging-Role', 'Resources': [{'ResourceName': 'vol-02e6465799de7075a'}], 'CloudTrailEvent': '{"eventVersion":"1.08","userIdentity":{"type":"AssumedRole","principalId":"ACCESSKEYAWSUSER:Tagging-Role","arn":"arn:aws:sts::000000000000:assumed-role/Tagging-Role/Tagging-Role","accountId":"000000000000","accessKeyId":"ACCESSKEYAWSUSER","sessionContext":{"sessionIssuer":{"type":"Role","principalId":"ACCESSKEYAWSUSER","arn":"arn:aws:iam::000000000000:role/Tagging-Role","accountId":"000000000000","userName":"Tagging-Role"},"webIdFederationData":{},"attributes":{"creationDate":"2021-09-24T10:52:26Z","mfaAuthenticated":"false"}}},"eventTime":"2021-09-24T11:16:39Z","eventSource":"ec2.amazonaws.com","eventName":"CreateTags","awsRegion":"us-east-1","sourceIPAddress":"192.168.1.2","userAgent":"Boto3/1.17.100 Python/3.6.14 Linux/4.14.243-194.434.amzn2.x86_64 exec-env/AWS_Lambda_python3.6 Botocore/1.20.112 Resource","requestParameters":{"resourcesSet":{"items":[{"resourceId":"vol-02e6465799de7075a"}]},"tagSet":{"items":[{"key":"MyTag/Name","value":"super-cool-value"}]}},"responseElements":{"requestId":"c6ba9477-de04-4139-ae02-d2067a439a23","_return":true},"requestID":"c6ba9477-de04-4139-ae02-d2067a439a23","eventID":"058b6e28-fc6b-4e24-999d-3688423c9f43","readOnly":false,"eventType":"AwsApiCall","managementEvent":true,"recipientAccountId":"000000000000","eventCategory":"Management"}'}
    fake_attach_event = {'EventId': 'c51f115c-8ff0-4cc1-9221-beb548c96319', 'EventName': 'AttachVolume', 'ReadOnly': 'false', 'AccessKeyId': 'ACCESSKEYAWSUSER', 'EventTime': datetime(2021, 9, 24, 7, 16, 34, tzinfo=tzlocal()), 'EventSource': 'ec2.amazonaws.com', 'Username': 'i-0d4afab1261582ea4', 'Resources': [{'ResourceType': 'AWS::EC2::Volume', 'ResourceName': 'vol-02e6465799de7075a'}, {'ResourceType': 'AWS::EC2::Instance', 'ResourceName': 'i-00a057f697c421b81'}], 'CloudTrailEvent': '{"eventVersion":"1.08","userIdentity":{"type":"AssumedRole","principalId":"ACCESSKEYAWSUSER:i-0d4afab1261582ea4","arn":"arn:aws:sts::000000000000:assumed-role/instance-profile/i-0d4afab1261582ea4","accountId":"000000000000","accessKeyId":"ACCESSKEYAWSUSER","sessionContext":{"sessionIssuer":{"type":"Role","principalId":"ACCESSKEYAWSUSER","arn":"arn:aws:iam::000000000000:role/instance-profile","accountId":"000000000000","userName":"instance-profile"},"webIdFederationData":{},"attributes":{"creationDate":"2021-09-24T08:03:19Z","mfaAuthenticated":"false"},"ec2RoleDelivery":"2.0"}},"eventTime":"2021-09-24T11:16:34Z","eventSource":"ec2.amazonaws.com","eventName":"AttachVolume","awsRegion":"us-east-1","sourceIPAddress":"192.168.1.2","userAgent":"kubernetes/v1.18.18 aws-sdk-go/1.28.2 (go1.13.15; linux; amd64)","requestParameters":{"volumeId":"vol-02e6465799de7075a","instanceId":"i-00a057f697c421b81","device":"/dev/xvdcj","deleteOnTermination":false},"responseElements":{"requestId":"a954674c-98cd-4540-bcec-0094f50016d8","volumeId":"vol-02e6465799de7075a","instanceId":"i-00a057f697c421b81","device":"/dev/xvdcj","status":"attaching","attachTime":1632482194610,"deleteOnTermination":false},"requestID":"a954674c-98cd-4540-bcec-0094f50016d8","eventID":"c51f115c-8ff0-4cc1-9221-beb548c96319","readOnly":false,"eventType":"AwsApiCall","managementEvent":true,"recipientAccountId":"000000000000","eventCategory":"Management"}'}
    fake_detatch_event = {'EventId': '5bd736e8-ea6c-4a0f-9379-4626a40690cb', 'EventName': 'DetachVolume', 'ReadOnly': 'false', 'AccessKeyId': 'ACCESSKEYAWSUSER', 'EventTime': datetime(2021, 9, 24, 7, 16, 27, tzinfo=tzlocal()), 'EventSource': 'ec2.amazonaws.com', 'Username': 'i-0d4afab1261582ea4', 'Resources': [{'ResourceType': 'AWS::EC2::Volume', 'ResourceName': 'vol-02e6465799de7075a'}, {'ResourceType': 'AWS::EC2::Instance', 'ResourceName': 'i-01cc30b73e6aa39e7'}], 'CloudTrailEvent': '{"eventVersion":"1.08","userIdentity":{"type":"AssumedRole","principalId":"ACCESSKEYAWSUSER:i-0d4afab1261582ea4","arn":"arn:aws:sts::000000000000:assumed-role/instance-profile/i-0d4afab1261582ea4","accountId":"000000000000","accessKeyId":"ACCESSKEYAWSUSER","sessionContext":{"sessionIssuer":{"type":"Role","principalId":"ACCESSKEYAWSUSER","arn":"arn:aws:iam::000000000000:role/instance-profile","accountId":"000000000000","userName":"instance-profile"},"webIdFederationData":{},"attributes":{"creationDate":"2021-09-24T08:03:19Z","mfaAuthenticated":"false"},"ec2RoleDelivery":"2.0"}},"eventTime":"2021-09-24T11:16:27Z","eventSource":"ec2.amazonaws.com","eventName":"DetachVolume","awsRegion":"us-east-1","sourceIPAddress":"192.168.1.2","userAgent":"kubernetes/v1.18.18 aws-sdk-go/1.28.2 (go1.13.15; linux; amd64)","requestParameters":{"volumeId":"vol-02e6465799de7075a","instanceId":"i-01cc30b73e6aa39e7","force":false},"responseElements":{"requestId":"61831d71-9e87-47d6-b629-71de34a3fcd6","volumeId":"vol-02e6465799de7075a","instanceId":"i-01cc30b73e6aa39e7","device":"/dev/xvdba","status":"detaching","attachTime":1632334646000},"requestID":"61831d71-9e87-47d6-b629-71de34a3fcd6","eventID":"5bd736e8-ea6c-4a0f-9379-4626a40690cb","readOnly":false,"eventType":"AwsApiCall","managementEvent":true,"recipientAccountId":"000000000000","eventCategory":"Management"}'}
    fake_events = [fake_tag_event, fake_attach_event, fake_detatch_event]
    events_rollup = []
    if self.mode == "attach":
      events_rollup.append(fake_attach_event)
    elif self.mode == "detatch":
      events_rollup.append(fake_detatch_event)
    while len(events_rollup) < MaxResults:
      # add random fake events to the list
      events_rollup.append(fake_events[randint(0, len(fake_events)-1)])
    event_boilerplate['Events'] = events_rollup
    return event_boilerplate
