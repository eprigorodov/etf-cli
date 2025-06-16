import pytest
from unfccc.etf.metadata import Metadata


@pytest.fixture
def metadata(raw_metadata):
    return Metadata(raw_metadata)


def test_collect_sector_uids(metadata, uid):
    lulucf = metadata.nodes[3]
    uid0 = uid()
    uid1 = uid()
    lulucf['node'].append({
        'uid': uid0,
        'node': [{
            'uid': uid1
        }]
    })
    filter = metadata.get_sector_filter('lulucf')
    assert metadata.collect_sector_uids(filter) == {
        'nodes': {'db7b9be0-76bc-497e-a4ee-9334ec2429d2', uid0, uid1},
        'variables': {'de6fab87-82f6-46d5-b8f5-73190d8e4ace'},
        'dimension_instances': {'db7b9be0-76bc-497e-a4ee-9334ec2429d2'},
    }
