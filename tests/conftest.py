import pytest
import uuid


@pytest.fixture
def uid():
    return lambda: str(uuid.uuid4())


@pytest.fixture
def parent_uid(uid):
    return uid()


@pytest.fixture
def metadata_node():
    return {
        'uid': '3665c27e-d055-47d7-8393-5f934f3ced9d',
        'name_prefix': '1.',
        'name': 'Energy',
        'template_node_uid': None,
    }


@pytest.fixture
def nodes(metadata_node, parent_uid, uid):
    return [
        metadata_node,
        {
            'uid': 'fed65b84-cdad-4e38-8848-ea6af3c391bc',
            'name_prefix': '2.',
            'name': 'Industrial processes and product use',
            'template_node_uid': None,
            'node': []
        },
        {
            'uid': '43bc1534-201c-416b-a348-e5866d69dddb',
            'name_prefix': '3.',
            'name': 'Agriculture',
            'template_node_uid': uid(),
            'node': []
        },
        {
            'uid': 'db7b9be0-76bc-497e-a4ee-9334ec2429d2',
            'name_prefix': '4.',
            'name': 'Land use, land-use change and forestry',
            'template_node_uid': uid(),
            'node': []
        },
        {
            'uid': 'b1e41219-79a2-493d-ba97-de0e4d7f9d0f',
            'name_prefix': '5.',
            'name': 'Waste',
            'parent_uid': None,
            'template_node_uid': uid(),
            'node': []
        }
    ]


@pytest.fixture
def raw_metadata(nodes):
    return {
        'Metadata': [
            {
                'node': nodes,
                'dimension': [
                    {
                        'id': 1,
                        'name': 'NAVIGATION'
                    }
                ],
                'dimension_instance': [
                    {
                        'dimension_id': 1,
                        'id': 301,
                        'uid': 'db7b9be0-76bc-497e-a4ee-9334ec2429d2',
                        'name': '4. Land use, land-use change and forestry',
                        'children': []
                    }
                ],
                'grid': [],
                'variable': [
                    {
                        'id': 483,
                        'uid': 'de6fab87-82f6-46d5-b8f5-73190d8e4ace',
                        'node_uid': 'db7b9be0-76bc-497e-a4ee-9334ec2429d2',
                        'name': '[4. Land use, land-use change and '
                        'forestry][no classification][Emissions]'
                        '[CO₂][no parameter][kt]',
                        'is_calculated': True,
                        'is_template': False,
                        'is_editable': False,
                        'dimension_to_change': [],
                    }
                ],
            }
        ]
    }
