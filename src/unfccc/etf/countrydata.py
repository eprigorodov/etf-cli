from copy import deepcopy
import functools
import logging
import secrets

from .json import JSONCatalog, JSONTree
from .util import pformat_size, sizeof_dict


logger = logging.getLogger(__name__)


class CountryData(JSONTree):

    stat_points = [
        ('Country specific dimension instances',
         'country_specific_data.dimension_instances'),
        ('Country specific nodes', 'country_specific_data.nodes'),
        ('Country specific variables', 'country_specific_data.variables'),
        ('Country specific grids', 'country_specific_data.grids'),
        ('Country specific drop-downs', 'country_specific_data.drop_downs'),
        ('Country specific line descriptions',
         'country_specific_data.line_description'),
        ('Country specific (meta)data', 'country_specific_data'),
        ('Country data', 'data')
    ]

    def __init__(self, metadata, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = metadata
        self.node_index = JSONCatalog(
            ['uid', 'parent_uid', 'template_node_uid', 'name_prefix', 'name'],
            self.traverse(self.nodes)
        )
        self.variable_index = JSONCatalog(
            ['uid', 'node_uid', 'template_var_uid'],
            self.variables
        )
        self.grid_index = JSONCatalog(['node_uid'],
                                      self.traverse(self.grids))

    @functools.cached_property
    def root(self):
        return self.tree

    @functools.cached_property
    def country_metadata(self):
        return self.root['country_specific_data']

    @functools.cached_property
    def nodes(self):
        return self.country_metadata['nodes']

    @functools.cached_property
    def variables(self):
        return self.country_metadata['variables']

    @functools.cached_property
    def grids(self):
        return self.country_metadata.setdefault('grids', [])

    @functools.cached_property
    def line_descriptions(self):
        return self.country_metadata.setdefault('line_description', [])

    @functools.cached_property
    def data(self):
        return self['data']['values']

    @staticmethod
    def is_metadata_uid(uid):
        return '-' in uid

    def get_node(self, uid, fallback_to_metadata=True):
        result = self.node_index.first(uid=uid)
        if result is None and fallback_to_metadata:
            result = self.metadata.get_node(uid)
        return result

    def get_grid(self, node_uid, fallback_to_metadata=True):
        result = self.grid_index.first(node_uid=node_uid)
        if result is None and fallback_to_metadata:
            result = self.metadata.get_grid(node_uid)
        return result

    def collect_sector_uids(self, filter_):
        result = self.metadata.collect_sector_uids(filter_)
        sector_uids = result['nodes']
        old_len = len(sector_uids)
        for node in self.nodes:
            if 'parent_uid' in node and node['parent_uid'] in sector_uids:
                for child in self.traverse(node):
                    sector_uids.add(child['uid'])
        logger.info('collected %s country specific node uids',
                    len(sector_uids) - old_len)
        return result

    @staticmethod
    def filter_out(item_list, filter_func, valid_uids=None):
        to_delete = []
        for index, item in enumerate(item_list):
            if filter_func(item):
                if valid_uids is not None:
                    valid_uids.add(item['uid'])
            else:
                to_delete.append(index)
        for index in reversed(to_delete):
            del item_list[index]
        return to_delete

    @staticmethod
    def make_uid():
        return secrets.token_hex(12)

    def make_variable(self, node_uid, template_var_uid):
        result = {
            'uid': self.make_uid(),
            'node_uid': node_uid,
            'template_var_uid': template_var_uid
        }
        self.variables.append(result)
        self.variable_index.index(result)
        return result

    def clone_grid_from_template(self, template_node_uid, node_uid):
        result = deepcopy(self.get_grid(template_node_uid))
        result['node_uid'] = node_uid
        for group in self.traverse(result['group']):
            if 'uid' not in group or 'variable_uid' not in group:
                # only traverse nested groups
                continue
            group['template_group_uid'] = group['uid']
            group['uid'] = self.make_uid()
            template_var_uid = group['variable_uid']
            if template_var_uid is None:
                continue
            variable = self.variable_index.first(
                node_uid=node_uid,
                template_var_uid=template_var_uid
            )
            if variable is None:
                logger.debug('adding missing variable "%s" '
                             'as required by grid "%s"',
                             (node_uid, template_var_uid), template_node_uid)
                variable = self.make_variable(node_uid, template_var_uid)
            group['variable_uid'] = variable['uid']
        return result

    def reparent_nodes(self):
        # reparent multi-level nodes into tree structure
        nested_nodes = []
        for index, node in enumerate(self.nodes):
            if 'template_node_uid' in node and 'parent_uid' in node:
                node_uid = node['uid']
                parent_uid = node['parent_uid']
                if self.is_metadata_uid(node_uid) \
                        or self.is_metadata_uid(parent_uid):
                    continue
                parent_node = self.get_node(parent_uid, False)
                if parent_node is None:
                    logger.error(
                        'node "%s" refers to missing parent node "%s"',
                        node_uid, parent_uid
                    )
                    continue
                yield node, parent_node
                parent_node.setdefault('node', []).append(node)
                nested_nodes.append(index)
                del node['parent_uid']
        # remove reparented nodes from the root level list and rebuild indexes
        if nested_nodes:
            self.node_index.clear()
            for index in reversed(nested_nodes):
                del self.nodes[index]
            self.node_index.index_iterable(self.traverse(self.nodes))

    def fix_node_grid(self, node):
        if 'template_node_uid' not in node:
            return
        node_uid = node['uid']
        if 'parent_uid' in node:
            logger.debug('skipping broken node %s due to missing parent %s',
                         node_uid, node['parent_uid'])
        grid = self.get_grid(node_uid, fallback_to_metadata=False)
        if grid is not None:
            return
        logger.debug('detected country specific node without grid, '
                     'uid="%s", path "%s"', node_uid, self.json_path(node))
        template_node_uid = node['template_node_uid']
        new_grid = self.clone_grid_from_template(template_node_uid, node_uid)
        self.grids.append(new_grid)
        self.grid_index.index(new_grid)

    def count_statistics(self):
        result = []
        for (label, json_path) in self.stat_points:
            item = self.locate(json_path)
            length = index = size = 0
            if item is not None:
                if not self.is_object(item):
                    # JSON array, report also flat length
                    length = len(item)
                for index, child in enumerate(self.traverse(item)):
                    size += sizeof_dict(child)
                index += 1
            result.append({
                'label': label,
                'objects_flat': length,
                'objects_nested': index,
                'size': pformat_size(size)
            })
        return result
