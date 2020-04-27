import pymongo
from bson import ObjectId
from typing import List, Set, Dict, Tuple, Optional
import KnowledgeNetExceptions

class MongoBackend:
    def __init__(self, db_path, db_name='world'):
        self.client = pymongo.MongoClient(db_path)
        self.db = self.client[db_name]
        self.node_coll = self.db['nodes']
        self.uni_rel_coll = self.db['uni_relations']
        self.bi_rel_coll = self.db['bi_relations']
        self.uni_rel_type_coll = self.db['uni_relation_types']
        self.bi_rel_type_coll = self.db['bi_relation_types']

    def add_uni_rel_type(self, type_name, type_description=''):
        result = self.uni_rel_type_coll.insert_one({'name': type_name, 'description': type_description, 'values': {}})
        return result.inserted_id

    def add_bi_rel_type(self, type_name, type_description=''):
        result = self.bi_rel_type_coll.insert_one({'name': type_name, 'description': type_description, 'values': {}})
        return result.inserted_id

    def add_node(self, node_name, node_description=''):
        # TODO: prevent allowing node names which could be an object id
        result = self.node_coll.insert_one({
            'name': node_name,
            'description': node_description,
            'in_relations': [],
            'out_relations': [],
            'bi_relations': []})
        return result.inserted_id

    def delete_node(self, node_id: ObjectId) -> None:
        """
        Deletes a node and all relations to that node
        :param node_id: The ID of the deleted node
        """
        relations = self.get_relation_ids_of_node(node_id)
        self.uni_rel_coll.delete_many({'_id': {'$in': relations['in_relations'] + relations['out_relations']}})
        self.bi_rel_coll.delete_many({'_id': {'$in': relations['bi_relations']}})
        self.node_coll.delete_one({'_id': node_id})

    def add_nodes(self, node_names):
        result = self.node_coll.insert_many(
            [{
                'name': node_name,
                'description': '',
                'in_relations': [],
                'out_relations': [],
                'bi_relations': []
            } for node_name in node_names])
        return result.inserted_ids

    def add_uni_relation(self, node_from_id, node_to_id, relation_type_id):
        new_relation = self.uni_rel_coll.insert_one({
            'type': relation_type_id,
            'node_from': node_from_id,
            'node_to': node_to_id})
        relation_id = new_relation.inserted_id
        self.node_coll.update_one({'_id': node_from_id}, {'$push': {'out_relations': relation_id}})
        self.node_coll.update_one({'_id': node_to_id}, {'$push': {'in_relations': relation_id}})
        return relation_id

    def add_bi_relation(self, node_1_id, node_2_id, relation_type_id):
        new_relation = self.bi_rel_coll.insert_one({
            'type': relation_type_id,
            'node_1': node_1_id,
            'node_2': node_2_id,
            'is_uni': False
        })
        relation_id = new_relation.inserted_id
        self.node_coll.update_one({'_id': node_1_id}, {'$push': {'bi_relations': relation_id}})
        self.node_coll.update_one({'_id': node_2_id}, {'$push': {'bi_relations': relation_id}})
        return relation_id

    def get_node(self, node_id):
        return self.node_coll.find_one({'_id': node_id})

    def get_nodes(self, node_ids):
        return self.node_coll.find({'_id': {'$in': node_ids}})

    def get_relation_type_name(self, rel_type_id: ObjectId, uni: bool) -> str:
        if uni:
            return self.uni_rel_type_coll.find_one({'_id': rel_type_id}, {'name': 1})['name']
        else:
            return self.bi_rel_coll.find_one({'_id': rel_type_id}, {'name': 1})['name']

    def get_relation_ids_of_node(self, node_id: ObjectId) -> Dict:
        """
        Returns a dictionary of lists of relations ('in_relations', 'out_relations' and 'bi_relations'
        of a certain node.
        :param node_id: The ID of the node.
        :return: dictionary of lists of relations ('in_relations', 'out_relations' and 'bi_relations' of the node.
        """
        return self.node_coll.find_one(
            {'_id': node_id},
            {'in_relations': 1, 'out_relations': 1, 'bi_relations': 1})

    def get_relation_objects_of_node(self, node_id):
        relations = self.get_relation_ids_of_node(node_id)
        return {
            'in_relations': self.uni_rel_coll.find({'_id': {'$in': relations['in_relations']}}),
            'out_relations': self.uni_rel_coll.find({'_id': {'$in': relations['out_relations']}}),
            'bi_relations': self.bi_rel_coll.find({'_id': {'$in': relations['bi_relations']}})
        }


    def list_nodes_by_name(self, node_name:str, sloppy: bool = False):
        if sloppy:
            return list(self.node_coll.find({'name': {'$regex': node_name, '$options': 'i'}}))
        return list(self.node_coll.find({'name': node_name}))

    def list_relationtypes_by_name(self, relation_type_name, uni):
        if uni:
            return list(self.uni_rel_type_coll.find({'name': relation_type_name}))
        else:
            return list(self.bi_rel_type_coll.find({'name': relation_type_name}))

    def get_relation_info_of_node(self, node_id):
        relations = self.get_relation_objects_of_node(node_id)
        return {
            'in_relations': [{
                'type': self.uni_rel_type_coll.find_one({'_id': relation['type']}),
                'from': self.node_coll.find_one({'_id': relation['node_from']})
            } for relation in relations['in_relations']],
            'out_relations': [{
                'type': self.uni_rel_type_coll.find_one({'_id': relation['type']}),
                'to': self.node_coll.find_one({'_id': relation['node_to']})
            } for relation in relations['out_relations']],
            'bi_relations': [{
                'type': self.bi_rel_type_coll.find_one({'_id': relation['type']}),
                'with': self.node_coll.find_one({'_id': relation['node_1']})
                if relation['node_1'] != node_id
                else self.node_coll.find_one({'_id': relation['node_2']})
            } for relation in relations['bi_relations']]
        }

    def get_all_node_names(self):
        return [node['name'] for node in self.node_coll.find()]

    def get_all_uni_connected_nodes(self, start_node_id, relation_type_id, stop_at: Optional[ObjectId]=None, include_direction=True, inverse_direction=False) -> List[ObjectId]:
        visited_nodes = set()
        current_nodes = {start_node_id}
        if include_direction:
            while current_nodes and (stop_at is None or stop_at in visited_nodes):
                result = self.uni_rel_coll.find(
                    {
                        'node_from': {'$in': list(current_nodes)},
                        'node_to': {'$nin': list(visited_nodes)},
                        'type': relation_type_id
                    },
                    {
                        '_id': 0,
                        'node_to': 1
                    }
                    )
                result = list(result)
                visited_nodes |= current_nodes
                current_nodes = set([result_elem['node_to'] for result_elem in result])
                current_nodes -= visited_nodes
        else:
            while current_nodes and (stop_at is None or stop_at in visited_nodes):
                result = self.uni_rel_coll.find(
                    {
                        '$or': [
                            {'node_from': {'$in': list(current_nodes)}},
                            {'node_to': {'$in': list(current_nodes)}}
                        ],
                        'node_to': {'$nin': list(visited_nodes)},
                        'node_from': {'$nin': list(visited_nodes)},
                        'type': relation_type_id
                    },
                    {
                        '_id': 0,
                        'node_from': 1,
                        'node_to': 1
                    }
                    )
                result = list(result)
                visited_nodes |= current_nodes
                current_nodes = set(
                    [result_elem['node_from'] for result_elem in result] +
                    [result_elem['node_to'] for result_elem in result])
                current_nodes -= visited_nodes
        return list(visited_nodes)

    def get_all_bi_connected_nodes(self, start_node_id: ObjectId, relation_type_id: ObjectId, stop_at: Optional[ObjectId] = None) -> List[ObjectId]:
        visited_nodes = set()
        current_nodes = {start_node_id}
        while current_nodes and (stop_at is None or stop_at in visited_nodes):
            result = self.bi_rel_coll.find(
                {
                    '$or': [
                        {'node_1': {'$in': list(current_nodes)}},
                        {'node_2': {'$in': list(current_nodes)}}
                    ],
                    'node_1': {'$nin': list(visited_nodes)},
                    'node_2': {'$nin': list(visited_nodes)},
                    'type': relation_type_id
                },
                {
                    '_id': 0,
                    'node_1': 1,
                    'node_2': 1
                }
                )
            result = list(result)
            visited_nodes |= current_nodes
            current_nodes = set(
                [result_elem['node_1'] for result_elem in result] +
                [result_elem['node_2'] for result_elem in result])
            current_nodes -= visited_nodes
        return list(visited_nodes)

    def get_uni_relationtype_usage_number(self, relation_type_id: ObjectId) -> int:
        return self.uni_rel_coll.count({'type': relation_type_id})

    def get_bi_relationtype_usage_number(self, relation_type_id: ObjectId) -> int:
        return self.bi_rel_coll.count({'type': relation_type_id})

    def get_relations_between(self, node_1_id: ObjectId, node_2_id: ObjectId) -> Tuple[List[Dict], List[Dict]]:
        uni_relations = self.uni_rel_coll.find({'$or': [
            {
                'node_from': node_1_id,
                'node_to': node_2_id
            },
            {
                'node_from': node_2_id,
                'node_to': node_1_id
            }
        ]})
        bi_relations = self.bi_rel_coll.find({'$or': [
            {
                'node_1': node_1_id,
                'node_2': node_2_id
            },
            {
                'node_1': node_2_id,
                'node_2': node_1_id
            }
        ]})
        return list(uni_relations), list(bi_relations)

    def get_uni_relationtypes(self, relation_ids: Optional[List[ObjectId]] = None):
        if relation_ids is None:
            return list(self.uni_rel_type_coll.find())
        return self.uni_rel_type_coll.find({
            '_id': {'$in': relation_ids}
        })

    def get_bi_relationtypes(self, relation_ids: Optional[List[ObjectId]] = None):
        if relation_ids is None:
            return list(self.bi_rel_type_coll.find())
        return list(self.bi_rel_type_coll.find({
            '_id': {'$in': relation_ids}
        }))


b = MongoBackend('mongodb://localhost:27017/')
