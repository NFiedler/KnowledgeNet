import pymongo
from bson import ObjectId
from typing import List, Set, Dict, Tuple, Optional, Union
import KnowledgeNetExceptions
from NetElements.Nodes.Node import Node
from NetElements.Relations.Relations import BiRelation, UniRelation, Relation, RelationType


class MongoBackend:
    def __init__(self, db_path, db_name='world'):
        self.client = pymongo.MongoClient(db_path)
        self.db = self.client[db_name]
        self.node_coll = self.db['nodes']
        self.uni_rel_coll = self.db['uni_relations']
        self.bi_rel_coll = self.db['bi_relations']
        self.uni_rel_type_coll = self.db['uni_relation_types']
        self.bi_rel_type_coll = self.db['bi_relation_types']

    def add_rel_type(self, rel_type: RelationType) -> ObjectId:
        if rel_type.is_uni:
            result = self.uni_rel_type_coll.insert_one(rel_type.to_dict())
        else:
            result = self.bi_rel_type_coll.insert_one(rel_type.to_dict())
        return result.inserted_id

    def add_node(self, node: Node) -> ObjectId:
        # TODO: prevent allowing node names which could be an object id
        result = self.node_coll.insert_one(node.to_dict())
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

    def add_nodes(self, nodes):
        result = self.node_coll.insert_many([node.to_dict() for node in nodes])
        return result.inserted_ids

    def add_relation(self, relation: Union[UniRelation, BiRelation]):
        if relation.is_uni:
            relation_type = RelationType.from_dict(self.uni_rel_type_coll.find_one({'_id': relation.type}))
        else:
            relation_type = RelationType.from_dict(self.bi_rel_type_coll.find_one({'_id': relation.type}))

        if relation.node_1 == relation.node_2 and not relation_type.reflexive:
            raise KnowledgeNetExceptions.ReflexiveException(
                'Tried to apply a reflexive relation of non-reflexive relation type!'
            )
        if relation.probability != 1.0 and not relation_type.probabilistic:
            raise KnowledgeNetExceptions.ProbabilisticException(
                f'Tried to add an uncertain relation with non-probabilistic type (\"{relation_type.name}\")!'
            )
        if relation.is_uni:
            new_relation = self.uni_rel_coll.insert_one(relation.to_dict())
            relation_id = new_relation.inserted_id
            self.node_coll.update_one({'_id': relation.node_from}, {'$push': {'out_relations': relation_id}})
            self.node_coll.update_one({'_id': relation.node_to}, {'$push': {'in_relations': relation_id}})
        else:
            new_relation = self.bi_rel_coll.insert_one(relation.to_dict())
            relation_id = new_relation.inserted_id
            self.node_coll.update_one({'_id': relation.node_1}, {'$push': {'bi_relations': relation_id}})
            self.node_coll.update_one({'_id': relation.node_2}, {'$push': {'bi_relations': relation_id}})
        return relation_id

    def get_node(self, node_id):
        return Node.from_dict(self.node_coll.find_one({'_id': node_id}))

    def get_nodes(self, node_ids):
        return Node.from_dict_list(self.node_coll.find({'_id': {'$in': node_ids}}))

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
            'in_relations': Relation.from_dict_list(self.uni_rel_coll.find({'_id': {'$in': relations['in_relations']}})),
            'out_relations': Relation.from_dict_list(self.uni_rel_coll.find({'_id': {'$in': relations['out_relations']}})),
            'bi_relations': Relation.from_dict_list(self.bi_rel_coll.find({'_id': {'$in': relations['bi_relations']}}))
        }

    def list_nodes_by_name(self, node_name:str, sloppy: bool = False):
        if sloppy:
            return Node.from_dict_list(self.node_coll.find({'name': {'$regex': node_name, '$options': 'i'}}))
        return Node.from_dict_list(self.node_coll.find({'name': node_name}))

    def list_relationtypes_by_name(self, relation_type_name, uni):
        if uni:
            return RelationType.from_dict_list(self.uni_rel_type_coll.find({'name': relation_type_name}))
        else:
            return RelationType.from_dict_list(self.bi_rel_type_coll.find({'name': relation_type_name}))

    def get_relation_info_of_node(self, node_id) -> Dict:
        relations = self.get_relation_objects_of_node(node_id)
        return {
            'in_relations': [{
                'id': relation.id,
                'prob': relation.probability,
                'type': RelationType.from_dict(self.uni_rel_type_coll.find_one({'_id': relation.type})),
                'from': Node.from_dict(self.node_coll.find_one({'_id': relation.node_from}))
            } for relation in relations['in_relations']],
            'out_relations': [{
                'id': relation.id,
                'prob': relation.probability,
                'type': RelationType.from_dict(self.uni_rel_type_coll.find_one({'_id': relation.type})),
                'to': Node.from_dict(self.node_coll.find_one({'_id': relation.node_to}))
            } for relation in relations['out_relations']],
            'bi_relations': [{
                'id': relation.id,
                'prob': relation.probability,
                'type': RelationType.from_dict(self.bi_rel_type_coll.find_one({'_id': relation.type})),
                'with': Node.from_dict(self.node_coll.find_one({'_id': relation.node_1}))
                if relation.node_1 != node_id
                else Node.from_dict(self.node_coll.find_one({'_id': relation.node_2}))
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

    def get_relations_between(self, node_1_id: ObjectId, node_2_id: ObjectId) -> Tuple[List[UniRelation], List[BiRelation]]:
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
        return Relation.from_dict_list(uni_relations), Relation.from_dict_list(bi_relations)

    def get_uni_relationtypes(self, relation_ids: Optional[List[ObjectId]] = None) -> List[RelationType]:
        if relation_ids is None:
            return RelationType.from_dict_list(self.uni_rel_type_coll.find())
        return RelationType.from_dict_list(self.uni_rel_type_coll.find({
            '_id': {'$in': relation_ids}
        }))

    def get_bi_relationtypes(self, relation_ids: Optional[List[ObjectId]] = None) -> List[RelationType]:
        if relation_ids is None:
            return RelationType.from_dict_list(self.bi_rel_type_coll.find())
        return RelationType.from_dict_list(self.bi_rel_type_coll.find({
            '_id': {'$in': relation_ids}
        }))


b = MongoBackend('mongodb://localhost:27017/')

# TODO: virtual relations (cached)
# TODO: required/optional relations together with other relations -> prompt when creating a new one
# TODO; fix relation between
# TODO: more options in node info view
# TODO: mark relation types as transitional
# TODO: values on relations and relation types
# TODO: probabilistic relations
# TODO: reflexive_check finish
# TODO: elements as classes
# TODO: differ between class nodes and Object Nodes
