from typing import List, Dict, Optional
from bson import ObjectId
from abc import abstractclassmethod


class RelationType:

    def __init__(self,
                 name: str,
                 uni: bool,
                 description: str = '',
                 values: Optional[Dict] = None,
                 reflexive: bool = False,
                 relation_type_id: Optional[ObjectId] = None,
                 ):
        self.name = name
        self.description = description
        if not values:
            values = dict()
        self.values = values
        self.reflexive = reflexive
        self.is_uni = uni
        self.id = relation_type_id

    @classmethod
    def from_dict(cls, relation_type_dict):
        if '_id' in relation_type_dict:
            return cls(
                name=relation_type_dict['name'],
                uni=relation_type_dict['is_uni'],
                description=relation_type_dict['description'],
                values=relation_type_dict['values'],
                reflexive=relation_type_dict['reflexive'],
                relation_type_id=relation_type_dict['_id'],
            )
        return cls(
                name=relation_type_dict['node_from'],
                uni=relation_type_dict['is_uni'],
                description=relation_type_dict['description'],
                values=relation_type_dict['values'],
                reflexive=relation_type_dict['reflexive'],
        )

    def to_dict(self, include_id: bool = False) -> Dict:
        relation_type_dict = {
            'name': self.name,
            'is_uni': self.is_uni,
            'description': self.description,
            'values': self.values,
            'reflexive': self.reflexive,
        }
        if include_id:
            relation_type_dict['_id'] = self.id
        return relation_type_dict


class Relation:

    @classmethod
    def from_dict(cls, relation_dict: Dict):
        if 'is_uni' not in relation_dict.keys():
            raise ValueError('is_uni was not part of the input Dict')
        if relation_dict['is_uni']:
            return UniRelation.from_dict(relation_dict)
        else:
            return UniRelation.from_dict(relation_dict)


class UniRelation:

    def __init__(self,
                 relation_type_id: ObjectId,
                 node_from_id: ObjectId,
                 node_to_id: ObjectId,
                 relation_id: Optional[ObjectId] = None,
                 ):

        self.type = relation_type_id
        self.node_from = node_from_id
        self.node_to = node_to_id
        self.is_uni = True
        self.id = relation_id

    @property
    def node_1(self):
        return self.node_from

    @property
    def node_2(self):
        return self.node_to

    @classmethod
    def from_dict(cls, relation_dict: Dict):
        if '_id' in relation_dict:
            return cls(
                relation_type_id=relation_dict['type'],
                node_from_id=relation_dict['node_from'],
                node_to_id=relation_dict['node_to'],
                relation_id=relation_dict['_id']
            )
        return cls(
            relation_type_id=relation_dict['type'],
            node_from_id=relation_dict['node_from'],
            node_to_id=relation_dict['node_to'],
        )

    def to_dict(self, include_id: bool = False) -> Dict:
        rel_dict = {
            'type': self.type,
            'node_from': self.node_from,
            'node_to': self.node_to,
            'is_uni': self.is_uni
        }
        if include_id:
            rel_dict['_id'] = self.id

        return rel_dict


class BiRelation:

    def __init__(self,
                 relation_type_id: ObjectId,
                 node_1_id: ObjectId,
                 node_2_id: ObjectId,
                 relation_id: Optional[ObjectId] = None,
                 ):
        self.type = relation_type_id
        self.node_1 = node_1_id
        self.node_2 = node_2_id

        self.is_uni = False
        self.id = relation_id

    @classmethod
    def from_dict(cls, relation_dict: Dict):
        if '_id' in relation_dict:
            return cls(
                relation_type_id=relation_dict['type'],
                node_1_id=relation_dict['node_1'],
                node_2_id=relation_dict['node_2'],
                relation_id=relation_dict['_id']
            )
        return cls(
            relation_type_id=relation_dict['type'],
            node_1_id=relation_dict['node_1'],
            node_2_id=relation_dict['node_2'],
        )

    def to_dict(self, include_id: bool = False) -> Dict:
        rel_dict = {
            'type': self.type,
            'node_from': self.node_1,
            'node_2': self.node_2,
            'is_uni': self.is_uni
        }
        if include_id:
            rel_dict['_id'] = self.id

        return rel_dict
