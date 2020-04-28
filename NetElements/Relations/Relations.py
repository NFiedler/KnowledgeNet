from typing import List, Dict, Optional
from bson import ObjectId
from abc import abstractclassmethod

EPSILON = 0.000000001

class RelationType:

    def __init__(self,
                 name: str,
                 uni: bool,
                 description: str = '',
                 values: Optional[Dict] = None,
                 reflexive: bool = False,
                 probabilistic: bool = False,
                 relation_type_id: Optional[ObjectId] = None,
                 ):
        self.name = name
        self.description = description
        if not values:
            values = dict()
        self.values = values
        self.reflexive = reflexive
        self.probabilistic = probabilistic
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
                probabilistic=relation_type_dict['probabilistic'],
                relation_type_id=relation_type_dict['_id'],
            )
        return cls(
                name=relation_type_dict['node_from'],
                uni=relation_type_dict['is_uni'],
                description=relation_type_dict['description'],
                values=relation_type_dict['values'],
                reflexive=relation_type_dict['reflexive'],
                probabilistic=relation_type_dict['probabilistic'],
        )

    @classmethod
    def from_dict_list(cls, relation_type_dict_list: List[Dict]):
        return [cls.from_dict(relation_type_dict) for relation_type_dict in relation_type_dict_list]

    def to_dict(self, include_id: bool = False) -> Dict:
        relation_type_dict = {
            'name': self.name,
            'is_uni': self.is_uni,
            'description': self.description,
            'values': self.values,
            'reflexive': self.reflexive,
            'probabilistic': self.probabilistic,
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
            return BiRelation.from_dict(relation_dict)

    @classmethod
    def from_dict_list(cls, relation_dict_list: List[Dict]):
        return [cls.from_dict(relation_dict) for relation_dict in relation_dict_list]

    @classmethod
    def create_relation(cls,
                        is_uni: bool,
                        relation_type_id: ObjectId,
                        node_from_id: ObjectId,
                        node_to_id: ObjectId,
                        probability: float = 1,
                        relation_id: Optional[ObjectId] = None,
                        ):
        if is_uni:
            return UniRelation(relation_type_id=relation_type_id,
                               node_from_id=node_from_id,
                               node_to_id=node_to_id,
                               probability=probability,
                               relation_id=relation_id)
        else:
            return BiRelation(relation_type_id=relation_type_id,
                              node_1_id=node_from_id,
                              node_2_id=node_to_id,
                              probability=probability,
                              relation_id=relation_id)


class UniRelation:

    def __init__(self,
                 relation_type_id: ObjectId,
                 node_from_id: ObjectId,
                 node_to_id: ObjectId,
                 probability: float = 1,
                 relation_id: Optional[ObjectId] = None,
                 ):

        self.type = relation_type_id
        self.node_from = node_from_id
        self.node_to = node_to_id
        self.is_uni = True
        self.probability = probability
        self.id = relation_id

    @property
    def certain(self) -> bool:
        return abs(self.probability - 1.0) < EPSILON

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
                probability=relation_dict['probability'],
                relation_id=relation_dict['_id'],
            )
        return cls(
            relation_type_id=relation_dict['type'],
            node_from_id=relation_dict['node_from'],
            node_to_id=relation_dict['node_to'],
            probability=relation_dict['probability'],
        )

    def to_dict(self, include_id: bool = False) -> Dict:
        rel_dict = {
            'type': self.type,
            'node_from': self.node_from,
            'node_to': self.node_to,
            'is_uni': self.is_uni,
            'probability': self.probability,
        }
        if include_id:
            rel_dict['_id'] = self.id

        return rel_dict


class BiRelation:

    def __init__(self,
                 relation_type_id: ObjectId,
                 node_1_id: ObjectId,
                 node_2_id: ObjectId,
                 probability: float = 1,
                 relation_id: Optional[ObjectId] = None,
                 ):
        self.type = relation_type_id
        self.node_1 = node_1_id
        self.node_2 = node_2_id
        self.probability = probability

        self.is_uni = False
        self.id = relation_id

    @property
    def certain(self) -> bool:
        return abs(self.probability - 1.0) < EPSILON

    @classmethod
    def from_dict(cls, relation_dict: Dict):
        if '_id' in relation_dict:
            return cls(
                relation_type_id=relation_dict['type'],
                node_1_id=relation_dict['node_1'],
                node_2_id=relation_dict['node_2'],
                probability=relation_dict['probability'],
                relation_id=relation_dict['_id']
            )
        return cls(
            relation_type_id=relation_dict['type'],
            node_1_id=relation_dict['node_1'],
            node_2_id=relation_dict['node_2'],
            probability=relation_dict['probability'],
        )

    def to_dict(self, include_id: bool = False) -> Dict:
        rel_dict = {
            'type': self.type,
            'node_1': self.node_1,
            'node_2': self.node_2,
            'is_uni': self.is_uni,
            'probability': self.probability,
        }
        if include_id:
            rel_dict['_id'] = self.id

        return rel_dict
