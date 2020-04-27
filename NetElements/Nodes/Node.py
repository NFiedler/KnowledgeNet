from typing import List, Dict, Optional
from bson import ObjectId


class Node:

    def __init__(self,
                 name: str,
                 description: str = '',
                 in_relations: Optional[List[ObjectId]] = None,
                 out_relations: Optional[List[ObjectId]] = None,
                 bi_relations: Optional[List[ObjectId]] = None,
                 object_id: Optional[ObjectId] = None
                 ):
        self.name = name
        self.description = description
        if in_relations:
            self.in_relations = in_relations
        else:
            self.in_relations = list()
        if out_relations:
            self.out_relations = out_relations
        else:
            self.out_relations = list()
        if bi_relations:
            self.bi_relations = bi_relations
        else:
            self.bi_relations = list()

        self.id = object_id

    @classmethod
    def from_dict(cls, node_dict: Dict):
        return cls(
            name=node_dict['name'],
            description=node_dict['description'],
            in_relations=node_dict['in_relations'],
            out_relations=node_dict['out_relations'],
            bi_relations=node_dict['bi_relations'],
            object_id=node_dict['_id'],
        )

    def to_dict(self, include_id: bool = False):
        node_dict = {
            'name': self.name,
            'description': self.description,
            'in_relations': self.in_relations,
            'out_relations': self.out_relations,
            'bi_relations': self.bi_relations,
        }
        if include_id:
            node_dict['_id'] = self.id
        return node_dict
