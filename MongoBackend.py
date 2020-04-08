import pymongo


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
            'node_2': node_2_id})
        relation_id = new_relation.inserted_id
        self.node_coll.update_one({'_id': node_1_id}, {'$push': {'bi_relations': relation_id}})
        self.node_coll.update_one({'_id': node_2_id}, {'$push': {'bi_relations': relation_id}})
        return relation_id

    def get_node(self, node_id):
        return self.node_coll.find_one({'_id': node_id})

    def get_relation_ids_of_node(self, node_id):
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

    def list_nodes_by_name(self, node_name):
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



b = MongoBackend('mongodb://localhost:27017/')
