#! /usr/bin/env python
from MongoBackend import *
from bson import ObjectId

from enum import IntEnum
from typing import Union, List, Dict
import argparse
import sys
import os


class MenuAction(IntEnum):
    DELETE_NODE = 0
    NODE_DETAIL = 1
    ASK = 2
    CREATE_RELATION = 3
    CREATE_NODE = 4

    HELP = 98
    QUIT = 99

class ParamsType(IntEnum):
    NODE = 0
    RELATION = 1
    NONE = 99


class KnowledgeNetFrontend:

    def __init__(self):
        self.rows, self.columns = os.popen('stty size', 'r').read().split()

        self.node_menu_action_mappings = [
            {
                'name': 'Delete',
                'key': 'd',
                'description': 'delete the current node',
                'action': MenuAction.DELETE_NODE,
                'required_params': [],
                'optional_params': [ParamsType.NODE],
            },
            {
                'name': 'Detail',
                'key': '+',
                'description': 'More details on a specific node',
                'action': MenuAction.NODE_DETAIL,
                'required_params': [ParamsType.NODE],
                'optional_params': [],
            },
            {
                'name': 'Create Node',
                'key': 'n',
                'description': 'Add a new node',
                'action': MenuAction.CREATE_NODE,
                'required_params': [],
                'optional_params': [ParamsType.NODE],
            },
            {
                'name': 'Create Relation',
                'key': 'r',
                'description': 'Add a new relation to the node',
                'action': MenuAction.CREATE_RELATION,
                'required_params': [],
                'optional_params': [ParamsType.NODE],
            },
            {
                'name': 'Help',
                'key': '?',
                'description': 'Print menu action help',
                'action': MenuAction.HELP,
                'required_params': [],
                'optional_params': [],
            },
            {
                'name': 'Quit',
                'key': 'q',
                'description': 'Quit the menu',
                'action': MenuAction.QUIT,
                'required_params': [],
                'optional_params': [],
            }
        ]

        self.backend = b
        parser = argparse.ArgumentParser(prog='KnowledgeNet UI')
        # parser.add_argument('--foo', action='store_true', help='foo help')
        subparsers = parser.add_subparsers(help='command groups')

        # create the parser for the "node" command group
        ################################################

        parser_node = subparsers.add_parser('node', help='node related commands')
        node_subparsers = parser_node.add_subparsers()

        # create a parser for the "node list" command
        parser_node_list = node_subparsers.add_parser('list', help='List all nodes')
        parser_node_list.set_defaults(func=self.list_nodes)

        # create a parser for the "node create" command
        parser_node_create = node_subparsers.add_parser('create', help='Create a new node')
        parser_node_create.add_argument('node_name', default='', nargs='?')
        parser_node_create.set_defaults(func=self.create_node)

        # info a parser for the "node info" command
        parser_node_info = node_subparsers.add_parser('info', help='Information on a node')
        parser_node_info.add_argument('node_name', default='', nargs='?')
        parser_node_info.set_defaults(func=self.node_info)

        # relations a parser for the "node relations" command
        parser_node_relations = node_subparsers.add_parser('relations', help='Information on a node')
        parser_node_relations.add_argument('node_name', default='', nargs='?')
        node_relations_dir_ndir_group = parser_node_relations.add_mutually_exclusive_group()
        node_relations_dir_ndir_group.add_argument('--dir', action='store_true', default=False)
        node_relations_dir_ndir_group.add_argument('--ndir', action='store_true', default=False)
        parser_node_relations.set_defaults(func=self.node_relations)

        # create the parser for the "relation" command group
        ####################################################
        parser_relation = subparsers.add_parser('relation', help='relation related commands')
        relation_subparsers = parser_relation.add_subparsers()

        # create a parser for the "relation create" command
        parser_relation_create = relation_subparsers.add_parser('create', help='Create a new relation')
        parser_relation_create.add_argument('relation_name', default='', nargs='?')
        relation_uni_bi_group = parser_relation_create.add_mutually_exclusive_group()
        relation_uni_bi_group.add_argument('--uni', action='store_true', default=False)
        relation_uni_bi_group.add_argument('--bi', action='store_true', default=False)
        parser_relation_create.set_defaults(func=self.create_relation)

        # create the parser for the "relationtype" command group
        ########################################################
        parser_relationtype = subparsers.add_parser('relationtype', help='relation type related commands')
        relationtype_subparsers = parser_relationtype.add_subparsers()

        # create a parser for the "relation type create" command
        parser_relationtype_create = relationtype_subparsers.add_parser('create', help='Create a new relation type')
        parser_relationtype_create.add_argument('relation_type_name', default='', nargs='?')
        relationtype_create_uni_bi_group = parser_relationtype_create.add_mutually_exclusive_group()
        relationtype_create_uni_bi_group.add_argument('--uni', action='store_true', default=False)
        relationtype_create_uni_bi_group.add_argument('--bi', action='store_true', default=False)
        parser_relationtype_create.set_defaults(func=self.create_relationtype)

        # list a parser for the "relation type list" command
        parser_relationtype_list = relationtype_subparsers.add_parser('list', help='Create a new relation type')
        parser_relationtype_list.add_argument('--uni', action='store_true', default=True)
        parser_relationtype_list.add_argument('--bi', action='store_true', default=True)
        parser_relationtype_list.set_defaults(func=self.list_relationtypes)

        
        
        # do stuff
        args = parser.parse_args()
        args.func(args)

    def list_nodes(self, args):
        print(self.backend.get_all_node_names())

    def list_relationtypes(self, args):
        if args.uni:
            print('Unidirectional relation types:')
            self.print_line('-')
            for uni_type in self.backend.get_uni_relationtypes():
                print(f'{uni_type["name"]}: {uni_type["description"]}')
            print()
        if args.bi:
            print('Bidirectional relation types:')
            self.print_line('-')
            for bi_type in self.backend.get_bi_relationtypes():
                print(f'{bi_type["name"]}: {bi_type["description"]}')

    def create_node(self, args=None):
        if args is not None:
            name = str(args.node_name)
        else:
            name = ''
        if name == '':
            name = input('Enter the name of the new node: ')
        description = ''
        if self.yes_no('Do you want to add a description? '):
            description = input('Enter the description of the new node: ')
        result = self.backend.add_node(name, description)
        print(result)
        return result, MenuAction.NODE_DETAIL

    def create_relationtype(self, args):
        uni, bi = args.uni, args.bi
        if uni == bi:
            uni = self.uni_bi('Enter \"uni\" or \"bi\" to create a unidirectional or a bidirectional relation type: ')
            bi = not uni

        name = str(args.relation_type_name)
        if name == '':
            name = input('Enter the name of the new {}directional relation type: '.format('uni' if uni else 'bi'))
        description = ''
        if self.yes_no('Do you want to add a description? '):
            description = input('Enter the description of the new relation type: ')
        if uni:
            result = self.backend.add_uni_rel_type(name, description)
        else:
            result = self.backend.add_bi_rel_type(name, description)
        print(result)

    def node_info(self, args):
        node = str(args.node_name)
        if node == '':
            node = input('Which node do you want to learn about? ')
        # handle follows
        self.node_detail_loop(node)

    def node_relations(self, args):
        node = str(args.node_name)
        if node == '':
            node = input('Which node do you want to learn about? ')
        node = self.to_node(node)
        relation_type_name = input('What is the relation type? ')
        uni, relation_type_id = self.find_relationtype_id(relation_type_name)
        if uni:
            directed, not_directed = args.dir, args.ndir
            if directed == not_directed:
                directed = self.yes_no('Do you want to consider the relation direction? ')
            node_ids = self.backend.get_all_uni_connected_nodes(node['_id'], relation_type_id, include_direction=directed)
        else:
            node_ids = self.backend.get_all_bi_connected_nodes(node['_id'], relation_type_id)
        nodes: List[Dict] = list(self.backend.get_nodes(node_ids))
        self.print_line()
        print(f'Nodes connected to {node["name"]}: ')
        self.print_line('-')
        for i, node in enumerate(nodes):
            print(f'{i} - {node["name"]}: {node["description"]}')
        print()
        selection = input('If you want to see a node in detail, enter the number in front of it. Otherwise type \'q\'.\n')
        follow_node = None
        while follow_node is None:
            selection = selection.lower()
            if selection == 'q':
                return None
            if selection.isdigit() and int(selection) < len(nodes):
                follow_node = list(nodes)[int(selection)]['_id']
            else:
                selection = input('Your input was invalid. If you want to  see a node in detail, enter the number in front of it. Otherwise type \'q\'.\n')
        self.node_detail_loop(follow_node)

    def node_detail_loop(self, init_node: Union[Dict, str, ObjectId]):
        node = init_node
        action = MenuAction.NODE_DETAIL
        while action is not MenuAction.QUIT:
            node = self.to_node(node)
            if action == MenuAction.NODE_DETAIL:
                node, action = self.node_detail(node)
            elif action == MenuAction.ASK:
                node, action = self.node_detail(node, ask=True)
            elif action == MenuAction.CREATE_NODE:
                node, action = self.create_node()
            elif action == MenuAction.CREATE_RELATION:
                self.create_relation(node1=node)
                action = MenuAction.NODE_DETAIL
            elif action == MenuAction.DELETE_NODE:
                if self.yes_no('Do you really want to delete this node and all connected relations? '):
                    self.backend.delete_node(node['_id'])
                    action = MenuAction.QUIT
                else:
                    action = MenuAction.NODE_DETAIL
            elif action == MenuAction.HELP:
                self.print_menu_action_help()
                action = MenuAction.ASK

    def node_detail(self, node: dict, ask: bool = False) -> Tuple[Optional[int], int]:

        relation_info = self.backend.get_relation_info_of_node(node['_id'])
        relation_counter = 0
        related_node_ids = [rel['from']['_id'] for rel in relation_info['in_relations']] + \
                           [rel['to']['_id'] for rel in relation_info['out_relations']] + \
                           [rel['with']['_id'] for rel in relation_info['bi_relations']]
        if not ask:
            self.print_line()
            print(node['name'])
            self.print_line('-')
            for key in node.keys():
                if 'relations' not in key:
                    print('{}: {}'.format(key, node[key]))
        if relation_info['in_relations']:
            if not ask:
                print('in_relations: ')
            for relation in relation_info['in_relations']:
                if not ask:
                    print('\t{} - \"{}\" from \"{}\"'.format(
                        relation_counter,
                        relation['type']['name'],
                        relation['from']['name']))
                relation_counter += 1
        if relation_info['out_relations']:
            if not ask:
                print('out_relations: ')
            for relation in relation_info['out_relations']:
                if not ask:
                    print('\t{} - \"{}\" to \"{}\"'.format(
                        relation_counter,
                        relation['type']['name'],
                        relation['to']['name']))
                relation_counter += 1
        if relation_info['bi_relations']:
            if not ask:
                print('bi_relations: ')
            for relation in relation_info['bi_relations']:
                if not ask:
                    print('\t{} - \"{}\" with \"{}\"'.format(
                        relation_counter,
                        relation['type']['name'],
                        relation['with']['name']))
                relation_counter += 1
        # if relation_counter == 0:
        #     return None

        # to be re-usable in case of invalid input
        follow_sentence = 'If you want to follow a relation, enter the number in front of it. Type \'q\' to quit. For help type \'?\'.\n-> '
        selection = input(follow_sentence)
        while True:
            selection = selection.lower()
            selection = selection.replace(' ', '')
            if selection == 'q':
                return None, MenuAction.QUIT
            if selection == 'n':
                return node['_id'], MenuAction.CREATE_NODE
            if selection == 'r':
                return node['_id'], MenuAction.CREATE_RELATION
            if selection == 'd':
                return node['_id'], MenuAction.DELETE_NODE
            if selection == '?':
                return node['_id'], MenuAction.HELP
            if (selection.isdigit() and int(selection) < relation_counter) or \
                    (selection[0] == '+' and selection[1:].isdigit() and int(selection[1:]) < relation_counter):
                return related_node_ids[int(selection)], MenuAction.NODE_DETAIL
            selection = input('Your input was invalid. ' + follow_sentence)
            
    def create_relation(self, args=None, node1: Dict = None, node2: Dict = None):
        if args is None:
            uni, rel_type = self.find_relationtype_id(input('Enter the name of the relation type: '))
        else:
            uni, bi = args.uni, args.bi
            if uni == bi:
                uni, rel_type = self.find_relationtype_id(input('Enter the name of the relation type: '))
            else:
                rel_type = self.to_relationtype_id(input('Enter the name of the relation type: '), uni=uni)
        if node1 is None:
            node1 = self.to_node_id(input('Enter the name of the first (origin) node: '))
            node2 = self.to_node_id(input('Enter the name of the second (target) node: '))
        elif uni:
            if self.in_out(f"Is the new {self.backend.get_relation_type_name(rel_type, uni)}-relation incoming or outgoing to the current node ({node1['name']})? "):
                node2 = node1['_id']
                node1 = self.to_node_id(input('Enter the name of the origin node: '))
            else:
                node1 = node1['_id']
                node2 = self.to_node_id(input('Enter the name of the target node: '))
        else:
            node1 = node1['_id']
            node2 = self.to_node_id(input('Enter the name of the other node: '))

        if uni:
            result = self.backend.add_uni_relation(node1, node2, rel_type)
        else:
            result = self.backend.add_bi_relation(node1, node2, rel_type)
        print(result)

    def relations_between(self, args):
        node_1_id = self.to_node_id(input('Enter the name of the first node: '))
        node_2_id = self.to_node_id(input('Enter the name of the second node: '))
        uni_relations, bi_relations = self.backend.get_relations_between(node_1_id, node_2_id)
        print(list(zip(self.backend.get_bi_relationtypes([rel['_id'] for rel in bi_relations]), bi_relations)))
        if args.uni:
            print('Unidirectional relations:')
            self.print_line('-')
            for uni_relation in zip(self.backend.get_uni_relationtypes([rel['_id'] for rel in uni_relations]), uni_relations):
                print(f'{uni_relation[0]["name"]}: {uni_relation[1]["id"]}')
            print()
        if args.bi:
            print('Bidirectional relations:')
            self.print_line('-')
            for bi_relation in zip(self.backend.get_bi_relationtypes([rel['_id'] for rel in bi_relations]), bi_relations):
                print(f'{bi_relation[0]["name"]}: {bi_relation[1]["id"]}')

    def to_node(self, name: Union[str, ObjectId, Dict], exit_on_err: bool = True) -> Dict:
        if type(name) == ObjectId:
            return self.backend.get_node(name)
        if type(name) == dict:
            return name
        nodes = self.backend.list_nodes_by_name(name)
        if not nodes:
            print('There is no node known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(nodes) == 1:
            return nodes[0]

        # Nodes is long. This means that there are multiple nodes by that name
        return nodes[int(self.select_node(nodes))]

    def to_node_id(self, name: Union[str, ObjectId, Dict], exit_on_err: bool = True) -> ObjectId:
        # TODO: include id in query
        if type(name) == dict:
            return name['_id']
        if type(name) == ObjectId:
            return name
        nodes = self.backend.list_nodes_by_name(name)
        if not nodes:
            print('There is no node known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(nodes) == 1:
            return nodes[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        return nodes[int(self.select_node(nodes))]['_id']

    def find_relationtype_id(self, name: Union[ObjectId, str], exit_on_err: bool = True) -> Tuple[Optional[bool], Optional[ObjectId]]:
        if type(name) == ObjectId:
            if self.backend.uni_rel_type_coll.find({'_id': name}).limit(1).size():
                uni = True
            elif self.backend.bi_rel_type_coll.find({'_id': name}).limit(1).size():
                uni = False
            else:
                uni = None
            return uni, name
        types = self.backend.list_relationtypes_by_name(name, uni=True)
        uni_count = len(types)
        types += self.backend.list_relationtypes_by_name(name, uni=False)
        if not types:
            print('There is no relation type known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None, None
        if len(types) == 1:
            return (uni_count > 0), types[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        uni, selection = self.select_relationtype(types, uni_count)
        return uni, types[selection]['_id']

    def to_relationtype_id(self, name: Union[ObjectId, str], uni: bool, exit_on_err: bool = True) -> ObjectId:
        if type(name) == ObjectId:
            return name
        types = self.backend.list_relationtypes_by_name(name, uni=uni)
        if not types:
            print('There is no relation type known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(types) == 1:
            return types[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        return types[int(self.select_relationtype(types))]['_id']

    def select_node(self, nodes: List[Dict]) -> int:
        print('There exist multiple nodes with the name \"{}\": '.format(nodes[0]['name']))
        for i, node in enumerate(nodes):
            print('{} - {}: {}'.format(i, node['name'], node['description']))
        selection = ''
        while not selection.isdigit() or len(nodes) <= int(selection):
            selection = input('Enter your selection: ')
        return int(selection)

    def select_relationtype(self, types: List[dict], uni_count=-1):
        print('There exist multiple relation types with the name \"{}\": '.format(types[0]['name']))
        rel_type_group = ''
        for i, type in enumerate(types):
            if uni_count >= 0:
                if i < uni_count:
                    rel_type_group = '[uni]'
                    usage_count = ' (used in {} relations)'.format(self.backend.get_uni_relationtype_usage_number(type['_id']))
                else:
                    rel_type_group = '[bi]'
                    usage_count = ' (used in {} relations)'.format(self.backend.get_bi_relationtype_usage_number(type['_id']))

            print('{} - {} {}: {}{}'.format(i, type['name'], rel_type_group, type['description'], usage_count))
        selection = ''
        while not selection.isdigit() or len(types) < int(selection):
            selection = input('Enter your selection: ')
        selection = int(selection)
        if uni_count >= 0:
            return (selection < uni_count), selection
        return selection

    def yes_no(self, answer: str) -> bool:
        yes = {'yes', 'y', 'ye', ''}
        no = {'no', 'n'}

        while True:
            choice = input(answer).lower()
            if choice in yes:
                return True
            elif choice in no:
                return False
            else:
                print('Please respond with \'yes\' or \'no\'')

    def uni_bi(self, answer) -> bool:
        uni = {'u', 'uni'}
        bi = {'b', 'bi'}

        while True:
            choice = input(answer).lower()
            if choice in uni:
                return True
            elif choice in bi:
                return False
            else:
                print('Please respond with \'uni\' or \'bi\'')

    def in_out(self, answer) -> bool:
        in_words = {'i', 'in'}
        out_words = {'o', 'out'}

        while True:
            choice = input(answer).lower()
            if choice in in_words:
                return True
            elif choice in out_words:
                return False
            else:
                print('Please respond with \'in\' or \'out\'')

    def print_menu_action_help(self):
        for action in self.node_menu_action_mappings:
            print(f"{action['key']} - {action['name']}: {action['description']}")

    def print_line(self, char='#') -> None:
        print(char * int(self.columns))


twf = KnowledgeNetFrontend()
